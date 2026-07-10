//! Localhost HTTP/WebSocket transport for the Matrix bridge adapter.

use std::{
    collections::HashMap,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{
        DefaultBodyLimit, FromRequestParts, Multipart, Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, request::Parts},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use inline_client::{
    AuthStartRequest, AuthVerifyRequest, ChatParticipantsRequest, ClientCommandError,
    ClientErrorCategory, ClientRequestError, ClientStatus, ConnectRequest, CreateDmRequest,
    CreateReplyThreadRequest, CreateThreadRequest, DeleteMessageRequest, DialogsRequest,
    EditMessageRequest, HistoryRequest, InlineClient, ReactRequest, ReadRequest, SendTextRequest,
    TypingRequest, UploadRequest,
};
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpListener,
    sync::{Mutex as AsyncMutex, broadcast},
    task::JoinHandle,
};

use crate::event_store::{AdapterEventStore, AdapterEventStoreError};
use crate::protocol::{
    ChatStateRequest, PROTOCOL_VERSION, ProtocolInfo, SidecarCommand, SidecarError,
    SidecarEventEnvelope, SidecarHealth, SidecarRequest, SidecarRequestId, SidecarResponse,
    SidecarResult, SidecarStatus,
};

const SIDECAR_UPLOAD_BODY_LIMIT_BYTES: usize = 101 * 1024 * 1024;
const SESSION_NAMESPACE_HEADER: &str = "x-inline-session-namespace";

/// Async factory used by production adapters to lazily create one native
/// client runtime per account namespace.
pub type AdapterClientFactory = Arc<
    dyn Fn(
            String,
        )
            -> Pin<Box<dyn Future<Output = Result<AdapterClientRegistration, String>> + Send>>
        + Send
        + Sync,
>;

/// A lazily constructed account client and whether its durable session should
/// be resumed after the adapter attaches the lossless event collector.
#[derive(Clone, Debug)]
pub struct AdapterClientRegistration {
    /// Isolated native client handle for one account namespace.
    pub client: InlineClient,
    /// Whether a durable session existed when the client was opened.
    pub resume_stored_session: bool,
}

/// Shared HTTP adapter state.
#[derive(Clone)]
pub struct AdapterHttpState {
    clients: Arc<StdMutex<HashMap<String, InlineClient>>>,
    client_creation_lock: Arc<AsyncMutex<()>>,
    client_factory: Option<AdapterClientFactory>,
    default_namespace: Option<String>,
    request_sequence: Arc<AtomicU64>,
    event_store: AdapterEventStore,
    events: broadcast::Sender<SidecarEventEnvelope>,
    event_collectors: Arc<StdMutex<HashMap<String, JoinHandle<()>>>>,
}

impl std::fmt::Debug for AdapterHttpState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdapterHttpState")
            .field(
                "client_count",
                &self.clients.lock().expect("adapter clients poisoned").len(),
            )
            .field("has_client_factory", &self.client_factory.is_some())
            .field("has_default_namespace", &self.default_namespace.is_some())
            .field("event_store", &self.event_store)
            .finish_non_exhaustive()
    }
}

impl AdapterHttpState {
    /// Creates HTTP adapter state for a client handle.
    pub fn new(client: InlineClient) -> Self {
        let event_store = AdapterEventStore::open_in_memory("test")
            .expect("in-memory adapter event store should open");
        Self::with_event_store(client, event_store)
    }

    /// Creates HTTP adapter state with a durable event replay store.
    pub fn with_event_store(client: InlineClient, event_store: AdapterEventStore) -> Self {
        let (events, _) = broadcast::channel(256);
        let namespace = event_store
            .active_namespace()
            .unwrap_or_else(|_| "test".to_owned());
        let clients = HashMap::from([(namespace.clone(), client.clone())]);
        let state = Self {
            clients: Arc::new(StdMutex::new(clients)),
            client_creation_lock: Arc::new(AsyncMutex::new(())),
            client_factory: None,
            default_namespace: Some(namespace.clone()),
            request_sequence: Arc::new(AtomicU64::new(0)),
            event_store,
            events,
            event_collectors: Arc::new(StdMutex::new(HashMap::new())),
        };
        state.start_event_collector(namespace, client);
        state
    }

    /// Creates shared HTTP state that lazily resolves isolated native clients
    /// by the session namespace request header.
    pub fn with_client_factory(
        event_store: AdapterEventStore,
        client_factory: AdapterClientFactory,
    ) -> Self {
        let (events, _) = broadcast::channel(256);
        Self {
            clients: Arc::new(StdMutex::new(HashMap::new())),
            client_creation_lock: Arc::new(AsyncMutex::new(())),
            client_factory: Some(client_factory),
            default_namespace: None,
            request_sequence: Arc::new(AtomicU64::new(0)),
            event_store,
            events,
            event_collectors: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    fn start_event_collector(&self, namespace: String, client: InlineClient) {
        let mut lossless_events = client
            .take_lossless_events()
            .expect("adapter must be the sole lossless inline-client event consumer");
        let mut best_effort_events = client.subscribe();
        let event_store = self.event_store.clone();
        let events = self.events.clone();
        let collector_namespace = namespace.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    lossless = lossless_events.recv_delivery() => {
                        let Some(delivery) = lossless else { break };
                        let envelope = loop {
                            match event_store.append_for_namespace_delivery(
                                &collector_namespace,
                                delivery.delivery_id(),
                                delivery.event().clone(),
                            ) {
                                Ok(envelope) => break envelope,
                                Err(error) => {
                                    log::error!(
                                        "failed to persist lossless adapter event; retrying: {error}"
                                    );
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                }
                            }
                        };
                        loop {
                            match delivery.ack().await {
                                Ok(()) => break,
                                Err(error) => {
                                    log::error!(
                                        "failed to acknowledge durable client event; retrying: {error}"
                                    );
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                }
                            }
                        }
                        if let Some(delivery_id) = delivery.delivery_id()
                            && let Err(error) = event_store.finalize_client_delivery(
                                &collector_namespace,
                                delivery_id,
                            )
                        {
                            log::warn!(
                                "failed to prune acknowledged client delivery receipt: {error}"
                            );
                        }
                        let _ = events.send(envelope);
                    }
                    best_effort = best_effort_events.recv() => {
                        match best_effort {
                            Ok(event) if event.reliability() == inline_client::EventReliability::BestEffort => {
                                match event_store.append_for_namespace(&collector_namespace, event) {
                                    Ok(envelope) => {
                                        let _ = events.send(envelope);
                                    }
                                    Err(error) => {
                                        log::debug!("dropping failed best-effort adapter event: {error}");
                                    }
                                }
                            }
                            Ok(_) => continue,
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                log::debug!(
                                    "adapter best-effort event subscriber lagged; skipped {skipped} transient events"
                                );
                                continue;
                            }
                        }
                    }
                }
            }
        });
        if let Some(previous) = self
            .event_collectors
            .lock()
            .expect("adapter event collectors poisoned")
            .insert(namespace, handle)
        {
            previous.abort();
        }
    }

    async fn client_for_namespace(&self, namespace: &str) -> Result<InlineClient, String> {
        let namespace = validate_session_namespace(namespace)?;
        if let Some(client) = self
            .clients
            .lock()
            .expect("adapter clients poisoned")
            .get(namespace)
            .cloned()
        {
            return Ok(client);
        }

        let _creation = self.client_creation_lock.lock().await;
        if let Some(client) = self
            .clients
            .lock()
            .expect("adapter clients poisoned")
            .get(namespace)
            .cloned()
        {
            return Ok(client);
        }
        let factory = self
            .client_factory
            .as_ref()
            .ok_or_else(|| format!("unknown Inline session namespace {namespace:?}"))?;
        let registration = factory(namespace.to_owned()).await?;
        let client = registration.client;
        self.clients
            .lock()
            .expect("adapter clients poisoned")
            .insert(namespace.to_owned(), client.clone());
        self.start_event_collector(namespace.to_owned(), client.clone());
        if registration.resume_stored_session
            && let Err(error) = client.resume_session().await
        {
            log::warn!("Inline account startup resume failed: {error}");
        }
        Ok(client)
    }

    async fn release_client(&self, namespace: &str, client: InlineClient) {
        if self.client_factory.is_none() {
            return;
        }
        let _creation = self.client_creation_lock.lock().await;
        self.clients
            .lock()
            .expect("adapter clients poisoned")
            .remove(namespace);
        if let Err(error) = client.shutdown().await {
            log::warn!("failed to shut down released Inline account runtime: {error}");
        }
        let collector = self
            .event_collectors
            .lock()
            .expect("adapter event collectors poisoned")
            .remove(namespace);
        if let Some(mut collector) = collector {
            match tokio::time::timeout(Duration::from_secs(5), &mut collector).await {
                Ok(Err(error)) if !error.is_cancelled() => {
                    log::warn!("released Inline account event collector failed: {error}");
                }
                Err(_) => {
                    log::warn!("released Inline account event collector did not stop; aborting");
                    collector.abort();
                    let _ = collector.await;
                }
                _ => {}
            }
        }
        if let Err(error) = self.event_store.remove_namespace(namespace) {
            log::error!("failed to remove released Inline account replay state: {error}");
        }
    }

    /// Returns the durable event replay store.
    pub fn event_store(&self) -> &AdapterEventStore {
        &self.event_store
    }

    /// Returns a live event subscription after durable persistence.
    pub fn subscribe_events(&self) -> broadcast::Receiver<SidecarEventEnvelope> {
        self.events.subscribe()
    }

    fn next_request_id(&self) -> SidecarRequestId {
        let sequence = self.request_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        SidecarRequestId::try_new(format!("http-{sequence}"))
            .expect("generated request ID should be valid")
    }
}

fn validate_session_namespace(namespace: &str) -> Result<&str, String> {
    let namespace = namespace.trim();
    if namespace.is_empty() {
        return Err("Inline session namespace must not be empty".to_owned());
    }
    if namespace.len() > 128 {
        return Err("Inline session namespace is too long".to_owned());
    }
    if !namespace
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err("Inline session namespace contains unsupported characters".to_owned());
    }
    Ok(namespace)
}

#[derive(Clone, Debug)]
struct AdapterRequestClient(InlineClient);

#[derive(Clone, Debug)]
struct AdapterRequestNamespace(String);

async fn request_namespace(parts: &Parts, state: &AdapterHttpState) -> Result<String, Response> {
    let namespace = parts
        .headers
        .get(SESSION_NAMESPACE_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| state.default_namespace.clone())
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "missing x-inline-session-namespace header",
            )
                .into_response()
        })?;
    validate_session_namespace(&namespace)
        .map_err(|error| (StatusCode::BAD_REQUEST, error).into_response())?;
    Ok(namespace)
}

impl FromRequestParts<AdapterHttpState> for AdapterRequestClient {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AdapterHttpState,
    ) -> Result<Self, Self::Rejection> {
        let namespace = request_namespace(parts, state).await?;
        state
            .client_for_namespace(&namespace)
            .await
            .map(Self)
            .map_err(|error| (StatusCode::BAD_REQUEST, error).into_response())
    }
}

impl FromRequestParts<AdapterHttpState> for AdapterRequestNamespace {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AdapterHttpState,
    ) -> Result<Self, Self::Rejection> {
        let namespace = request_namespace(parts, state).await?;
        state
            .client_for_namespace(&namespace)
            .await
            .map_err(|error| (StatusCode::BAD_REQUEST, error).into_response())?;
        Ok(Self(namespace))
    }
}

/// Builds the localhost sidecar router consumed by the Go bridge.
pub fn sidecar_router(client: InlineClient) -> Router {
    sidecar_router_with_state(AdapterHttpState::new(client))
}

/// Builds the localhost sidecar router with prestarted shared state.
pub fn sidecar_router_with_state(state: AdapterHttpState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/status", get(status))
        .route("/ws/events", get(events_ws))
        .route("/rpc", post(rpc))
        .route("/rpc/auth/start", post(rpc_auth_start))
        .route("/rpc/auth/verify", post(rpc_auth_verify))
        .route("/rpc/resume", post(rpc_resume))
        .route("/rpc/connect", post(rpc_connect))
        .route("/rpc/logout", post(rpc_logout))
        .route("/rpc/dialogs", post(rpc_dialogs))
        .route("/rpc/state/dialogs", post(rpc_cached_dialogs))
        .route("/rpc/state/account", get(rpc_account_state))
        .route("/rpc/state/chat", post(rpc_chat_state))
        .route("/rpc/state/chat/history", post(rpc_cached_history))
        .route("/rpc/history", post(rpc_history))
        .route("/rpc/chat/participants", post(rpc_chat_participants))
        .route("/rpc/chat/participants/add", post(rpc_add_chat_participant))
        .route(
            "/rpc/chat/participants/remove",
            post(rpc_remove_chat_participant),
        )
        .route("/rpc/chat/info", post(rpc_update_chat_info))
        .route("/rpc/chat/delete", post(rpc_delete_chat))
        .route("/rpc/chat/create-dm", post(rpc_create_dm))
        .route("/rpc/chat/create-thread", post(rpc_create_thread))
        .route(
            "/rpc/chat/create-reply-thread",
            post(rpc_create_reply_thread),
        )
        .route("/rpc/send", post(rpc_send_text))
        .route("/rpc/send_text", post(rpc_send_text))
        .route("/rpc/edit", post(rpc_edit_message))
        .route("/rpc/delete", post(rpc_delete_message))
        .route("/rpc/react", post(rpc_react))
        .route("/rpc/read", post(rpc_read))
        .route("/rpc/marked-unread", post(rpc_set_marked_unread))
        .route(
            "/rpc/dialog/notifications",
            post(rpc_update_dialog_notifications),
        )
        .route("/rpc/typing", post(rpc_typing))
        .route("/rpc/events/ack", post(rpc_events_ack))
        .route("/rpc/upload", post(rpc_upload))
        .layer(DefaultBodyLimit::max(SIDECAR_UPLOAD_BODY_LIMIT_BYTES))
        .with_state(state)
}

/// Serves the localhost adapter router on an existing TCP listener.
pub async fn serve_adapter_http(
    listener: TcpListener,
    client: InlineClient,
) -> std::io::Result<()> {
    let local_addr = listener.local_addr().ok();
    if let Some(addr) = local_addr {
        log::info!("starting Matrix Inline adapter HTTP transport on {addr}");
    } else {
        log::info!("starting Matrix Inline adapter HTTP transport");
    }
    axum::serve(listener, sidecar_router(client)).await
}

/// Serves a prestarted adapter state on an existing TCP listener.
pub async fn serve_adapter_http_state(
    listener: TcpListener,
    state: AdapterHttpState,
) -> std::io::Result<()> {
    if let Ok(addr) = listener.local_addr() {
        log::info!("starting Matrix Inline adapter HTTP transport on {addr}");
    }
    axum::serve(listener, sidecar_router_with_state(state)).await
}

/// Binds and serves the localhost adapter router.
pub async fn bind_adapter_http(bind_addr: SocketAddr, client: InlineClient) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    serve_adapter_http(listener, client).await
}

/// Binds and serves a prestarted adapter HTTP state.
pub async fn bind_adapter_http_state(
    bind_addr: SocketAddr,
    state: AdapterHttpState,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    serve_adapter_http_state(listener, state).await
}

async fn health(State(state): State<AdapterHttpState>, headers: HeaderMap) -> Response {
    let requested_namespace = headers
        .get(SESSION_NAMESPACE_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let status = if let Some(namespace) = requested_namespace {
        match state.client_for_namespace(namespace).await {
            Ok(client) => client.status(),
            Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
        }
    } else {
        let clients = state.clients.lock().expect("adapter clients poisoned");
        clients
            .values()
            .map(InlineClient::status)
            .find(|status| matches!(status, ClientStatus::Connected | ClientStatus::Reconnecting))
            .or_else(|| clients.values().next().map(InlineClient::status))
            .unwrap_or(ClientStatus::Disconnected)
    };
    let event_namespace = requested_namespace
        .map(ToOwned::to_owned)
        .or_else(|| state.event_store.active_namespace().ok())
        .unwrap_or_else(|| "default".to_owned());
    let event_generation = match state.event_store.generation(&event_namespace) {
        Ok(generation) => generation,
        Err(error) => {
            log::error!("failed to load adapter event generation: {error}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    Json(SidecarHealth {
        ok: true,
        protocol: ProtocolInfo::current(),
        status,
        event_generation,
    })
    .into_response()
}

async fn status(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::Status).await
}

async fn rpc(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<SidecarRequest>,
) -> Response {
    dispatch_request(state, client, request).await
}

async fn rpc_auth_start(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<AuthStartRequest>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::AuthStart(request)).await
}

async fn rpc_auth_verify(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<AuthVerifyRequest>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::AuthVerify(request)).await
}

async fn rpc_resume(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::Resume).await
}

async fn rpc_connect(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<ConnectRequest>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::Connect(request)).await
}

async fn rpc_logout(
    AdapterRequestNamespace(namespace): AdapterRequestNamespace,
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
) -> Response {
    let request_id = state.next_request_id();
    match client.logout().await {
        Ok(()) => {
            state.release_client(&namespace, client).await;
            Json(SidecarResponse::ok(request_id, SidecarResult::Empty)).into_response()
        }
        Err(error) => request_error_response(request_id, error),
    }
}

async fn rpc_dialogs(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<DialogsRequest>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::Dialogs(request)).await
}

async fn rpc_cached_dialogs(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<DialogsRequest>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::CachedDialogs(request)).await
}

async fn rpc_account_state(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::AccountState).await
}

async fn rpc_chat_state(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<ChatStateRequest>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::ChatState(request)).await
}

async fn rpc_history(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<HistoryRequest>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::History(request)).await
}

async fn rpc_cached_history(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<HistoryRequest>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::CachedHistory(request)).await
}

async fn rpc_chat_participants(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<ChatParticipantsRequest>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::ChatParticipants(request)).await
}

async fn rpc_add_chat_participant(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<inline_client::AddChatParticipantRequest>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::AddChatParticipant(request)).await
}

async fn rpc_remove_chat_participant(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<inline_client::RemoveChatParticipantRequest>,
) -> Response {
    dispatch_command(
        state,
        client,
        SidecarCommand::RemoveChatParticipant(request),
    )
    .await
}

async fn rpc_update_chat_info(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<inline_client::UpdateChatInfoRequest>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::UpdateChatInfo(request)).await
}

async fn rpc_delete_chat(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<inline_client::DeleteChatRequest>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::DeleteChat(request)).await
}

async fn rpc_create_dm(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<CreateDmRequest>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::CreateDm(request)).await
}

async fn rpc_create_thread(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<CreateThreadRequest>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::CreateThread(request)).await
}

async fn rpc_create_reply_thread(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<CreateReplyThreadRequest>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::CreateReplyThread(request)).await
}

async fn rpc_send_text(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<SendTextRequest>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::SendText(request)).await
}

async fn rpc_edit_message(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<EditMessageRequest>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::EditMessage(request)).await
}

async fn rpc_delete_message(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<DeleteMessageRequest>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::DeleteMessage(request)).await
}

async fn rpc_react(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<ReactRequest>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::React(request)).await
}

async fn rpc_read(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<ReadRequest>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::Read(request)).await
}

async fn rpc_set_marked_unread(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<inline_client::SetMarkedUnreadRequest>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::SetMarkedUnread(request)).await
}

async fn rpc_update_dialog_notifications(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<inline_client::UpdateDialogNotificationsRequest>,
) -> Response {
    dispatch_command(
        state,
        client,
        SidecarCommand::UpdateDialogNotifications(request),
    )
    .await
}

async fn rpc_typing(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    Json(request): Json<TypingRequest>,
) -> Response {
    dispatch_command(state, client, SidecarCommand::Typing(request)).await
}

async fn rpc_upload(
    AdapterRequestClient(client): AdapterRequestClient,
    State(state): State<AdapterHttpState>,
    mut multipart: Multipart,
) -> Response {
    let request_id = state.next_request_id();
    let mut metadata: Option<UploadRequest> = None;
    let mut bytes: Option<Vec<u8>> = None;
    let mut multipart_file_name: Option<String> = None;

    loop {
        let field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => break,
            Err(error) => {
                return upload_error_response(
                    request_id,
                    format!("invalid upload multipart body: {error}"),
                );
            }
        };
        let name = field.name().map(ToOwned::to_owned);
        match name.as_deref() {
            Some("metadata") => {
                let text = match field.text().await {
                    Ok(text) => text,
                    Err(error) => {
                        return upload_error_response(
                            request_id,
                            format!("invalid upload metadata field: {error}"),
                        );
                    }
                };
                match serde_json::from_str::<UploadRequest>(&text) {
                    Ok(request) => metadata = Some(request),
                    Err(error) => {
                        return upload_error_response(
                            request_id,
                            format!("decode upload metadata: {error}"),
                        );
                    }
                }
            }
            Some("file") => {
                multipart_file_name = field.file_name().map(ToOwned::to_owned);
                let data = match field.bytes().await {
                    Ok(data) => data,
                    Err(error) => {
                        return upload_error_response(
                            request_id,
                            format!("invalid upload file field: {error}"),
                        );
                    }
                };
                bytes = Some(data.to_vec());
            }
            Some(_) | None => {}
        }
    }

    let mut request = match metadata {
        Some(request) => request,
        None => return upload_error_response(request_id, "missing upload metadata field"),
    };
    if request.file_name.is_none() {
        request.file_name = multipart_file_name;
    }
    let bytes = match bytes {
        Some(bytes) if !bytes.is_empty() => bytes,
        Some(_) => return upload_error_response(request_id, "upload file field is empty"),
        None => return upload_error_response(request_id, "missing upload file field"),
    };

    match client.send_media(request, bytes).await {
        Ok(mutation) => Json(SidecarResponse::ok(
            request_id,
            SidecarResult::Message(mutation),
        ))
        .into_response(),
        Err(error) => request_error_response(request_id, error),
    }
}

async fn dispatch_command(
    state: AdapterHttpState,
    client: InlineClient,
    command: SidecarCommand,
) -> Response {
    let request = SidecarRequest::current(state.next_request_id(), command);
    dispatch_request(state, client, request).await
}

async fn dispatch_request(
    _state: AdapterHttpState,
    client: InlineClient,
    request: SidecarRequest,
) -> Response {
    let request_id = request.id.clone();
    if request.protocol_version != PROTOCOL_VERSION {
        log::warn!(
            "adapter protocol mismatch: got {}, expected {}",
            request.protocol_version,
            PROTOCOL_VERSION
        );
        return Json(SidecarResponse::error(
            request_id,
            SidecarError::new(
                ClientErrorCategory::ProtocolMismatch,
                format!(
                    "unsupported adapter protocol version {}; expected {}",
                    request.protocol_version, PROTOCOL_VERSION
                ),
            ),
        ))
        .into_response();
    }

    log::debug!("handling adapter command: {}", request.command.kind());
    match handle_command(&client, request.command).await {
        Ok(result) => Json(SidecarResponse::ok(request_id, result)).into_response(),
        Err(error) => request_error_response(request_id, error),
    }
}

async fn handle_command(
    client: &InlineClient,
    command: SidecarCommand,
) -> Result<SidecarResult, ClientRequestError> {
    match command {
        SidecarCommand::Status => Ok(SidecarResult::Status(SidecarStatus::from_client(
            client.status_snapshot(),
        ))),
        SidecarCommand::AuthStart(auth) => {
            client.auth_start(auth).await.map(SidecarResult::AuthStart)
        }
        SidecarCommand::AuthVerify(auth) => client
            .auth_verify(auth)
            .await
            .map(|result| SidecarResult::AuthVerify(result.into())),
        SidecarCommand::Resume => {
            let snapshot = client.status_snapshot();
            if matches!(
                snapshot.status,
                ClientStatus::Connected | ClientStatus::Reconnecting
            ) {
                return Ok(SidecarResult::Status(SidecarStatus::from_client(snapshot)));
            }
            client
                .resume_session()
                .await
                .map(SidecarStatus::from_client)
                .map(SidecarResult::Status)
        }
        SidecarCommand::Connect(connect) => client
            .connect(connect)
            .await
            .map(SidecarStatus::from_client)
            .map(SidecarResult::Status),
        SidecarCommand::Logout => {
            client.logout().await?;
            Ok(SidecarResult::Empty)
        }
        SidecarCommand::Dialogs(dialogs) => {
            client.dialogs(dialogs).await.map(SidecarResult::Dialogs)
        }
        SidecarCommand::CachedDialogs(dialogs) => client
            .cached_dialogs(dialogs)
            .await
            .map(SidecarResult::Dialogs),
        SidecarCommand::AccountState => client
            .account_state()
            .await
            .map(SidecarResult::AccountState),
        SidecarCommand::ChatState(request) => client
            .chat_state(request.chat_id)
            .await
            .map(Box::new)
            .map(SidecarResult::ChatState),
        SidecarCommand::History(history) => {
            client.history(history).await.map(SidecarResult::History)
        }
        SidecarCommand::CachedHistory(history) => client
            .cached_history(history)
            .await
            .map(SidecarResult::History),
        SidecarCommand::ChatParticipants(participants) => client
            .chat_participants(participants)
            .await
            .map(SidecarResult::ChatParticipants),
        SidecarCommand::AddChatParticipant(request) => {
            client.add_chat_participant(request).await?;
            Ok(SidecarResult::Empty)
        }
        SidecarCommand::RemoveChatParticipant(request) => {
            client.remove_chat_participant(request).await?;
            Ok(SidecarResult::Empty)
        }
        SidecarCommand::UpdateChatInfo(request) => {
            client.update_chat_info(request).await?;
            Ok(SidecarResult::Empty)
        }
        SidecarCommand::DeleteChat(request) => {
            client.delete_chat(request).await?;
            Ok(SidecarResult::Empty)
        }
        SidecarCommand::CreateDm(request) => client
            .create_dm(request)
            .await
            .map(SidecarResult::CreatedChat),
        SidecarCommand::CreateThread(request) => client
            .create_thread(request)
            .await
            .map(SidecarResult::CreatedChat),
        SidecarCommand::CreateReplyThread(request) => client
            .create_reply_thread(request)
            .await
            .map(SidecarResult::CreatedChat),
        SidecarCommand::SendText(send) => client.send_text(send).await.map(SidecarResult::Message),
        SidecarCommand::EditMessage(edit) => {
            client.edit_message(edit).await?;
            Ok(SidecarResult::Empty)
        }
        SidecarCommand::DeleteMessage(delete) => {
            client.delete_message(delete).await?;
            Ok(SidecarResult::Empty)
        }
        SidecarCommand::React(react) => {
            client.react(react).await?;
            Ok(SidecarResult::Empty)
        }
        SidecarCommand::Read(read) => {
            client.read(read).await?;
            Ok(SidecarResult::Empty)
        }
        SidecarCommand::SetMarkedUnread(request) => {
            client.set_marked_unread(request).await?;
            Ok(SidecarResult::Empty)
        }
        SidecarCommand::UpdateDialogNotifications(request) => {
            client.update_dialog_notifications(request).await?;
            Ok(SidecarResult::Empty)
        }
        SidecarCommand::Typing(typing) => {
            client.typing(typing).await?;
            Ok(SidecarResult::Empty)
        }
        SidecarCommand::Upload(_) => Err(ClientRequestError::Backend(
            inline_client::BackendError::new(
                ClientErrorCategory::Unsupported,
                "upload bytes must be sent through the adapter upload transport",
            ),
        )),
    }
}

fn request_error_response(request_id: SidecarRequestId, error: ClientRequestError) -> Response {
    match error {
        ClientRequestError::Backend(error) => {
            Json(SidecarResponse::error(request_id, error.into())).into_response()
        }
        ClientRequestError::Command(error) => command_error_response(request_id, error),
        _ => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(SidecarResponse::error(
                request_id,
                SidecarError::new(
                    ClientErrorCategory::Internal,
                    "unhandled inline client request error",
                ),
            )),
        )
            .into_response(),
    }
}

fn command_error_response(request_id: SidecarRequestId, error: ClientCommandError) -> Response {
    log::warn!("adapter command dispatch failed: {error}");
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(SidecarResponse::error(
            request_id,
            SidecarError::new(ClientErrorCategory::Internal, error.to_string()),
        )),
    )
        .into_response()
}

fn upload_error_response(request_id: SidecarRequestId, message: impl Into<String>) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(SidecarResponse::error(
            request_id,
            SidecarError::new(ClientErrorCategory::InvalidInput, message.into()),
        )),
    )
        .into_response()
}

#[derive(Debug, Default, Deserialize)]
struct EventsQuery {
    #[serde(default)]
    after_sequence: u64,
    session_namespace: Option<String>,
    generation: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct EventsAckRequest {
    session_namespace: String,
    #[serde(default)]
    generation: String,
    sequence: u64,
}

#[derive(Debug, Serialize)]
struct EventsAckResponse {
    generation: String,
    acknowledged_sequence: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct EventsReplayError {
    error: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    requested_after_sequence: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    oldest_retained_sequence: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    latest_sequence: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    event_generation: Option<String>,
    #[serde(default)]
    reset_required: bool,
}

async fn rpc_events_ack(
    AdapterRequestNamespace(namespace): AdapterRequestNamespace,
    State(state): State<AdapterHttpState>,
    Json(request): Json<EventsAckRequest>,
) -> Response {
    if request.session_namespace.trim() != namespace {
        return (
            StatusCode::BAD_REQUEST,
            "event acknowledgement namespace does not match session header",
        )
            .into_response();
    }
    match state.event_store.acknowledge_for_generation(
        &namespace,
        Some(&request.generation),
        request.sequence,
    ) {
        Ok(()) => Json(EventsAckResponse {
            generation: request.generation,
            acknowledged_sequence: request.sequence,
        })
        .into_response(),
        Err(AdapterEventStoreError::AckAhead { .. }) => (
            StatusCode::CONFLICT,
            Json(EventsReplayError {
                error: "sidecar_event_ack_ahead".to_owned(),
                message: "acknowledgement is ahead of the latest adapter event".to_owned(),
                requested_after_sequence: None,
                oldest_retained_sequence: None,
                latest_sequence: None,
                event_generation: None,
                reset_required: false,
            }),
        )
            .into_response(),
        Err(AdapterEventStoreError::GenerationChanged {
            generation,
            latest_sequence,
        }) => event_reset_response(generation, latest_sequence),
        Err(error) => {
            log::error!("failed to acknowledge adapter events: {error}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(EventsReplayError {
                    error: "sidecar_event_store_failed".to_owned(),
                    message: "adapter event acknowledgement failed".to_owned(),
                    requested_after_sequence: None,
                    oldest_retained_sequence: None,
                    latest_sequence: None,
                    event_generation: None,
                    reset_required: false,
                }),
            )
                .into_response()
        }
    }
}

async fn events_ws(
    AdapterRequestNamespace(namespace): AdapterRequestNamespace,
    State(state): State<AdapterHttpState>,
    Query(query): Query<EventsQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    if query
        .session_namespace
        .as_deref()
        .map(str::trim)
        .filter(|requested| !requested.is_empty())
        .is_some_and(|requested| requested != namespace)
    {
        return (
            StatusCode::BAD_REQUEST,
            "event replay namespace does not match session header",
        )
            .into_response();
    }
    let live = state.subscribe_events();
    let replay = match state.event_store.replay_for_generation(
        &namespace,
        query.generation.as_deref(),
        query.after_sequence,
    ) {
        Ok(replay) => replay,
        Err(AdapterEventStoreError::ReplayGap {
            after_sequence,
            oldest_sequence,
            latest_sequence,
        }) => {
            let generation = state.event_store.generation(&namespace).ok();
            return replay_gap_response(
                after_sequence,
                oldest_sequence,
                latest_sequence,
                generation,
            );
        }
        Err(AdapterEventStoreError::GenerationChanged {
            generation,
            latest_sequence,
        })
        | Err(AdapterEventStoreError::CursorAheadReset {
            generation,
            latest_sequence,
            ..
        }) => return event_reset_response(generation, latest_sequence),
        Err(error) => {
            log::error!("failed to load adapter event replay: {error}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    ws.on_upgrade(move |socket| {
        event_socket(state, socket, namespace, query.after_sequence, replay, live)
    })
    .into_response()
}

fn replay_gap_response(
    after_sequence: u64,
    oldest_sequence: Option<u64>,
    latest_sequence: u64,
    generation: Option<String>,
) -> Response {
    (
        StatusCode::GONE,
        Json(EventsReplayError {
            error: "sidecar_event_replay_unavailable".to_owned(),
            message: "requested adapter event history is no longer retained".to_owned(),
            requested_after_sequence: Some(after_sequence),
            oldest_retained_sequence: oldest_sequence,
            latest_sequence: Some(latest_sequence),
            event_generation: generation,
            reset_required: false,
        }),
    )
        .into_response()
}

fn event_reset_response(generation: String, latest_sequence: u64) -> Response {
    (
        StatusCode::GONE,
        Json(EventsReplayError {
            error: "sidecar_event_generation_reset".to_owned(),
            message: "adapter event log changed; bridge reconciliation is required".to_owned(),
            requested_after_sequence: None,
            oldest_retained_sequence: None,
            latest_sequence: Some(latest_sequence),
            event_generation: Some(generation),
            reset_required: true,
        }),
    )
        .into_response()
}

async fn event_socket(
    state: AdapterHttpState,
    mut socket: WebSocket,
    namespace: String,
    mut last_sent: u64,
    replay: Vec<SidecarEventEnvelope>,
    mut events: broadcast::Receiver<SidecarEventEnvelope>,
) {
    for envelope in replay {
        if send_event_envelope(&mut socket, &envelope).await.is_err() {
            return;
        }
        if let Some(sequence) = envelope.sequence {
            last_sent = last_sent.max(sequence);
        }
    }

    loop {
        let envelope = match events.recv().await {
            Ok(envelope) => envelope,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                log::warn!(
                    "adapter websocket lagged by {skipped} live events; replaying after {last_sent}"
                );
                let replay = match state.event_store.replay(&namespace, last_sent) {
                    Ok(replay) => replay,
                    Err(error) => {
                        log::error!("adapter websocket replay failed after lag: {error}");
                        break;
                    }
                };
                for envelope in replay {
                    if send_event_envelope(&mut socket, &envelope).await.is_err() {
                        return;
                    }
                    if let Some(sequence) = envelope.sequence {
                        last_sent = last_sent.max(sequence);
                    }
                }
                continue;
            }
        };
        if envelope.session_namespace != namespace {
            continue;
        }
        if envelope
            .sequence
            .is_some_and(|sequence| sequence <= last_sent)
        {
            continue;
        }
        if send_event_envelope(&mut socket, &envelope).await.is_err() {
            break;
        }
        if let Some(sequence) = envelope.sequence {
            last_sent = sequence;
        }
    }
}

async fn send_event_envelope(
    socket: &mut WebSocket,
    envelope: &SidecarEventEnvelope,
) -> Result<(), ()> {
    let payload = serde_json::to_string(envelope).map_err(|error| {
        log::error!("failed to serialize adapter event envelope: {error}");
    })?;
    socket
        .send(Message::Text(payload.into()))
        .await
        .map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use inline_client::{
        AuthCredential, AuthToken, ChatParticipantRecord, InMemoryBackend, InlineId, PeerRef,
    };
    use tower::ServiceExt;

    use crate::protocol::SidecarOutcome;

    use super::*;

    async fn json_response<T: serde::de::DeserializeOwned>(response: Response) -> T {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn health_endpoint_reports_protocol_and_status() {
        let client = InlineClient::builder()
            .initial_status(inline_client::ClientStatus::Connected)
            .build()
            .spawn();
        let app = sidecar_router(client);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let health: SidecarHealth = json_response(response).await;
        assert!(health.ok);
        assert_eq!(health.protocol.protocol_version, PROTOCOL_VERSION);
        assert_eq!(health.status, inline_client::ClientStatus::Connected);
    }

    #[tokio::test]
    async fn event_collector_persists_lossless_events_without_a_websocket() {
        let client = InlineClient::builder().build().spawn();
        let state = AdapterHttpState::new(client.clone());

        client
            .set_status(inline_client::ClientStatus::Connected, None)
            .await
            .unwrap();

        let replay = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let events = state.event_store().replay("test", 0).unwrap();
                if events.iter().any(|event| {
                    matches!(
                        &event.event,
                        inline_client::ClientEvent::StatusChanged {
                            status: inline_client::ClientStatus::Connected,
                            ..
                        }
                    )
                }) {
                    return events;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let connected = replay.iter().find(|event| {
            matches!(
                &event.event,
                inline_client::ClientEvent::StatusChanged {
                    status: inline_client::ClientStatus::Connected,
                    ..
                }
            )
        });
        assert!(connected.is_some());
    }

    #[tokio::test]
    async fn event_ack_endpoint_prunes_replay() {
        let client = InlineClient::builder().build().spawn();
        let state = AdapterHttpState::new(client);
        state
            .event_store()
            .append(inline_client::ClientEvent::ChatUpserted {
                chat_id: InlineId::new(7),
            })
            .unwrap();
        let app = sidecar_router_with_state(state.clone());
        let body = serde_json::to_vec(&EventsAckRequest {
            session_namespace: "test".to_owned(),
            generation: String::new(),
            sequence: 1,
        })
        .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rpc/events/ack")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(state.event_store().replay("test", 1).unwrap().is_empty());
    }

    #[tokio::test]
    async fn event_ack_rejects_namespace_mismatch() {
        let client = InlineClient::builder().build().spawn();
        let state = AdapterHttpState::new(client);
        state
            .event_store()
            .append(inline_client::ClientEvent::ChatUpserted {
                chat_id: InlineId::new(7),
            })
            .unwrap();
        let app = sidecar_router_with_state(state.clone());
        let body = serde_json::to_vec(&EventsAckRequest {
            session_namespace: "other-account".to_owned(),
            generation: String::new(),
            sequence: 1,
        })
        .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rpc/events/ack")
                    .header(SESSION_NAMESPACE_HEADER, "test")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(state.event_store().replay("test", 0).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn replay_gap_response_includes_reconciliation_cursor() {
        let response = replay_gap_response(0, Some(1), 7, Some("generation-1".to_owned()));

        assert_eq!(response.status(), StatusCode::GONE);
        let error: EventsReplayError = json_response(response).await;
        assert_eq!(error.requested_after_sequence, Some(0));
        assert_eq!(error.oldest_retained_sequence, Some(1));
        assert_eq!(error.latest_sequence, Some(7));
    }

    #[tokio::test]
    async fn status_endpoint_dispatches_through_adapter_protocol() {
        let client = InlineClient::builder()
            .initial_status(inline_client::ClientStatus::Connected)
            .build()
            .spawn();
        let app = sidecar_router(client);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let response: SidecarResponse = json_response(response).await;
        match response.outcome {
            SidecarOutcome::Ok(SidecarResult::Status(status)) => {
                assert_eq!(status.status, inline_client::ClientStatus::Connected);
                assert_eq!(status.protocol.protocol_version, PROTOCOL_VERSION);
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[tokio::test]
    async fn client_factory_isolates_and_reuses_account_namespaces() {
        let creations = Arc::new(AtomicUsize::new(0));
        let factory_creations = creations.clone();
        let factory: AdapterClientFactory = Arc::new(move |namespace| {
            let factory_creations = factory_creations.clone();
            Box::pin(async move {
                factory_creations.fetch_add(1, AtomicOrdering::SeqCst);
                let initial_status = if namespace == "alpha" {
                    ClientStatus::Connected
                } else {
                    ClientStatus::AuthRequired
                };
                Ok(AdapterClientRegistration {
                    client: InlineClient::builder()
                        .initial_status(initial_status)
                        .build()
                        .spawn(),
                    resume_stored_session: false,
                })
            })
        });
        let state = AdapterHttpState::with_client_factory(
            AdapterEventStore::open_in_memory("unused").unwrap(),
            factory,
        );
        let app = sidecar_router_with_state(state);

        for (namespace, expected) in [
            ("alpha", ClientStatus::Connected),
            ("alpha", ClientStatus::Connected),
            ("beta", ClientStatus::AuthRequired),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/status")
                        .header(SESSION_NAMESPACE_HEADER, namespace)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let response: SidecarResponse = json_response(response).await;
            match response.outcome {
                SidecarOutcome::Ok(SidecarResult::Status(status)) => {
                    assert_eq!(status.status, expected);
                }
                other => panic!("unexpected response: {other:?}"),
            }
        }
        assert_eq!(creations.load(AtomicOrdering::SeqCst), 2);

        let missing = app
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn client_factory_attaches_lossless_collector_before_resume() {
        let factory: AdapterClientFactory = Arc::new(move |_| {
            Box::pin(async move {
                Ok(AdapterClientRegistration {
                    client: InlineClient::builder().build().spawn(),
                    resume_stored_session: true,
                })
            })
        });
        let event_store = AdapterEventStore::open_in_memory("unused").unwrap();
        let state = AdapterHttpState::with_client_factory(event_store.clone(), factory);
        let app = sidecar_router_with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .header(SESSION_NAMESPACE_HEADER, "account-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let events = event_store.replay("account-1", 0).unwrap();
        assert!(events.iter().any(|event| matches!(
            event.event,
            inline_client::ClientEvent::StatusChanged {
                status: ClientStatus::AuthRequired,
                ..
            }
        )));
    }

    #[tokio::test]
    async fn logout_releases_factory_client_for_clean_recreation() {
        let creations = Arc::new(AtomicUsize::new(0));
        let factory_creations = creations.clone();
        let factory: AdapterClientFactory = Arc::new(move |_| {
            let factory_creations = factory_creations.clone();
            Box::pin(async move {
                factory_creations.fetch_add(1, AtomicOrdering::SeqCst);
                Ok(AdapterClientRegistration {
                    client: InlineClient::builder().build().spawn(),
                    resume_stored_session: false,
                })
            })
        });
        let state = AdapterHttpState::with_client_factory(
            AdapterEventStore::open_in_memory("unused").unwrap(),
            factory,
        );
        let app = sidecar_router_with_state(state);

        let status = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .header(SESSION_NAMESPACE_HEADER, "account-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::OK);

        let logout = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rpc/logout")
                    .header(SESSION_NAMESPACE_HEADER, "account-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(logout.status(), StatusCode::OK);

        let recreated = app
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .header(SESSION_NAMESPACE_HEADER, "account-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(recreated.status(), StatusCode::OK);
        assert_eq!(creations.load(AtomicOrdering::SeqCst), 2);
    }

    #[tokio::test]
    async fn resume_endpoint_returns_current_status_when_already_connected() {
        let client = InlineClient::builder()
            .initial_status(inline_client::ClientStatus::Connected)
            .build()
            .spawn();
        let app = sidecar_router(client);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rpc/resume")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let response: SidecarResponse = json_response(response).await;
        match response.outcome {
            SidecarOutcome::Ok(SidecarResult::Status(status)) => {
                assert_eq!(status.status, inline_client::ClientStatus::Connected);
                assert_eq!(status.protocol.protocol_version, PROTOCOL_VERSION);
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[tokio::test]
    async fn send_route_dispatches_to_native_client() {
        let client = InlineClient::builder().build().spawn();
        let app = sidecar_router(client);

        let connect_body = serde_json::to_vec(&ConnectRequest::new(AuthCredential::AccessToken {
            token: AuthToken::try_new("token").unwrap(),
        }))
        .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rpc/connect")
                    .header("content-type", "application/json")
                    .body(Body::from(connect_body))
                    .unwrap(),
            )
            .await
            .unwrap();

        let send_body = serde_json::to_vec(&SendTextRequest::new(
            PeerRef::Chat {
                chat_id: InlineId::new(7),
            },
            "hello",
        ))
        .unwrap();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rpc/send")
                    .header("content-type", "application/json")
                    .body(Body::from(send_body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let response: SidecarResponse = json_response(response).await;
        match response.outcome {
            SidecarOutcome::Ok(SidecarResult::Message(message)) => {
                assert_eq!(message.message_id, Some(InlineId::new(1)));
                assert_eq!(
                    message.state,
                    Some(inline_client::TransactionState::Completed)
                );
                assert!(message.failure.is_none());
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[tokio::test]
    async fn chat_participants_route_dispatches_to_native_client() {
        let backend = InMemoryBackend::new();
        backend.set_chat_participants(
            InlineId::new(7),
            vec![ChatParticipantRecord {
                user_id: InlineId::new(42),
                date: Some(100),
            }],
        );
        let client = InlineClient::builder().backend(backend).build().spawn();
        let app = sidecar_router(client);

        let connect_body = serde_json::to_vec(&ConnectRequest::new(AuthCredential::AccessToken {
            token: AuthToken::try_new("token").unwrap(),
        }))
        .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rpc/connect")
                    .header("content-type", "application/json")
                    .body(Body::from(connect_body))
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = serde_json::to_vec(&ChatParticipantsRequest {
            chat_id: InlineId::new(7),
        })
        .unwrap();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rpc/chat/participants")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let response: SidecarResponse = json_response(response).await;
        match response.outcome {
            SidecarOutcome::Ok(SidecarResult::ChatParticipants(page)) => {
                assert_eq!(page.participants.len(), 1);
                assert_eq!(page.participants[0].user_id, InlineId::new(42));
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[tokio::test]
    async fn chat_management_routes_dispatch_to_native_client() {
        let client = InlineClient::builder()
            .backend(InMemoryBackend::new())
            .build()
            .spawn();
        let app = sidecar_router(client);
        let connect_body = serde_json::to_vec(&ConnectRequest::new(AuthCredential::AccessToken {
            token: AuthToken::try_new("token").unwrap(),
        }))
        .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rpc/connect")
                    .header("content-type", "application/json")
                    .body(Body::from(connect_body))
                    .unwrap(),
            )
            .await
            .unwrap();

        let requests = [
            (
                "/rpc/chat/participants/add",
                serde_json::to_vec(&inline_client::AddChatParticipantRequest {
                    chat_id: InlineId::new(7),
                    user_id: InlineId::new(42),
                })
                .unwrap(),
            ),
            (
                "/rpc/chat/info",
                serde_json::to_vec(&inline_client::UpdateChatInfoRequest {
                    chat_id: InlineId::new(7),
                    title: Some("Renamed".to_owned()),
                    emoji: None,
                })
                .unwrap(),
            ),
            (
                "/rpc/marked-unread",
                serde_json::to_vec(&inline_client::SetMarkedUnreadRequest {
                    chat_id: InlineId::new(7),
                    unread: true,
                })
                .unwrap(),
            ),
            (
                "/rpc/dialog/notifications",
                serde_json::to_vec(&inline_client::UpdateDialogNotificationsRequest {
                    chat_id: InlineId::new(7),
                    mode: Some(inline_client::DialogNotificationMode::None),
                })
                .unwrap(),
            ),
            (
                "/rpc/chat/participants/remove",
                serde_json::to_vec(&inline_client::RemoveChatParticipantRequest {
                    chat_id: InlineId::new(7),
                    user_id: InlineId::new(42),
                })
                .unwrap(),
            ),
            (
                "/rpc/chat/delete",
                serde_json::to_vec(&inline_client::DeleteChatRequest {
                    chat_id: InlineId::new(7),
                })
                .unwrap(),
            ),
        ];

        for (uri, body) in requests {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(uri)
                        .header("content-type", "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "route {uri}");
            let response: SidecarResponse = json_response(response).await;
            assert!(
                matches!(&response.outcome, SidecarOutcome::Ok(SidecarResult::Empty)),
                "route {uri} returned {:?}",
                response.outcome
            );
        }
    }
}
