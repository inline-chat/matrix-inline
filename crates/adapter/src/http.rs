//! Localhost HTTP/WebSocket transport for the Matrix bridge adapter.

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use axum::{
    Json, Router,
    extract::{
        DefaultBodyLimit, Multipart, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
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
use tokio::net::TcpListener;

use crate::protocol::{
    PROTOCOL_VERSION, ProtocolInfo, SidecarCommand, SidecarError, SidecarEventEnvelope,
    SidecarHealth, SidecarRequest, SidecarRequestId, SidecarResponse, SidecarResult, SidecarStatus,
};

const SIDECAR_UPLOAD_BODY_LIMIT_BYTES: usize = 101 * 1024 * 1024;

/// Shared HTTP adapter state.
#[derive(Clone, Debug)]
pub struct AdapterHttpState {
    client: InlineClient,
    request_sequence: Arc<AtomicU64>,
    event_sequence: Arc<AtomicU64>,
}

impl AdapterHttpState {
    /// Creates HTTP adapter state for a client handle.
    pub fn new(client: InlineClient) -> Self {
        Self {
            client,
            request_sequence: Arc::new(AtomicU64::new(0)),
            event_sequence: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Returns the underlying client handle.
    pub fn client(&self) -> &InlineClient {
        &self.client
    }

    fn next_request_id(&self) -> SidecarRequestId {
        let sequence = self.request_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        SidecarRequestId::try_new(format!("http-{sequence}"))
            .expect("generated request ID should be valid")
    }

    fn next_event_sequence(&self) -> u64 {
        self.event_sequence.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// Builds the localhost sidecar router consumed by the Go bridge.
pub fn sidecar_router(client: InlineClient) -> Router {
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
        .route("/rpc/history", post(rpc_history))
        .route("/rpc/chat/participants", post(rpc_chat_participants))
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
        .route("/rpc/typing", post(rpc_typing))
        .route("/rpc/upload", post(rpc_upload))
        .layer(DefaultBodyLimit::max(SIDECAR_UPLOAD_BODY_LIMIT_BYTES))
        .with_state(AdapterHttpState::new(client))
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

/// Binds and serves the localhost adapter router.
pub async fn bind_adapter_http(bind_addr: SocketAddr, client: InlineClient) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    serve_adapter_http(listener, client).await
}

async fn health(State(state): State<AdapterHttpState>) -> Json<SidecarHealth> {
    Json(SidecarHealth {
        ok: true,
        protocol: ProtocolInfo::current(),
        status: state.client.status(),
    })
}

async fn status(State(state): State<AdapterHttpState>) -> Response {
    dispatch_command(state, SidecarCommand::Status).await
}

async fn rpc(
    State(state): State<AdapterHttpState>,
    Json(request): Json<SidecarRequest>,
) -> Response {
    dispatch_request(state, request).await
}

async fn rpc_auth_start(
    State(state): State<AdapterHttpState>,
    Json(request): Json<AuthStartRequest>,
) -> Response {
    dispatch_command(state, SidecarCommand::AuthStart(request)).await
}

async fn rpc_auth_verify(
    State(state): State<AdapterHttpState>,
    Json(request): Json<AuthVerifyRequest>,
) -> Response {
    dispatch_command(state, SidecarCommand::AuthVerify(request)).await
}

async fn rpc_resume(State(state): State<AdapterHttpState>) -> Response {
    dispatch_command(state, SidecarCommand::Resume).await
}

async fn rpc_connect(
    State(state): State<AdapterHttpState>,
    Json(request): Json<ConnectRequest>,
) -> Response {
    dispatch_command(state, SidecarCommand::Connect(request)).await
}

async fn rpc_logout(State(state): State<AdapterHttpState>) -> Response {
    dispatch_command(state, SidecarCommand::Logout).await
}

async fn rpc_dialogs(
    State(state): State<AdapterHttpState>,
    Json(request): Json<DialogsRequest>,
) -> Response {
    dispatch_command(state, SidecarCommand::Dialogs(request)).await
}

async fn rpc_history(
    State(state): State<AdapterHttpState>,
    Json(request): Json<HistoryRequest>,
) -> Response {
    dispatch_command(state, SidecarCommand::History(request)).await
}

async fn rpc_chat_participants(
    State(state): State<AdapterHttpState>,
    Json(request): Json<ChatParticipantsRequest>,
) -> Response {
    dispatch_command(state, SidecarCommand::ChatParticipants(request)).await
}

async fn rpc_create_dm(
    State(state): State<AdapterHttpState>,
    Json(request): Json<CreateDmRequest>,
) -> Response {
    dispatch_command(state, SidecarCommand::CreateDm(request)).await
}

async fn rpc_create_thread(
    State(state): State<AdapterHttpState>,
    Json(request): Json<CreateThreadRequest>,
) -> Response {
    dispatch_command(state, SidecarCommand::CreateThread(request)).await
}

async fn rpc_create_reply_thread(
    State(state): State<AdapterHttpState>,
    Json(request): Json<CreateReplyThreadRequest>,
) -> Response {
    dispatch_command(state, SidecarCommand::CreateReplyThread(request)).await
}

async fn rpc_send_text(
    State(state): State<AdapterHttpState>,
    Json(request): Json<SendTextRequest>,
) -> Response {
    dispatch_command(state, SidecarCommand::SendText(request)).await
}

async fn rpc_edit_message(
    State(state): State<AdapterHttpState>,
    Json(request): Json<EditMessageRequest>,
) -> Response {
    dispatch_command(state, SidecarCommand::EditMessage(request)).await
}

async fn rpc_delete_message(
    State(state): State<AdapterHttpState>,
    Json(request): Json<DeleteMessageRequest>,
) -> Response {
    dispatch_command(state, SidecarCommand::DeleteMessage(request)).await
}

async fn rpc_react(
    State(state): State<AdapterHttpState>,
    Json(request): Json<ReactRequest>,
) -> Response {
    dispatch_command(state, SidecarCommand::React(request)).await
}

async fn rpc_read(
    State(state): State<AdapterHttpState>,
    Json(request): Json<ReadRequest>,
) -> Response {
    dispatch_command(state, SidecarCommand::Read(request)).await
}

async fn rpc_typing(
    State(state): State<AdapterHttpState>,
    Json(request): Json<TypingRequest>,
) -> Response {
    dispatch_command(state, SidecarCommand::Typing(request)).await
}

async fn rpc_upload(State(state): State<AdapterHttpState>, mut multipart: Multipart) -> Response {
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

    match state.client.send_media(request, bytes).await {
        Ok(mutation) => Json(SidecarResponse::ok(
            request_id,
            SidecarResult::Message(mutation),
        ))
        .into_response(),
        Err(error) => request_error_response(request_id, error),
    }
}

async fn dispatch_command(state: AdapterHttpState, command: SidecarCommand) -> Response {
    let request = SidecarRequest::current(state.next_request_id(), command);
    dispatch_request(state, request).await
}

async fn dispatch_request(state: AdapterHttpState, request: SidecarRequest) -> Response {
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
    match handle_command(&state.client, request.command).await {
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
        SidecarCommand::History(history) => {
            client.history(history).await.map(SidecarResult::History)
        }
        SidecarCommand::ChatParticipants(participants) => client
            .chat_participants(participants)
            .await
            .map(SidecarResult::ChatParticipants),
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
        ClientRequestError::Backend(error) => Json(SidecarResponse::error(
            request_id,
            SidecarError::new(error.category, error.message),
        ))
        .into_response(),
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

async fn events_ws(State(state): State<AdapterHttpState>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| event_socket(state, socket))
        .into_response()
}

async fn event_socket(state: AdapterHttpState, mut socket: WebSocket) {
    let mut events = state.client.subscribe();
    loop {
        let event = match events.recv().await {
            Ok(event) => event,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                log::warn!("adapter event stream lagged; skipped {skipped} events");
                continue;
            }
        };
        let envelope =
            SidecarEventEnvelope::current(event).with_sequence(state.next_event_sequence());

        let payload = match serde_json::to_string(&envelope) {
            Ok(payload) => payload,
            Err(error) => {
                log::error!("failed to serialize adapter event envelope: {error}");
                continue;
            }
        };

        if socket.send(Message::Text(payload.into())).await.is_err() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
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
}
