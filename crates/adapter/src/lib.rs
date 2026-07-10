#![warn(missing_docs)]
#![forbid(unsafe_code)]

//! Bridge-owned Rust adapter for `matrix-inline`.
//!
//! This crate owns the localhost HTTP/WebSocket protocol consumed by the Go
//! mautrix-go bridge. It depends on the native `inline-client` crate but keeps
//! Matrix/Beeper bridge protocol, process, and deployment concerns out of the
//! reusable Inline client library.

/// Localhost HTTP/WebSocket transport for the bridge adapter.
pub mod http;

/// Durable adapter-to-bridge event replay storage.
pub mod event_store;

/// Versioned sidecar protocol owned by the bridge adapter.
pub mod protocol;

pub use event_store::{AdapterEventStore, AdapterEventStoreError};
pub use http::{
    AdapterClientFactory, AdapterClientRegistration, AdapterHttpState, bind_adapter_http,
    bind_adapter_http_state, serve_adapter_http, serve_adapter_http_state, sidecar_router,
    sidecar_router_with_state,
};
pub use protocol::{
    ChatStateRequest, PROTOCOL_VERSION, ProtocolInfo, SidecarCommand, SidecarError,
    SidecarEventEnvelope, SidecarHealth, SidecarOutcome, SidecarRequest, SidecarRequestId,
    SidecarResponse, SidecarResult, SidecarStatus,
};
