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

/// Versioned sidecar protocol owned by the bridge adapter.
pub mod protocol;

pub use http::{AdapterHttpState, bind_adapter_http, serve_adapter_http, sidecar_router};
pub use protocol::{
    PROTOCOL_VERSION, ProtocolInfo, SidecarCommand, SidecarError, SidecarEventEnvelope,
    SidecarHealth, SidecarOutcome, SidecarRequest, SidecarRequestId, SidecarResponse,
    SidecarResult, SidecarStatus,
};
