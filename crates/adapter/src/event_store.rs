//! Durable adapter-to-bridge event replay storage.

use std::{
    fmt,
    path::Path,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use inline_client::{ClientEvent, EventReliability};
use rusqlite::{Connection, OptionalExtension, params};

use crate::protocol::SidecarEventEnvelope;

const DEFAULT_NAMESPACE: &str = "default";
const MAX_RETAINED_EVENTS_PER_NAMESPACE: u64 = 10_000;

/// Error returned by durable sidecar event storage.
#[derive(Debug, thiserror::Error)]
pub enum AdapterEventStoreError {
    /// SQLite persistence failed.
    #[error("sidecar event database operation failed: {0}")]
    Database(#[from] rusqlite::Error),
    /// A persisted event envelope could not be encoded or decoded.
    #[error("sidecar event payload operation failed: {0}")]
    Payload(#[from] serde_json::Error),
    /// The requested replay cursor is older than retained event history.
    #[error(
        "sidecar event replay gap after sequence {after_sequence}; oldest retained is {oldest_sequence:?}, latest assigned is {latest_sequence}"
    )]
    ReplayGap {
        /// Cursor requested by the bridge.
        after_sequence: u64,
        /// Oldest retained event, if any.
        oldest_sequence: Option<u64>,
        /// Latest sequence ever assigned in this namespace.
        latest_sequence: u64,
    },
    /// The bridge tried to acknowledge a sequence that has not been assigned.
    #[error("cannot acknowledge sidecar sequence {sequence}; latest assigned is {latest_sequence}")]
    AckAhead {
        /// Invalid acknowledgement sequence.
        sequence: u64,
        /// Latest assigned sequence.
        latest_sequence: u64,
    },
}

/// SQLite-backed replay log shared by adapter HTTP connections.
#[derive(Clone)]
pub struct AdapterEventStore {
    connection: Arc<Mutex<Connection>>,
    namespace: Arc<Mutex<String>>,
    read_client_session: bool,
}

impl fmt::Debug for AdapterEventStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdapterEventStore")
            .field("connection", &"<sqlite>")
            .field("read_client_session", &self.read_client_session)
            .finish()
    }
}

impl AdapterEventStore {
    /// Opens the event log beside the client tables in the adapter SQLite file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, AdapterEventStoreError> {
        Self::from_connection(Connection::open(path)?, DEFAULT_NAMESPACE, true)
    }

    /// Opens an in-memory event log with a fixed namespace.
    pub fn open_in_memory(namespace: impl Into<String>) -> Result<Self, AdapterEventStoreError> {
        Self::from_connection(Connection::open_in_memory()?, namespace, false)
    }

    fn from_connection(
        connection: Connection,
        namespace: impl Into<String>,
        read_client_session: bool,
    ) -> Result<Self, AdapterEventStoreError> {
        migrate(&connection)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            namespace: Arc::new(Mutex::new(normalize_namespace(&namespace.into()))),
            read_client_session,
        })
    }

    /// Returns the current client session namespace, retaining the last known
    /// value across logout status events.
    pub fn active_namespace(&self) -> Result<String, AdapterEventStoreError> {
        if self.read_client_session {
            let connection = self.connection.lock().expect("event store poisoned");
            let session_namespace = connection
                .query_row(
                    "SELECT account_namespace FROM sessions WHERE id = 1",
                    [],
                    |row| row.get::<_, Option<String>>(0),
                )
                .optional()?
                .flatten();
            drop(connection);
            if let Some(namespace) = session_namespace
                .map(|namespace| normalize_namespace(&namespace))
                .filter(|namespace| !namespace.is_empty())
            {
                *self.namespace.lock().expect("event namespace poisoned") = namespace;
            }
        }
        Ok(self
            .namespace
            .lock()
            .expect("event namespace poisoned")
            .clone())
    }

    /// Persists a lossless event before delivery. Best-effort events remain
    /// unsequenced and are returned for live delivery only.
    pub fn append(
        &self,
        event: ClientEvent,
    ) -> Result<SidecarEventEnvelope, AdapterEventStoreError> {
        let namespace = self.active_namespace()?;
        self.append_for_namespace(&namespace, event)
    }

    /// Persists an event in an explicitly selected account namespace.
    pub fn append_for_namespace(
        &self,
        namespace: &str,
        event: ClientEvent,
    ) -> Result<SidecarEventEnvelope, AdapterEventStoreError> {
        let namespace = normalize_namespace(namespace);
        if event.reliability() == EventReliability::BestEffort {
            return Ok(SidecarEventEnvelope::current(event).with_session_namespace(namespace));
        }

        let mut connection = self.connection.lock().expect("event store poisoned");
        let transaction = connection.transaction()?;
        let current = latest_sequence(&transaction, &namespace)?;
        let sequence = current.saturating_add(1);
        transaction.execute(
            "INSERT INTO sidecar_event_sequences (session_namespace, latest_sequence)
             VALUES (?1, ?2)
             ON CONFLICT(session_namespace) DO UPDATE SET
               latest_sequence = excluded.latest_sequence",
            params![namespace, sequence as i64],
        )?;
        let envelope = SidecarEventEnvelope::current(event)
            .with_session_namespace(namespace.clone())
            .with_sequence(sequence);
        let payload = serde_json::to_string(&envelope)?;
        transaction.execute(
            "INSERT INTO sidecar_events (
               session_namespace, sequence, reliability, payload_json, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                namespace,
                sequence as i64,
                "lossless",
                payload,
                now_seconds()
            ],
        )?;
        let retention_floor = sequence.saturating_sub(MAX_RETAINED_EVENTS_PER_NAMESPACE);
        if retention_floor > 0 {
            transaction.execute(
                "DELETE FROM sidecar_events
                 WHERE session_namespace = ?1 AND sequence <= ?2",
                params![namespace, retention_floor as i64],
            )?;
        }
        transaction.commit()?;
        log::debug!("persisted lossless adapter event sequence={sequence}");
        Ok(envelope)
    }

    /// Loads all retained events after a bridge cursor, rejecting retention gaps.
    pub fn replay(
        &self,
        namespace: &str,
        after_sequence: u64,
    ) -> Result<Vec<SidecarEventEnvelope>, AdapterEventStoreError> {
        let namespace = normalize_namespace(namespace);
        let connection = self.connection.lock().expect("event store poisoned");
        let latest = latest_sequence(&connection, &namespace)?;
        let oldest = connection
            .query_row(
                "SELECT MIN(sequence) FROM sidecar_events WHERE session_namespace = ?1",
                params![namespace],
                |row| row.get::<_, Option<i64>>(0),
            )?
            .and_then(|value| u64::try_from(value).ok());
        if latest > after_sequence
            && oldest.is_none_or(|oldest| oldest > after_sequence.saturating_add(1))
        {
            log::warn!(
                "adapter event replay gap after_sequence={after_sequence} oldest_sequence={oldest:?} latest_sequence={latest}"
            );
            return Err(AdapterEventStoreError::ReplayGap {
                after_sequence,
                oldest_sequence: oldest,
                latest_sequence: latest,
            });
        }

        let mut statement = connection.prepare(
            "SELECT payload_json FROM sidecar_events
             WHERE session_namespace = ?1 AND sequence > ?2
             ORDER BY sequence ASC",
        )?;
        let payloads = statement
            .query_map(params![namespace, after_sequence as i64], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let replay = payloads
            .into_iter()
            .map(|payload| serde_json::from_str(&payload).map_err(AdapterEventStoreError::from))
            .collect::<Result<Vec<_>, _>>()?;
        if !replay.is_empty() {
            log::debug!(
                "loaded adapter event replay after_sequence={after_sequence} count={}",
                replay.len()
            );
        }
        Ok(replay)
    }

    /// Acknowledges durable bridge progress and prunes delivered events.
    pub fn acknowledge(
        &self,
        namespace: &str,
        sequence: u64,
    ) -> Result<(), AdapterEventStoreError> {
        let namespace = normalize_namespace(namespace);
        let connection = self.connection.lock().expect("event store poisoned");
        let latest = latest_sequence(&connection, &namespace)?;
        if sequence > latest {
            return Err(AdapterEventStoreError::AckAhead {
                sequence,
                latest_sequence: latest,
            });
        }
        connection.execute(
            "DELETE FROM sidecar_events
             WHERE session_namespace = ?1 AND sequence <= ?2",
            params![namespace, sequence as i64],
        )?;
        log::debug!("acknowledged adapter events through sequence={sequence}");
        Ok(())
    }

    /// Removes all replay state for an intentionally released account.
    pub fn remove_namespace(&self, namespace: &str) -> Result<(), AdapterEventStoreError> {
        let namespace = normalize_namespace(namespace);
        let mut connection = self.connection.lock().expect("event store poisoned");
        let transaction = connection.transaction()?;
        transaction.execute(
            "DELETE FROM sidecar_events WHERE session_namespace = ?1",
            params![namespace],
        )?;
        transaction.execute(
            "DELETE FROM sidecar_event_sequences WHERE session_namespace = ?1",
            params![namespace],
        )?;
        transaction.commit()?;
        log::debug!("removed released adapter account replay state");
        Ok(())
    }
}

fn migrate(connection: &Connection) -> Result<(), rusqlite::Error> {
    connection.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS sidecar_event_sequences (
            session_namespace TEXT PRIMARY KEY,
            latest_sequence INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sidecar_events (
            session_namespace TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            reliability TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (session_namespace, sequence)
        );

        CREATE INDEX IF NOT EXISTS idx_sidecar_events_replay
            ON sidecar_events (session_namespace, sequence);
        ",
    )
}

fn latest_sequence(connection: &Connection, namespace: &str) -> Result<u64, rusqlite::Error> {
    connection
        .query_row(
            "SELECT latest_sequence FROM sidecar_event_sequences
             WHERE session_namespace = ?1",
            params![namespace],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map(|value| {
            value
                .and_then(|value| u64::try_from(value).ok())
                .unwrap_or_default()
        })
}

fn normalize_namespace(namespace: &str) -> String {
    let namespace = namespace.trim();
    if namespace.is_empty() {
        DEFAULT_NAMESPACE.to_owned()
    } else {
        namespace.to_owned()
    }
}

fn now_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use inline_client::{ClientStatus, InlineId};

    use super::*;

    #[test]
    fn lossless_events_are_sequenced_replayed_and_acknowledged() {
        let store = AdapterEventStore::open_in_memory("team").unwrap();
        let first = store
            .append(ClientEvent::ChatUpserted {
                chat_id: InlineId::new(7),
            })
            .unwrap();
        let second = store
            .append(ClientEvent::MessageDeleted {
                chat_id: InlineId::new(7),
                message_id: InlineId::new(11),
            })
            .unwrap();

        assert_eq!(first.sequence, Some(1));
        assert_eq!(second.sequence, Some(2));
        assert_eq!(first.session_namespace, "team");
        assert_eq!(
            store.replay("team", 0).unwrap(),
            vec![first, second.clone()]
        );

        store.acknowledge("team", 1).unwrap();
        assert_eq!(store.replay("team", 1).unwrap(), vec![second]);
        store.acknowledge("team", 2).unwrap();
        assert!(store.replay("team", 2).unwrap().is_empty());
    }

    #[test]
    fn best_effort_events_are_live_only_and_unsequenced() {
        let store = AdapterEventStore::open_in_memory("team").unwrap();
        let event = store
            .append(ClientEvent::Typing {
                chat_id: InlineId::new(7),
                user_id: InlineId::new(2),
                is_typing: true,
            })
            .unwrap();

        assert_eq!(event.sequence, None);
        assert!(store.replay("team", 0).unwrap().is_empty());
    }

    #[test]
    fn replay_detects_missing_acknowledged_history() {
        let store = AdapterEventStore::open_in_memory("team").unwrap();
        store
            .append(ClientEvent::StatusChanged {
                status: ClientStatus::Connected,
                failure: None,
            })
            .unwrap();
        store.acknowledge("team", 1).unwrap();

        assert!(matches!(
            store.replay("team", 0),
            Err(AdapterEventStoreError::ReplayGap {
                after_sequence: 0,
                latest_sequence: 1,
                ..
            })
        ));
    }

    #[test]
    fn released_namespace_removes_events_and_sequence_state() {
        let store = AdapterEventStore::open_in_memory("team").unwrap();
        store
            .append(ClientEvent::ChatUpserted {
                chat_id: InlineId::new(7),
            })
            .unwrap();

        store.remove_namespace("team").unwrap();

        assert!(store.replay("team", 0).unwrap().is_empty());
        store.acknowledge("team", 0).unwrap();
    }
}
