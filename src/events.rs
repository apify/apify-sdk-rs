//! Platform events: the Apify platform sends a run's events over a websocket
//! (`ACTOR_EVENTS_WEBSOCKET_URL`) as `{"name": ..., "data": ...}` messages. They are relayed to a
//! crawlee-rs [`EventManager`], like `PlatformEventManager` of the JS SDK does.

use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use crawlee::core::{Event, EventManager, SystemInfo};
use futures_util::StreamExt as _;
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

/// Names of the events the platform sends (`ACTOR_EVENT_NAMES` in `@apify/consts`).
pub mod names {
    pub const CPU_INFO: &str = "cpuInfo";
    pub const SYSTEM_INFO: &str = "systemInfo";
    pub const MIGRATING: &str = "migrating";
    pub const PERSIST_STATE: &str = "persistState";
    pub const ABORTING: &str = "aborting";
    pub const EXIT: &str = "exit";
}

/// The platform's `systemInfo` payload as a crawlee-rs [`SystemInfo`]. When the platform leaves
/// out `memTotalBytes`, `memory_limit_bytes` (the run's memory) stands in for it.
fn system_info(data: &Value, memory_limit_bytes: Option<u64>) -> SystemInfo {
    let created_at = data
        .get("createdAt")
        .and_then(Value::as_str)
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map_or_else(Utc::now, |date| date.with_timezone(&Utc));
    SystemInfo {
        created_at,
        cpu_current_usage: data.get("cpuCurrentUsage").and_then(Value::as_f64).unwrap_or(0.0),
        is_cpu_overloaded: data.get("isCpuOverloaded").and_then(Value::as_bool).unwrap_or(false),
        mem_total_bytes: data.get("memTotalBytes").and_then(Value::as_u64).or(memory_limit_bytes),
        mem_current_bytes: data.get("memCurrentBytes").and_then(Value::as_u64),
    }
}

/// Relays one platform message. Returns `false` when it is not a valid event.
pub(crate) fn relay(message: &str, events: &EventManager, memory_limit_bytes: Option<u64>) -> bool {
    let Ok(Value::Object(mut message)) = serde_json::from_str::<Value>(message) else { return false };
    let Some(Value::String(name)) = message.remove("name") else { return false };
    let data = message.remove("data").unwrap_or(Value::Null);
    match name.as_str() {
        names::PERSIST_STATE => {
            let is_migrating = data.get("isMigrating").and_then(Value::as_bool).unwrap_or(false);
            events.emit(Event::PersistState { is_migrating });
        }
        names::SYSTEM_INFO => events.emit(Event::SystemInfo(system_info(&data, memory_limit_bytes))),
        names::MIGRATING => {
            events.emit(Event::Migrating);
            // No more periodic saves: this one is the last before the run moves.
            events.stop_periodic_persist_state();
            events.emit(Event::PersistState { is_migrating: true });
        }
        names::ABORTING => events.emit(Event::Aborting),
        names::EXIT => events.emit(Event::Exit),
        _ => events.emit(Event::Custom { name, data }),
    }
    true
}

/// Close codes of a permanent condition, after which the connection is not reopened: the platform
/// sends 1008 for an unknown run or too many connections; the others are protocol failures.
const NON_RETRYABLE_CLOSE_CODES: [u16; 5] = [1002, 1003, 1007, 1008, 1010];
/// Close codes of a clean close, as opposed to a lost connection.
const CLEAN_CLOSE_CODES: [u16; 2] = [1000, 1001];
/// A connection open at least this long was healthy: when it drops, it is reopened at once.
const HEALTHY_CONNECTION_MIN_DURATION: Duration = Duration::from_secs(1);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

type Socket = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Delays between reconnection attempts, as in the JS SDK (after the Python `websockets` library):
/// a random one of up to 5 s, then from 3.1 s growing by 1.618 up to 90 s.
#[derive(Default)]
struct Backoff {
    next_millis: Option<f64>,
}

impl Backoff {
    fn next(&mut self) -> Duration {
        let millis = match self.next_millis {
            None => {
                self.next_millis = Some(3100.0);
                rand::random::<f64>() * 5000.0
            }
            Some(millis) => {
                self.next_millis = Some(if millis < 90_000.0 { millis * 1.618 } else { millis });
                millis.min(90_000.0)
            }
        };
        Duration::from_secs_f64(millis / 1000.0)
    }
}

async fn open(url: &str) -> Result<Socket, String> {
    match tokio::time::timeout(HANDSHAKE_TIMEOUT, tokio_tungstenite::connect_async(url)).await {
        Ok(Ok((socket, _))) => Ok(socket),
        Ok(Err(err)) => Err(err.to_string()),
        Err(_) => Err("Opening handshake has timed out".to_owned()),
    }
}

/// Relays the messages of `socket` until it closes, and returns the close code (1005 for a close
/// without one, 1006 for a connection lost without a close).
async fn relay_until_closed(mut socket: Socket, events: &EventManager, memory_limit_bytes: Option<u64>) -> u16 {
    while let Some(message) = socket.next().await {
        let text = match message {
            Ok(Message::Text(text)) => text.to_string(),
            Ok(Message::Binary(bytes)) => String::from_utf8_lossy(&bytes).into_owned(),
            Ok(Message::Close(frame)) => return frame.map_or(1005, |frame| frame.code.into()),
            Ok(_) => continue,
            Err(err) => {
                tracing::error!("Platform events websocket error: {err}");
                return 1006;
            }
        };
        if !text.is_empty() && !relay(&text, events, memory_limit_bytes) {
            tracing::error!("Cannot parse Actor event: {text}");
        }
    }
    1006
}

/// Connects to the platform's event websocket and relays its messages to `events`. Fails when the
/// first connection cannot be opened. A connection that drops is reopened, with backoff on
/// repeated rapid drops, unless it was closed for good; aborting the task stops it.
pub(crate) async fn connect(
    url: &str,
    events: EventManager,
    memory_limit_bytes: Option<u64>,
) -> Result<JoinHandle<()>, String> {
    let mut socket = open(url).await?;
    let url = url.to_owned();
    Ok(tokio::spawn(async move {
        let mut backoff: Option<Backoff> = None;
        loop {
            let opened_at = Instant::now();
            let code = relay_until_closed(socket, &events, memory_limit_bytes).await;
            if NON_RETRYABLE_CLOSE_CODES.contains(&code) {
                tracing::error!(
                    "Connection to platform events websocket was closed with a non-retryable code (code={code}); not \
                     reconnecting. No further platform events will be received for the rest of this run, so migration \
                     and abort handling (e.g. persisting state before a migration) is now disabled."
                );
                return;
            }
            if CLEAN_CLOSE_CODES.contains(&code) {
                tracing::info!("Connection to platform events websocket was closed (code={code}), reconnecting...");
            } else {
                tracing::warn!("Connection to platform events websocket was lost (code={code}), reconnecting...");
            }
            // A healthy connection is reopened at once; the first rapid drop too, the next ones back off.
            let mut delay = Duration::ZERO;
            if opened_at.elapsed() >= HEALTHY_CONNECTION_MIN_DURATION {
                backoff = None;
            } else if let Some(backoff) = &mut backoff {
                delay = backoff.next();
            } else {
                backoff = Some(Backoff::default());
            }
            tokio::time::sleep(delay).await;
            socket = loop {
                match open(&url).await {
                    Ok(socket) => {
                        tracing::info!("Reconnected to the platform events websocket.");
                        break socket;
                    }
                    Err(err) => tracing::warn!("Reconnecting to platform events websocket failed: {err}"),
                }
                tokio::time::sleep(backoff.get_or_insert_with(Backoff::default).next()).await;
            };
        }
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crawlee::core::EventKind;
    use parking_lot::Mutex;

    use super::*;

    fn recorder(events: &EventManager) -> Arc<Mutex<Vec<Event>>> {
        let seen = Arc::new(Mutex::new(Vec::new()));
        for kind in [
            EventKind::PersistState,
            EventKind::SystemInfo,
            EventKind::Migrating,
            EventKind::Aborting,
            EventKind::Custom,
        ] {
            let seen = seen.clone();
            events.on(kind, move |event| {
                seen.lock().push(event);
                async {}
            });
        }
        seen
    }

    #[tokio::test]
    async fn platform_messages_become_crawlee_events() {
        let events = EventManager::new(Duration::from_secs(60));
        let seen = recorder(&events);

        let info = r#"{"name":"systemInfo","data":{"createdAt":"2026-01-01T00:00:00.000Z","cpuCurrentUsage":42.5,
            "isCpuOverloaded":true,"memCurrentBytes":1024}}"#;
        assert!(relay(info, &events, Some(4096)));
        events.wait_for_all_listeners_to_complete().await;
        let Event::SystemInfo(info) = seen.lock()[0].clone() else { panic!("{:?}", seen.lock()) };
        assert_eq!((info.cpu_current_usage, info.is_cpu_overloaded), (42.5, true));
        assert_eq!((info.mem_current_bytes, info.mem_total_bytes), (Some(1024), Some(4096)));
        assert_eq!(info.created_at.to_rfc3339(), "2026-01-01T00:00:00+00:00");

        seen.lock().clear();
        assert!(relay(r#"{"name":"migrating","data":null}"#, &events, None));
        assert!(relay(r#"{"name":"cpuInfo","data":{"isCpuOverloaded":false}}"#, &events, None));
        assert!(!relay("not json", &events, None));
        events.wait_for_all_listeners_to_complete().await;
        let seen = seen.lock().clone();
        assert!(seen.contains(&Event::Migrating));
        assert!(seen.contains(&Event::PersistState { is_migrating: true }));
        assert!(seen.contains(&Event::Custom {
            name: "cpuInfo".to_owned(),
            data: serde_json::json!({ "isCpuOverloaded": false })
        }));
    }

    #[tokio::test]
    async fn a_dropped_connection_is_reopened_until_a_permanent_close() {
        use futures_util::SinkExt as _;
        use tokio_tungstenite::tungstenite::protocol::CloseFrame;
        use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let close = |code: CloseCode| Some(CloseFrame { code, reason: "".into() });
        tokio::spawn(async move {
            // The platform goes away, then delivers an event on the next connection and closes it for good.
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            socket.close(close(CloseCode::Away)).await.unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            socket.send(Message::text(r#"{"name":"aborting","data":null}"#)).await.unwrap();
            socket.close(close(CloseCode::Policy)).await.unwrap();
            while let Some(Ok(_)) = socket.next().await {}
        });

        let events = EventManager::new(Duration::from_secs(60));
        let seen = recorder(&events);
        let relaying = connect(&url, events.clone(), None).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), relaying).await.expect("not reopened after 1008").unwrap();
        events.wait_for_all_listeners_to_complete().await;
        assert_eq!(*seen.lock(), [Event::Aborting]);
    }

    #[tokio::test]
    async fn a_failed_first_connection_is_an_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        drop(listener);
        let events = EventManager::new(Duration::from_secs(60));
        assert!(connect(&url, events, None).await.is_err());
    }
}
