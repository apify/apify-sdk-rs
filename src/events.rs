//! Platform events: the Apify platform sends a run's events over a websocket
//! (`ACTOR_EVENTS_WEBSOCKET_URL`) as `{"name": ..., "data": ...}` messages. They are relayed to a
//! crawlee-rs [`EventManager`], like `PlatformEventManager` of the JS SDK does.

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

/// Connects to the platform's event websocket and relays its messages to `events` until the
/// connection closes. As in the JS SDK, a closed connection is not reopened.
pub(crate) async fn connect(url: &str, events: EventManager, memory_limit_bytes: Option<u64>) -> JoinHandle<()> {
    let connection = tokio_tungstenite::connect_async(url).await;
    tokio::task::spawn_local(async move {
        let mut socket = match connection {
            Ok((socket, _)) => socket,
            Err(err) => {
                tracing::error!("Connecting to the platform events websocket failed: {err}");
                return;
            }
        };
        while let Some(message) = socket.next().await {
            let text = match message {
                Ok(Message::Text(text)) => text.to_string(),
                Ok(Message::Binary(bytes)) => String::from_utf8_lossy(&bytes).into_owned(),
                Ok(Message::Close(_)) => break,
                Ok(_) => continue,
                Err(err) => {
                    tracing::error!("The platform events websocket failed: {err}");
                    break;
                }
            };
            if !text.is_empty() && !relay(&text, &events, memory_limit_bytes) {
                tracing::error!("Cannot parse Actor event: {text}");
            }
        }
        tracing::debug!("The platform events websocket has been closed");
    })
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::time::Duration;

    use crawlee::core::EventKind;
    use std::cell::RefCell;

    use super::*;

    fn recorder(events: &EventManager) -> Rc<RefCell<Vec<Event>>> {
        let seen = Rc::new(RefCell::new(Vec::new()));
        for kind in [
            EventKind::PersistState,
            EventKind::SystemInfo,
            EventKind::Migrating,
            EventKind::Aborting,
            EventKind::Custom,
        ] {
            let seen = seen.clone();
            events.on(kind, move |event| {
                seen.borrow_mut().push(event);
                async {}
            });
        }
        seen
    }

    #[tokio::test(flavor = "local")]
    async fn platform_messages_become_crawlee_events() {
        let events = EventManager::new(Duration::from_secs(60));
        let seen = recorder(&events);

        let info = r#"{"name":"systemInfo","data":{"createdAt":"2026-01-01T00:00:00.000Z","cpuCurrentUsage":42.5,
            "isCpuOverloaded":true,"memCurrentBytes":1024}}"#;
        assert!(relay(info, &events, Some(4096)));
        events.wait_for_all_listeners_to_complete().await;
        let Event::SystemInfo(info) = seen.borrow_mut()[0].clone() else { panic!("{:?}", seen.borrow_mut()) };
        assert_eq!((info.cpu_current_usage, info.is_cpu_overloaded), (42.5, true));
        assert_eq!((info.mem_current_bytes, info.mem_total_bytes), (Some(1024), Some(4096)));
        assert_eq!(info.created_at.to_rfc3339(), "2026-01-01T00:00:00+00:00");

        seen.borrow_mut().clear();
        assert!(relay(r#"{"name":"migrating","data":null}"#, &events, None));
        assert!(relay(r#"{"name":"cpuInfo","data":{"isCpuOverloaded":false}}"#, &events, None));
        assert!(!relay("not json", &events, None));
        events.wait_for_all_listeners_to_complete().await;
        let seen = seen.borrow_mut().clone();
        assert!(seen.contains(&Event::Migrating));
        assert!(seen.contains(&Event::PersistState { is_migrating: true }));
        assert!(seen.contains(&Event::Custom {
            name: "cpuInfo".to_owned(),
            data: serde_json::json!({ "isCpuOverloaded": false })
        }));
    }

    #[tokio::test(flavor = "local")]
    async fn websocket_messages_are_relayed() {
        use futures_util::SinkExt as _;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        tokio::task::spawn_local(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            socket.send(Message::text(r#"{"name":"aborting","data":null}"#)).await.unwrap();
            socket.close(None).await.unwrap();
        });

        let events = EventManager::new(Duration::from_secs(60));
        let seen = recorder(&events);
        connect(&url, events.clone(), None).await.await.unwrap();
        events.wait_for_all_listeners_to_complete().await;
        assert_eq!(*seen.borrow_mut(), [Event::Aborting]);
    }
}
