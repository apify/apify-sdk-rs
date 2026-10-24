//! The log of a run started by this one, redirected into this run's log while it runs, as
//! `StreamedLog` of the JS client does for `Actor.call`.
//!
//! The log is read on the SDK's reqwest, as the API client's own log stream has no TLS in this
//! build (its transport is replaced by [`crate::client::ReqwestBackend`]).

use std::sync::LazyLock;
use std::time::Duration;

use apify_client::{ApifyClient, ApifyClientResult};
use chrono::{DateTime, Utc};
use regex::Regex;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// The first pause before reopening a log stream that ended empty (the run has not logged yet).
const EMPTY_LOG_STREAM_RETRY: Duration = Duration::from_millis(500);
const EMPTY_LOG_STREAM_MAX_RETRY: Duration = Duration::from_secs(5);
/// How long [`StreamedLog::stop`] waits for lines in flight before cutting the stream.
const STOP_GRACE: Duration = Duration::from_secs(1);

/// The timestamp each message of a log starts with.
static MESSAGE_MARKER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?:\n|^)(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)").expect("a regex"));

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Running,
    /// Read what is in flight, then end.
    Stopping,
    /// The grace period ended: cut the stream.
    Aborted,
}

/// Turns log text into messages, each logged with a prefix naming the run.
struct Redirect {
    prefix: String,
    /// Older messages are skipped (a resumed run's log was redirected before).
    since: Option<DateTime<Utc>>,
}

impl Redirect {
    /// The messages of complete log lines: each starts with a timestamp, and runs until the next
    /// one. Text before the first timestamp is dropped, as in JS.
    fn messages(&self, text: &str) -> Vec<String> {
        let markers: Vec<_> = MESSAGE_MARKER.captures_iter(text).collect();
        let mut messages = Vec::new();
        for (index, captures) in markers.iter().enumerate() {
            let marker = captures.get(1).expect("a marker");
            let end = markers.get(index + 1).map_or(text.len(), |next| next.get(0).expect("a match").start());
            if let Some(since) = self.since
                && DateTime::parse_from_rfc3339(marker.as_str()).is_ok_and(|time| time < since)
            {
                continue;
            }
            messages.push(text[marker.start()..end].trim().to_owned());
        }
        messages
    }

    fn log(&self, text: &str) {
        for message in self.messages(text) {
            // The original level is unknown: all messages are logged at info, as in JS.
            tracing::info!("{}{message}", self.prefix);
        }
    }

    /// Logs the complete lines of `remainder` and `chunk`, and returns the incomplete rest.
    fn chunk(&self, remainder: Option<Vec<u8>>, chunk: &[u8]) -> Vec<u8> {
        let mut buffer = remainder.unwrap_or_default();
        buffer.extend_from_slice(chunk);
        match buffer.iter().rposition(|&byte| byte == b'\n') {
            Some(end) => {
                let rest = buffer.split_off(end);
                self.log(&String::from_utf8_lossy(&buffer));
                rest
            }
            None => buffer,
        }
    }

    /// Logs what is left when the log ends: maybe the last line, without an end of line.
    fn flush(&self, remainder: &[u8]) {
        let message = String::from_utf8_lossy(remainder);
        let message = message.trim();
        if !message.is_empty() {
            tracing::info!("{}{message}", self.prefix);
        }
    }
}

/// Where the log is read.
struct Source {
    http: reqwest::Client,
    url: String,
    token: Option<String>,
    user_agent: String,
}

impl Source {
    async fn get(&self, stream: bool) -> Result<reqwest::Response, String> {
        let query = if stream { "?stream=1&raw=1" } else { "?raw=1" };
        let mut request = self.http.get(format!("{}{query}", self.url)).header("User-Agent", &self.user_agent);
        if let Some(token) = &self.token {
            request = request.header("Authorization", format!("Bearer {token}"));
        }
        let response = request.send().await.map_err(|err| err.to_string())?;
        response.error_for_status().map_err(|err| err.to_string())
    }
}

/// Returns once `phase` reaches `target`, or the [`StreamedLog`] is gone.
async fn reached(phase: &mut watch::Receiver<Phase>, target: Phase) {
    let _ = phase.wait_for(|phase| *phase == target).await;
}

fn is(phase: &watch::Receiver<Phase>, target: Phase) -> bool {
    *phase.borrow() == target || (target == Phase::Aborted && phase.has_changed().is_err())
}

fn stopping(phase: &watch::Receiver<Phase>) -> bool {
    !is(phase, Phase::Running) || phase.has_changed().is_err()
}

/// Reads an open log stream until it ends, is stopped or cut. Returns the incomplete rest, or
/// `None` when no chunk arrived.
async fn read(
    redirect: &Redirect,
    mut response: reqwest::Response,
    phase: &mut watch::Receiver<Phase>,
) -> Result<Option<Vec<u8>>, String> {
    let mut remainder = None;
    loop {
        let chunk = tokio::select! {
            chunk = response.chunk() => chunk,
            // A cut stream still hands back its unterminated last line, to flush.
            () = reached(phase, Phase::Aborted) => break,
        };
        match chunk {
            Ok(Some(chunk)) => remainder = Some(redirect.chunk(remainder, &chunk)),
            Ok(None) => break,
            Err(_) if is(phase, Phase::Aborted) => break,
            Err(err) => return Err(err.to_string()),
        }
        if stopping(phase) {
            break;
        }
    }
    Ok(remainder)
}

async fn redirect_log(source: Source, redirect: Redirect, mut phase: watch::Receiver<Phase>) -> Result<(), String> {
    let mut remainder = None;
    let mut retry = EMPTY_LOG_STREAM_RETRY;
    // The API serves the log of a run that has not logged yet as a stream that ends at once, so it
    // is reopened until the first bytes arrive. Stopped before that, the log is read in one request.
    while remainder.is_none() {
        if stopping(&phase) {
            let log = source.get(false).await?.bytes().await.map_err(|err| err.to_string())?;
            remainder = Some(redirect.chunk(None, &log));
            break;
        }
        let response = tokio::select! {
            response = source.get(true) => match response {
                Ok(response) => response,
                // Stopped while connecting: read the log in one request.
                Err(_) if stopping(&phase) => continue,
                Err(err) => return Err(err),
            },
            () = reached(&mut phase, Phase::Aborted) => continue,
        };
        // A stream opened during the stop would be cut after its first chunk.
        if stopping(&phase) {
            continue;
        }
        remainder = read(&redirect, response, &mut phase).await?;
        if remainder.is_none() {
            tokio::select! {
                () = tokio::time::sleep(retry) => {}
                () = reached(&mut phase, Phase::Stopping) => {}
            }
            retry = (retry * 2).min(EMPTY_LOG_STREAM_MAX_RETRY);
        }
    }
    redirect.flush(&remainder.unwrap_or_default());
    Ok(())
}

/// The redirection of a run's log, from [`StreamedLog::start`] to [`StreamedLog::stop`].
pub(crate) struct StreamedLog {
    phase: watch::Sender<Phase>,
    task: JoinHandle<()>,
}

impl StreamedLog {
    /// Starts redirecting the log of `run_id`, prefixed with the name of its Actor and the run id.
    /// Unless `from_start`, the messages logged before now are skipped.
    pub(crate) async fn start(
        client: &ApifyClient,
        token: Option<&str>,
        run_id: &str,
        from_start: bool,
    ) -> ApifyClientResult<StreamedLog> {
        let run = client.run(run_id).get().await?;
        let actor_name = match run.as_ref().and_then(|run| run.act_id.clone()) {
            Some(actor_id) => client.actor(actor_id).get().await?.and_then(|actor| actor.name),
            None => None,
        };
        let run_id_part = format!("runId:{}", run.as_ref().map_or("", |run| run.id.as_str()));
        let name = [actor_name.unwrap_or_default(), run_id_part]
            .into_iter()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        let redirect = Redirect { prefix: format!("{name} -> "), since: (!from_start).then(Utc::now) };
        let source = Source {
            http: reqwest::Client::new(),
            url: format!("{}/actor-runs/{run_id}/log", client.api_base_url()),
            token: token.map(str::to_owned),
            user_agent: client.user_agent().to_owned(),
        };
        let (phase, receiver) = watch::channel(Phase::Running);
        let task = tokio::spawn(async move {
            let aborted = receiver.clone();
            if let Err(err) = redirect_log(source, redirect, receiver).await
                && !is(&aborted, Phase::Aborted)
            {
                tracing::warn!("Log redirection stopped due to error: {err}");
            }
        });
        Ok(StreamedLog { phase, task })
    }

    /// Stops the redirection: waits up to a second for the lines in flight, then cuts the stream.
    /// When nothing was read yet, the whole log is read in one request.
    pub(crate) async fn stop(mut self) {
        let _ = self.phase.send(Phase::Stopping);
        if tokio::time::timeout(STOP_GRACE, &mut self.task).await.is_err() {
            let _ = self.phase.send(Phase::Aborted);
            let _ = (&mut self.task).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_start_at_timestamps_and_skip_older_ones() {
        let log = "partial line\n2026-01-01T00:00:00.000Z first\n  continued\n2026-01-01T00:00:02.000Z second\n";
        let redirect = Redirect { prefix: String::new(), since: None };
        assert_eq!(
            redirect.messages(log),
            ["2026-01-01T00:00:00.000Z first\n  continued", "2026-01-01T00:00:02.000Z second"]
        );
        let since = DateTime::parse_from_rfc3339("2026-01-01T00:00:01Z").unwrap().with_timezone(&Utc);
        let redirect = Redirect { prefix: String::new(), since: Some(since) };
        assert_eq!(redirect.messages(log), ["2026-01-01T00:00:02.000Z second"]);
    }

    #[test]
    fn chunks_keep_the_incomplete_last_line() {
        let redirect = Redirect { prefix: String::new(), since: None };
        let rest = redirect.chunk(None, b"2026-01-01T00:00:00.000Z one\n2026-01-01T00:00:01.000Z tw");
        assert_eq!(rest, b"\n2026-01-01T00:00:01.000Z tw");
        let rest = redirect.chunk(Some(rest), b"o");
        assert_eq!(rest, b"\n2026-01-01T00:00:01.000Z two");
    }
}
