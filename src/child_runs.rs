//! Named child runs (`runName` in JS): runs this one started under a local name, tracked in the
//! default key-value store, so that the run finds them again after a migration or resurrection.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use apify_client::clients::run::RunClient;
use apify_client::models::ActorRun;
use apify_client::{ApifyClient, ApifyClientError, ApifyClientResult, RunResurrectOptions};
use chrono::{SecondsFormat, Utc};
use crawlee::{Services, StorageIdentifier};
use indexmap::IndexMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

/// The key of the tracked runs in the default key-value store.
pub const CHILD_RUNS_KVS_KEY: &str = "__ACTOR_CHILD_RUNS";

/// How many times a tracked run the API reports as missing is looked up again before it counts as
/// gone: a run started moments ago may not be on every API replica yet.
const NOT_FOUND_RETRIES: u32 = 12;
const NOT_FOUND_RETRY_INTERVAL: Duration = Duration::from_millis(250);

/// Statuses that shortly end as `ABORTED` / `TIMED-OUT`.
const SETTLING_STATUSES: [&str; 2] = ["ABORTING", "TIMING-OUT"];
const RESURRECTABLE_STATUSES: [&str; 2] = ["ABORTED", "TIMED-OUT"];
const REUSABLE_STATUSES: [&str; 5] = ["READY", "RUNNING", "SUCCEEDED", "ABORTED", "TIMED-OUT"];
/// The status of a tracked run the platform no longer returns (removed or expired).
const LOST: &str = "LOST";

/// A child run as tracked.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChildRunInfo {
    pub run_id: String,
    /// A run status, or `LOST`.
    pub status: String,
    /// ISO 8601.
    pub started_at: String,
}

/// The run tracked under a name.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TrackedChildRun {
    #[serde(flatten)]
    pub run: ChildRunInfo,
    /// Identifies the Actor or task and the input the run was started with.
    pub checksum: String,
    /// Earlier runs replaced under the same name, oldest first.
    #[serde(default)]
    pub history: Vec<ChildRunInfo>,
}

/// What a child run is started for.
pub(crate) struct ChildRunRequest {
    /// `actor` or `task`.
    pub kind: &'static str,
    pub id: String,
    pub input: Option<Value>,
}

impl ChildRunRequest {
    /// The SHA-256 of the request's JSON with sorted keys, as in JS.
    fn checksum(&self) -> String {
        let mut request = serde_json::json!({ "type": self.kind, "id": self.id });
        if let Some(input) = &self.input {
            request["input"] = input.clone();
        }
        let json = serde_json::to_string(&sort_keys(request)).expect("JSON");
        Sha256::digest(json.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

/// Object keys in the order JavaScript compares strings (by UTF-16 code units).
fn sort_keys(value: Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.into_iter().map(sort_keys).collect()),
        Value::Object(object) => {
            let mut entries: Vec<(String, Value)> = object.into_iter().collect();
            entries.sort_by(|(a, _), (b, _)| a.encode_utf16().cmp(b.encode_utf16()));
            Value::Object(entries.into_iter().map(|(key, value)| (key, sort_keys(value))).collect())
        }
        value => value,
    }
}

fn info(run: &ActorRun) -> ChildRunInfo {
    ChildRunInfo {
        run_id: run.id.clone(),
        status: run.status.clone().unwrap_or_default(),
        started_at: run.started_at.unwrap_or_else(Utc::now).to_rfc3339_opts(SecondsFormat::Millis, true),
    }
}

fn status(run: &ActorRun) -> &str {
    run.status.as_deref().unwrap_or_default()
}

/// A tracked run, retrying a 404 for a few seconds, so that a run started moments ago is not
/// taken for gone (which would start a duplicate).
async fn get_tracked_run(client: &ApifyClient, run_id: &str) -> ApifyClientResult<Option<ActorRun>> {
    let run_client = client.run(run_id);
    let mut run = run_client.get().await?;
    for _ in 0..NOT_FOUND_RETRIES {
        if run.is_some() {
            break;
        }
        tokio::time::sleep(NOT_FOUND_RETRY_INTERVAL).await;
        run = run_client.get().await?;
    }
    Ok(run)
}

fn storage_error(err: crawlee::core::StorageError) -> ApifyClientError {
    ApifyClientError::InvalidResponse(err.to_string())
}

/// The child runs of this run (`ChildRunTracker` in JS).
pub(crate) struct ChildRunTracker {
    services: Services,
    /// Loaded from the default key-value store on first use; the lock also orders the writes.
    tracked: tokio::sync::Mutex<Option<IndexMap<String, TrackedChildRun>>>,
    /// The client each name was last started or resumed with in this process, to keep a token.
    clients: Mutex<HashMap<String, ApifyClient>>,
    /// One start or resume at a time per name.
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl ChildRunTracker {
    pub(crate) fn new(services: Services) -> Self {
        ChildRunTracker {
            services,
            tracked: tokio::sync::Mutex::default(),
            clients: Mutex::default(),
            locks: Mutex::default(),
        }
    }

    /// Loads the tracked runs from the default key-value store, once.
    pub(crate) async fn load(&self) -> ApifyClientResult<()> {
        let mut tracked = self.tracked.lock().await;
        self.loaded(&mut tracked).await?;
        Ok(())
    }

    async fn loaded<'a>(
        &self,
        tracked: &'a mut Option<IndexMap<String, TrackedChildRun>>,
    ) -> ApifyClientResult<&'a mut IndexMap<String, TrackedChildRun>> {
        if tracked.is_none() {
            let store = self.services.open_key_value_store(&StorageIdentifier::Default).await.map_err(storage_error)?;
            let stored: Option<Value> = store.get_value(CHILD_RUNS_KVS_KEY).await.map_err(storage_error)?;
            let runs =
                serde_json::from_value(stored.unwrap_or_else(|| Value::Object(Default::default()))).map_err(|err| {
                    ApifyClientError::InvalidResponse(format!(
                        "The child runs stored under the \"{CHILD_RUNS_KVS_KEY}\" key in the default key-value store \
                         are malformed: {err}"
                    ))
                })?;
            *tracked = Some(runs);
        }
        Ok(tracked.as_mut().expect("loaded"))
    }

    async fn persist(&self, tracked: &IndexMap<String, TrackedChildRun>) -> ApifyClientResult<()> {
        let store = self.services.open_key_value_store(&StorageIdentifier::Default).await.map_err(storage_error)?;
        store.set_value(CHILD_RUNS_KVS_KEY, tracked).await.map_err(storage_error)
    }

    async fn get(&self, run_name: &str) -> ApifyClientResult<Option<TrackedChildRun>> {
        let mut tracked = self.tracked.lock().await;
        Ok(self.loaded(&mut tracked).await?.get(run_name).cloned())
    }

    /// Returns the run tracked under `run_name` when it is in progress or succeeded, resurrects it
    /// with `resurrect` when it was aborted or timed out (once it finished aborting or timing out),
    /// and otherwise (failed or gone) starts a new one with `start`, keeping the replaced one in the
    /// history. Also returns whether the run was resumed rather than started.
    pub(crate) async fn start_or_resume<F, Fut>(
        &self,
        client: &ApifyClient,
        run_name: &str,
        request: ChildRunRequest,
        resurrect: RunResurrectOptions,
        start: F,
    ) -> ApifyClientResult<(ActorRun, bool)>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ApifyClientResult<ActorRun>>,
    {
        let lock = self.locks.lock().entry(run_name.to_owned()).or_default().clone();
        let _guard = lock.lock().await;

        let tracked = self.get(run_name).await?;
        let mut tracked_run = match &tracked {
            Some(tracked) => get_tracked_run(client, &tracked.run.run_id).await?,
            None => None,
        };
        if let Some(run) = &tracked_run
            && SETTLING_STATUSES.contains(&status(run))
        {
            tracked_run = Some(client.run(run.id.clone()).wait_for_finish(None).await?);
        }

        if let (Some(tracked), Some(run)) = (&tracked, &tracked_run)
            && REUSABLE_STATUSES.contains(&status(run))
        {
            // The tracked run would otherwise be returned or resurrected for another request.
            if tracked.checksum != request.checksum() {
                return Err(ApifyClientError::InvalidArgument(format!(
                    "The run name \"{run_name}\" was already used for a different Actor, task or input. Use a \
                     unique `run_name` for each child run."
                )));
            }
            let run = if RESURRECTABLE_STATUSES.contains(&status(run)) {
                client.run(run.id.clone()).resurrect(resurrect).await?
            } else {
                run.clone()
            };
            self.update(run_name, &run).await?;
            self.clients.lock().insert(run_name.to_owned(), client.clone());
            return Ok((run, true));
        }

        let replaced_status = tracked_run.as_ref().map_or(LOST, status).to_owned();
        let run = start().await?;
        self.track(run_name, &run, &request, replaced_status).await?;
        self.clients.lock().insert(run_name.to_owned(), client.clone());
        Ok((run, false))
    }

    /// Tracks `run` under `run_name`, moving a run tracked earlier under it to the history with
    /// `replaced_status` as its last known status.
    async fn track(
        &self,
        run_name: &str,
        run: &ActorRun,
        request: &ChildRunRequest,
        replaced_status: String,
    ) -> ApifyClientResult<()> {
        let mut tracked = self.tracked.lock().await;
        let runs = self.loaded(&mut tracked).await?;
        let history = match runs.get(run_name) {
            Some(previous) => {
                let mut history = previous.history.clone();
                history.push(ChildRunInfo { status: replaced_status, ..previous.run.clone() });
                history
            }
            None => Vec::new(),
        };
        runs.insert(run_name.to_owned(), TrackedChildRun { run: info(run), checksum: request.checksum(), history });
        self.persist(runs).await
    }

    /// Records the latest state of the run tracked under `run_name`.
    pub(crate) async fn update(&self, run_name: &str, run: &ActorRun) -> ApifyClientResult<()> {
        let mut tracked = self.tracked.lock().await;
        let runs = self.loaded(&mut tracked).await?;
        let Some(current) = runs.get_mut(run_name) else { return Ok(()) };
        let status = status(run);
        if current.run.run_id != run.id || current.run.status == status {
            return Ok(());
        }
        current.run.status = status.to_owned();
        self.persist(runs).await
    }

    /// A client of the current run under each name loaded so far: the client the name was last
    /// started or resumed with in this process, or `default_client`.
    pub(crate) async fn run_clients(&self, default_client: &ApifyClient) -> IndexMap<String, RunClient> {
        let tracked = self.tracked.lock().await;
        let clients = self.clients.lock();
        tracked
            .iter()
            .flatten()
            .map(|(name, tracked)| {
                let client = clients.get(name).unwrap_or(default_client);
                (name.clone(), client.run(tracked.run.run_id.clone()))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_checksum_ignores_key_order_and_an_absent_input() {
        let request = |input: Option<Value>| ChildRunRequest { kind: "actor", id: "user/a".to_owned(), input };
        let ordered = request(Some(serde_json::json!({ "b": 1, "a": { "y": [1, { "d": 2, "c": 3 }], "x": null } })));
        let reordered = request(Some(serde_json::json!({ "a": { "x": null, "y": [1, { "c": 3, "d": 2 }] }, "b": 1 })));
        assert_eq!(ordered.checksum(), reordered.checksum());
        assert_ne!(ordered.checksum(), request(None).checksum());
        // The SHA-256 of `{"id":"user/a","type":"actor"}`, as JS hashes it.
        assert_eq!(request(None).checksum(), "d3ed8cb32ef8fd5fa3f20b7df70aa3622e6396b8c1934c863bec89610b0284ec");
    }
}
