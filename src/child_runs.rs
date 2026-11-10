//! Named child runs (`runName` in JS): runs this one started under a local name, tracked in the
//! default key-value store, so that the run finds them again after a migration or resurrection.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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
const ABORTED_STATUSES: [&str; 2] = ["ABORTING", "ABORTED"];

/// How long `abort_runs_with_parent` waits for the child runs to be aborted. The platform stops a
/// gracefully aborted run 30 seconds after the `aborting` event, which leaves `Actor::exit` time
/// for the rest of its work.
pub const ABORT_WITH_PARENT_TIMEOUT: Duration = Duration::from_secs(10);
/// The status of a tracked run the platform no longer returns (removed or expired).
const LOST: &str = "LOST";

/// The API's refusals of a start for lack of memory or Actor jobs on the account, which
/// [`CapacityQueue`] retries.
const CAPACITY_ERROR_TYPES: [&str; 2] = ["actor-memory-limit-exceeded", "concurrent-runs-limit-exceeded"];
/// How long no start is tried after one was refused for lack of capacity (`MAIN_LOOP_COOLDOWN_MS`
/// of `apify-orchestrator`).
const CAPACITY_COOLDOWN: Duration = Duration::from_secs(10);
/// How often a waiting start checks whether it can go (`MAIN_LOOP_INTERVAL_MS`).
const CAPACITY_CHECK_INTERVAL: Duration = Duration::from_secs(1);

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
    /// Whether the run is aborted when this run is gracefully aborted.
    #[serde(default, rename = "abortWithParent")]
    pub abort_with_parent: bool,
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
    /// The tracked runs loaded so far.
    runs: Mutex<IndexMap<String, TrackedChildRun>>,
    /// Whether the runs were loaded from the default key-value store; held while they are loaded
    /// or written, so that they are loaded once and written in order.
    io: tokio::sync::Mutex<bool>,
    /// The client each name was last started or resumed with in this process, to keep a token.
    clients: Mutex<HashMap<String, ApifyClient>>,
    /// One start or resume at a time per name.
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Set once this run is being gracefully aborted, after which no run with `abort_with_parent`
    /// is started.
    is_parent_aborting: AtomicBool,
    /// The starts that wait for capacity.
    pub(crate) capacity: CapacityQueue,
}

impl ChildRunTracker {
    pub(crate) fn new(services: Services) -> Self {
        ChildRunTracker {
            services,
            runs: Mutex::default(),
            io: tokio::sync::Mutex::default(),
            clients: Mutex::default(),
            locks: Mutex::default(),
            is_parent_aborting: AtomicBool::new(false),
            capacity: CapacityQueue::default(),
        }
    }

    /// Loads the tracked runs from the default key-value store, once.
    pub(crate) async fn load(&self) -> ApifyClientResult<()> {
        let mut loaded = self.io.lock().await;
        self.ensure_loaded(&mut loaded).await
    }

    async fn ensure_loaded(&self, loaded: &mut bool) -> ApifyClientResult<()> {
        if *loaded {
            return Ok(());
        }
        let store = self.services.open_key_value_store(&StorageIdentifier::Default).await.map_err(storage_error)?;
        let stored: Option<Value> = store.get_value(CHILD_RUNS_KVS_KEY).await.map_err(storage_error)?;
        let runs =
            serde_json::from_value(stored.unwrap_or_else(|| Value::Object(Default::default()))).map_err(|err| {
                ApifyClientError::InvalidResponse(format!(
                    "The child runs stored under the \"{CHILD_RUNS_KVS_KEY}\" key in the default key-value store are \
                 malformed: {err}"
                ))
            })?;
        *self.runs.lock() = runs;
        *loaded = true;
        Ok(())
    }

    async fn persist(&self, runs: &IndexMap<String, TrackedChildRun>) -> ApifyClientResult<()> {
        let store = self.services.open_key_value_store(&StorageIdentifier::Default).await.map_err(storage_error)?;
        store.set_value(CHILD_RUNS_KVS_KEY, runs).await.map_err(storage_error)
    }

    async fn get(&self, run_name: &str) -> ApifyClientResult<Option<TrackedChildRun>> {
        self.load().await?;
        Ok(self.runs.lock().get(run_name).cloned())
    }

    /// Returns the run tracked under `run_name` when it is in progress or succeeded, resurrects it
    /// with `resurrect` when it was aborted or timed out (once it finished aborting or timing out),
    /// and otherwise (failed or gone) starts a new one with `start`, keeping the replaced one in the
    /// history. `abort_with_parent` replaces the value tracked under `run_name`. Also returns
    /// whether the run was resumed rather than started.
    pub(crate) async fn start_or_resume<F, Fut>(
        &self,
        client: &ApifyClient,
        run_name: &str,
        request: ChildRunRequest,
        resurrect: RunResurrectOptions,
        abort_with_parent: bool,
        start: F,
    ) -> ApifyClientResult<(ActorRun, bool)>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ApifyClientResult<ActorRun>>,
    {
        let lock = self.lock_of(run_name);
        let _guard = lock.lock().await;

        if abort_with_parent && self.is_parent_aborting.load(Ordering::Acquire) {
            return Err(ApifyClientError::InvalidArgument(format!(
                "Child run \"{run_name}\" with `abort_with_parent` was not started, because this run is being \
                 aborted."
            )));
        }

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
            if tracked.abort_with_parent != abort_with_parent {
                self.set_abort_with_parent(run_name, abort_with_parent).await?;
            }
            self.update(run_name, &run).await?;
            self.clients.lock().insert(run_name.to_owned(), client.clone());
            return Ok((run, true));
        }

        let replaced_status = tracked_run.as_ref().map_or(LOST, status).to_owned();
        let run = start().await?;
        self.track(run_name, &run, &request, replaced_status, abort_with_parent).await?;
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
        abort_with_parent: bool,
    ) -> ApifyClientResult<()> {
        let mut loaded = self.io.lock().await;
        self.ensure_loaded(&mut loaded).await?;
        let runs = {
            let mut runs = self.runs.lock();
            let history = match runs.get(run_name) {
                Some(previous) => {
                    let mut history = previous.history.clone();
                    history.push(ChildRunInfo { status: replaced_status, ..previous.run.clone() });
                    history
                }
                None => Vec::new(),
            };
            let tracked = TrackedChildRun { run: info(run), checksum: request.checksum(), history, abort_with_parent };
            runs.insert(run_name.to_owned(), tracked);
            runs.clone()
        };
        self.persist(&runs).await
    }

    fn lock_of(&self, run_name: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.locks.lock().entry(run_name.to_owned()).or_default().clone()
    }

    async fn set_abort_with_parent(&self, run_name: &str, abort_with_parent: bool) -> ApifyClientResult<()> {
        let mut loaded = self.io.lock().await;
        self.ensure_loaded(&mut loaded).await?;
        let runs = {
            let mut runs = self.runs.lock();
            let Some(current) = runs.get_mut(run_name) else { return Ok(()) };
            current.abort_with_parent = abort_with_parent;
            runs.clone()
        };
        self.persist(&runs).await
    }

    /// Gracefully aborts the current run under each name tracked with `abort_with_parent`, and
    /// makes any later start with it fail. A start in flight is waited for, so the run it starts is
    /// aborted too. Returns after [`ABORT_WITH_PARENT_TIMEOUT`] at the latest, leaving the aborts
    /// still in progress to go on.
    pub(crate) async fn abort_runs_with_parent(&self, default_client: &ApifyClient) {
        self.is_parent_aborting.store(true, Ordering::Release);
        if let Err(err) = self.load().await {
            tracing::error!("Failed to load the child runs to abort with the parent: {err}");
            return;
        }
        let mut names: Vec<String> = self.runs.lock().keys().cloned().collect();
        for name in self.locks.lock().keys() {
            if !names.contains(name) {
                names.push(name.clone());
            }
        }
        let aborts = futures_util::future::join_all(names.iter().map(|name| async move {
            let lock = self.lock_of(name);
            let _guard = lock.lock().await;
            let Some(tracked) = self.runs.lock().get(name).filter(|tracked| tracked.abort_with_parent).cloned() else {
                return;
            };
            let client = self.clients.lock().get(name).cloned().unwrap_or_else(|| default_client.clone());
            match client.run(tracked.run.run_id.clone()).abort(Some(true)).await {
                Ok(run) if ABORTED_STATUSES.contains(&status(&run)) => {
                    tracing::info!(run_id = %run.id, "Aborted child run \"{name}\" with the parent");
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::error!(run_id = %tracked.run.run_id, "Failed to abort child run \"{name}\": {err}")
                }
            }
        }));
        if tokio::time::timeout(ABORT_WITH_PARENT_TIMEOUT, aborts).await.is_err() {
            tracing::warn!(
                "Aborting the child runs with the parent timed out after {}s",
                ABORT_WITH_PARENT_TIMEOUT.as_secs()
            );
        }
    }

    /// Records the latest state of the run tracked under `run_name`.
    pub(crate) async fn update(&self, run_name: &str, run: &ActorRun) -> ApifyClientResult<()> {
        let mut loaded = self.io.lock().await;
        self.ensure_loaded(&mut loaded).await?;
        let runs = {
            let mut runs = self.runs.lock();
            let Some(current) = runs.get_mut(run_name) else { return Ok(()) };
            let status = status(run);
            if current.run.run_id != run.id || current.run.status == status {
                return Ok(());
            }
            current.run.status = status.to_owned();
            runs.clone()
        };
        self.persist(&runs).await
    }

    /// A client of the current run under each name loaded so far: the client the name was last
    /// started or resumed with in this process, or `default_client`.
    pub(crate) fn run_clients(&self, default_client: &ApifyClient) -> IndexMap<String, RunClient> {
        let clients = self.clients.lock();
        self.runs
            .lock()
            .iter()
            .map(|(name, tracked)| {
                let client = clients.get(name).unwrap_or(default_client);
                (name.clone(), client.run(tracked.run.run_id.clone()))
            })
            .collect()
    }
}

/// The starts of [`CallOptions::wait_for_capacity`](crate::CallOptions::wait_for_capacity), as
/// `apify-orchestrator` schedules them: one at a time. A start refused for lack of memory or Actor
/// jobs on the account is tried again every second once [`CAPACITY_COOLDOWN`] has passed, in which
/// no other start goes either. Once this run is shutting down (aborting, migrating or exiting), no
/// start goes out.
#[derive(Default)]
pub(crate) struct CapacityQueue {
    lock: tokio::sync::Mutex<()>,
    cooldown_until: Mutex<Option<tokio::time::Instant>>,
    closed: AtomicBool,
}

impl CapacityQueue {
    /// Starts no more runs: the waiting starts fail.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    /// Runs `start` until the account has capacity for the run it starts.
    pub(crate) async fn start<F, Fut>(&self, mut start: F) -> ApifyClientResult<ActorRun>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = ApifyClientResult<ActorRun>>,
    {
        loop {
            {
                let _exclusive = self.lock.lock().await;
                if self.closed.load(Ordering::Acquire) {
                    return Err(ApifyClientError::InvalidArgument(
                        "The run was not started, because this run is shutting down.".to_owned(),
                    ));
                }
                let cooling_down = self.cooldown_until.lock().is_some_and(|until| tokio::time::Instant::now() < until);
                if !cooling_down {
                    match start().await {
                        Err(err) if is_capacity_error(&err) => {
                            *self.cooldown_until.lock() = Some(tokio::time::Instant::now() + CAPACITY_COOLDOWN);
                            tracing::warn!(
                                "Not enough capacity to start a run, retrying in {}s: {err}",
                                CAPACITY_COOLDOWN.as_secs()
                            );
                        }
                        result => return result,
                    }
                }
            }
            tokio::time::sleep(CAPACITY_CHECK_INTERVAL).await;
        }
    }
}

fn is_capacity_error(err: &ApifyClientError) -> bool {
    err.as_api_error()
        .and_then(|err| err.error_type.as_deref())
        .is_some_and(|error_type| CAPACITY_ERROR_TYPES.contains(&error_type))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    fn refusal(error_type: &str) -> ApifyClientError {
        ApifyClientError::Api(Box::new(apify_client::ApiError {
            status_code: 402,
            error_type: Some(error_type.to_owned()),
            message: error_type.to_owned(),
            attempt: 1,
            http_method: Some("POST".to_owned()),
            path: Some("/v2/acts/user~a/runs".to_owned()),
            data: None,
        }))
    }

    fn run(id: &str) -> ActorRun {
        serde_json::from_value(serde_json::json!({ "id": id, "actId": "a", "status": "READY" })).unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn a_start_refused_for_capacity_is_retried_after_the_cooldown() {
        let queue = CapacityQueue::default();
        let calls = AtomicUsize::new(0);
        let started = tokio::time::Instant::now();
        let result = queue
            .start(|| async {
                match calls.fetch_add(1, Ordering::SeqCst) {
                    0 => Err(refusal("actor-memory-limit-exceeded")),
                    1 => Err(refusal("concurrent-runs-limit-exceeded")),
                    _ => Ok(run("child-1")),
                }
            })
            .await;
        assert_eq!(result.unwrap().id, "child-1");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert!(started.elapsed() >= 2 * CAPACITY_COOLDOWN, "{:?}", started.elapsed());
    }

    #[tokio::test(start_paused = true)]
    async fn starts_go_one_at_a_time_and_other_errors_are_not_retried() {
        let queue = CapacityQueue::default();
        let (in_flight, peak) = (AtomicUsize::new(0), AtomicUsize::new(0));
        let start = || async {
            peak.fetch_max(in_flight.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(100)).await;
            in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok(run("child"))
        };
        let (a, b) = tokio::join!(queue.start(start), queue.start(start));
        assert!(a.is_ok() && b.is_ok());
        assert_eq!(peak.load(Ordering::SeqCst), 1);

        let calls = AtomicUsize::new(0);
        let failed = queue
            .start(|| async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(refusal("actor-not-found"))
            })
            .await;
        assert!(failed.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn no_start_goes_out_once_the_run_shuts_down() {
        let queue = CapacityQueue::default();
        let calls = AtomicUsize::new(0);
        let waiting = queue.start(|| async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(refusal("actor-memory-limit-exceeded"))
        });
        let close = async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            queue.close();
        };
        let (result, ()) = tokio::join!(waiting, close);
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

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
