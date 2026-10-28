//! Request queues on the Apify platform.
//!
//! Two modes, ported from the JS SDK: [`ApifySingleRequestQueue`] (one consumer, no locks) and
//! [`ApifySharedRequestQueue`](super::request_queue_shared::ApifySharedRequestQueue) (any number
//! of consumers, requests locked on the platform).

use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;

use apify_client::clients::request_queue::{BatchAddRequestsOptions, ListRequestsOptions, RequestQueueClient};
use apify_client::models::RequestQueueRequest;
use async_trait::async_trait;
use crawlee::Request;
use crawlee::core::StorageResult;
use crawlee::core::errors::StorageError;
use crawlee::core::request::unique_key_to_request_id;
use crawlee::core::storage::backend::{
    BatchAddRequestsResult, ProcessedRequest, QueueOperationInfo, RequestQueueBackend, RequestQueueInfo,
    UnprocessedRequest,
};
use lru::LruCache;
use parking_lot::Mutex;

use super::{DropHook, backend_error, date, extra_date, extra_u64, unsupported};

/// How a request queue on the platform is consumed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RequestQueueAccess {
    /// This client is the only consumer (other clients may add requests). No request locking, so
    /// fewer API calls. The default.
    #[default]
    Single,
    /// Any number of consumers, with requests locked on the platform while they are processed. Costs
    /// about one more API call per request.
    Shared,
}

/// Maximum number of requests cached locally.
const MAX_CACHED_REQUESTS: usize = 1_000_000;
/// The API returns at most this many head items.
const MAX_HEAD_ITEMS: usize = 1000;
/// How many new head items to aim for per head listing.
const DESIRED_NEW_HEAD_ITEMS: usize = 200;
/// How many existing requests to prefetch into the local caches on the first add.
const INIT_CACHES_REQUEST_LIMIT: i64 = 10_000;
/// The longest page of the request listing the API returns.
const MAX_LISTED_REQUESTS: i64 = 10_000;

fn to_api_request(request: &Request) -> StorageResult<RequestQueueRequest> {
    Ok(serde_json::from_value(serde_json::to_value(request)?)?)
}

fn from_api_request(request: RequestQueueRequest) -> StorageResult<Request> {
    Ok(serde_json::from_value(serde_json::to_value(request)?)?)
}

pub(super) async fn get_request_by_id(client: &RequestQueueClient, id: &str) -> StorageResult<Option<Request>> {
    match client.get_request(id).await.map_err(backend_error)? {
        Some(request) => Ok(Some(from_api_request(request)?)),
        None => Ok(None),
    }
}

pub(super) async fn update_request(
    client: &RequestQueueClient,
    request: &Request,
    forefront: bool,
) -> StorageResult<QueueOperationInfo> {
    let info = client.update_request(&to_api_request(request)?, forefront).await.map_err(backend_error)?;
    Ok(QueueOperationInfo {
        request_id: info.request_id,
        was_already_present: info.was_already_present,
        was_already_handled: info.was_already_handled,
    })
}

/// Adds requests on the platform, which assigns their ids.
pub(super) async fn send_batch(
    client: &RequestQueueClient,
    requests: &[Request],
    forefront: bool,
) -> StorageResult<BatchAddRequestsResult> {
    let api_requests = requests
        .iter()
        .map(|request| {
            let mut api_request = to_api_request(request)?;
            api_request.id = None;
            Ok(api_request)
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let options = BatchAddRequestsOptions { forefront, ..BatchAddRequestsOptions::default() };
    let added = client.batch_add_requests(&api_requests, options).await.map_err(backend_error)?;
    Ok(BatchAddRequestsResult {
        processed_requests: added
            .processed_requests
            .into_iter()
            .filter_map(|processed| {
                let unique_key = processed.unique_key?;
                Some(ProcessedRequest {
                    request_id: processed.request_id.unwrap_or_else(|| unique_key_to_request_id(&unique_key)),
                    unique_key,
                    was_already_present: processed.was_already_present.unwrap_or(false),
                    was_already_handled: processed.was_already_handled.unwrap_or(false),
                })
            })
            .collect(),
        unprocessed_requests: added
            .unprocessed_requests
            .into_iter()
            .map(|unprocessed| UnprocessedRequest {
                unique_key: unprocessed.unique_key,
                url: unprocessed.url,
                method: unprocessed.method,
            })
            .collect(),
    })
}

/// The queue's metadata, with counters no lower than the local estimates (the platform's lag).
pub(super) async fn queue_metadata(
    client: &RequestQueueClient,
    estimated_total: u64,
    estimated_handled: u64,
) -> StorageResult<RequestQueueInfo> {
    let queue = client
        .get()
        .await
        .map_err(backend_error)?
        .ok_or_else(|| StorageError::NotFound("Request queue not found or has been deleted.".to_owned()))?;
    Ok(RequestQueueInfo {
        created_at: date(queue.created_at),
        modified_at: date(queue.modified_at),
        accessed_at: extra_date(&queue.extra, "accessedAt"),
        total_request_count: (queue.total_request_count.unwrap_or(0).max(0) as u64).max(estimated_total),
        handled_request_count: extra_u64(&queue.extra, "handledRequestCount").max(estimated_handled),
        pending_request_count: extra_u64(&queue.extra, "pendingRequestCount"),
        id: queue.id,
        name: queue.name,
    })
}

pub(super) const PURGE_UNSUPPORTED: &str = "Purging a request queue is not supported on the Apify platform. \
     Use `drop()` to delete the queue entirely, or open a new queue instead.";

/// Counts the requests a batch add actually added.
pub(super) fn newly_added(result: &BatchAddRequestsResult) -> u64 {
    result.processed_requests.iter().filter(|p| !p.was_already_present && !p.was_already_handled).count() as u64
}

/// What this client knows about the queue.
struct State {
    /// Local estimate of the queue head: request ids in the order they should be fetched.
    head: VecDeque<String>,
    /// Unhandled requests added by (or fetched through) this client, by id.
    cached: LruCache<String, Request>,
    /// Ids of requests known to be handled.
    handled: HashSet<String>,
    /// Ids of requests being processed by this client.
    in_progress: HashSet<String>,
    /// Whether every unhandled request of the queue is in `head` or `in_progress`, so nothing
    /// needs listing: from the first add to a queue without unhandled requests, from a listing of
    /// the pending requests, or from a head listing that returned fewer items than asked for.
    /// Only requests other clients add later are missing; the head listing in `is_finished`
    /// finds them.
    head_complete: bool,
    /// Local estimates of the counters, which lag behind on the platform.
    estimated_total: u64,
    estimated_handled: u64,
}

impl State {
    fn cache(&mut self, request: Request) {
        if let Some(id) = request.id.clone() {
            self.cached.put(id, request);
        }
    }

    fn push_head(&mut self, id: String, forefront: bool) {
        if forefront {
            self.head.push_front(id);
        } else {
            self.head.push_back(id);
        }
    }
}

/// A request queue with a single consumer: this client keeps a local estimate of the queue head
/// and a cache of the requests it added, so a fetched request usually needs no API call and no
/// request is locked.
///
/// Constraints, as in the JS SDK: only one client may fetch and process requests; others may add
/// requests; nobody else may delete or modify requests.
///
/// Unlike the JS SDK, this client lists the queue's requests (a paid read per listed request)
/// only until it knows every unhandled request: all pending requests at once, page by page, not
/// the head (at most 1,000 requests, those in progress included). It lists the head once more to
/// confirm the end. Requests other clients add after that are picked up only by that last
/// listing, when this client has nothing else left (see docs/allowed-differences.md).
pub(crate) struct ApifySingleRequestQueue {
    client: RequestQueueClient,
    state: Mutex<State>,
    caches_initialized: tokio::sync::OnceCell<()>,
    /// Held while the pending requests are listed, so concurrent fetches list them once.
    listing: tokio::sync::Mutex<()>,
    drop_hook: DropHook,
}

impl ApifySingleRequestQueue {
    pub(crate) fn new(client: RequestQueueClient, drop_hook: DropHook) -> Self {
        ApifySingleRequestQueue {
            client,
            drop_hook,
            state: Mutex::new(State {
                head: VecDeque::new(),
                cached: LruCache::new(NonZeroUsize::new(MAX_CACHED_REQUESTS).expect("non-zero")),
                handled: HashSet::new(),
                in_progress: HashSet::new(),
                head_complete: false,
                estimated_total: 0,
                estimated_handled: 0,
            }),
            caches_initialized: tokio::sync::OnceCell::new(),
            listing: tokio::sync::Mutex::new(()),
        }
    }

    async fn get_request_by_id(&self, id: &str) -> StorageResult<Option<Request>> {
        get_request_by_id(&self.client, id).await
    }

    async fn update_request(&self, request: &Request, forefront: bool) -> StorageResult<QueueOperationInfo> {
        update_request(&self.client, request, forefront).await
    }

    /// One-time prefetch of the queue contents, so that the requests a resurrected run adds again
    /// are deduplicated locally (one read for the whole cache) instead of on the platform (one
    /// paid write per request). It runs before this client first adds requests: when the queue
    /// has no unhandled request then, this client knows every request it will have to process.
    async fn init_caches(&self) {
        let options = ListRequestsOptions { limit: Some(INIT_CACHES_REQUEST_LIMIT), ..ListRequestsOptions::default() };
        let page = match self.client.list_requests(options).await {
            Ok(page) => page,
            Err(err) => {
                // An optimization only: deduplication falls back to the platform.
                tracing::warn!("Failed to prefetch the request queue contents into the local cache: {err}");
                return;
            }
        };
        let mut state = self.state.lock();
        let mut complete = page.next_cursor.is_none() && (page.items.len() as i64) < INIT_CACHES_REQUEST_LIMIT;
        for request in page.items {
            let Ok(request) = from_api_request(request) else {
                complete = false;
                continue;
            };
            let Some(id) = request.id.clone() else { continue };
            if request.handled_at.is_some() {
                state.handled.insert(id);
            } else {
                complete = false;
                state.cache(request);
            }
        }
        if complete {
            state.head_complete = true;
        }
    }

    async fn ensure_head_is_non_empty(&self) -> StorageResult<()> {
        let needs_listing = || {
            let state = self.state.lock();
            state.head.len() <= 1 && !state.head_complete
        };
        if !needs_listing() {
            return Ok(());
        }
        let _listing = self.listing.lock().await;
        if needs_listing() {
            self.list_pending().await?;
        }
        Ok(())
    }

    /// Adds every pending request of the queue this client does not know to the local head, in
    /// the queue's order, page by page.
    async fn list_pending(&self) -> StorageResult<()> {
        let mut cursor = None;
        loop {
            let options = ListRequestsOptions {
                limit: Some(MAX_LISTED_REQUESTS),
                cursor: cursor.take(),
                filter: Some(vec!["pending".to_owned()]),
                ..ListRequestsOptions::default()
            };
            let page = self.client.list_requests(options).await.map_err(backend_error)?;
            let mut state = self.state.lock();
            let mut in_head: HashSet<String> = state.head.iter().cloned().collect();
            for request in page.items {
                let Ok(request) = from_api_request(request) else { continue };
                let Some(id) = request.id.clone() else { continue };
                if request.handled_at.is_some()
                    || state.in_progress.contains(&id)
                    || state.handled.contains(&id)
                    || !in_head.insert(id.clone())
                {
                    continue;
                }
                state.cache(request);
                state.head.push_back(id);
            }
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        self.state.lock().head_complete = true;
        Ok(())
    }

    /// Adds the requests of the platform's queue head this client does not know to the local head.
    /// Used to confirm the end: when nothing is in progress, it returns only requests this client
    /// does not know.
    async fn list_head(&self) -> StorageResult<()> {
        // The head listing includes the requests in progress, so ask for enough to find new ones.
        let limit = MAX_HEAD_ITEMS.min(DESIRED_NEW_HEAD_ITEMS + self.state.lock().in_progress.len());
        let head = self.client.list_head(Some(limit as i64)).await.map_err(backend_error)?;
        let mut state = self.state.lock();
        // Fewer than asked for: the listing has every unhandled request.
        state.head_complete = head.items.len() < limit;
        for item in head.items {
            let Some(id) = item.id else { continue };
            if state.in_progress.contains(&id) || state.handled.contains(&id) || state.head.contains(&id) {
                continue;
            }
            state.head.push_back(id);
        }
        Ok(())
    }

    /// Whether the queue has the request, asking the platform only when this client does not know it.
    async fn is_known_or_exists(&self, id: &str) -> StorageResult<bool> {
        {
            let mut state = self.state.lock();
            if state.in_progress.contains(id) || state.handled.contains(id) || state.cached.get(id).is_some() {
                return Ok(true);
            }
        }
        Ok(self.get_request_by_id(id).await?.is_some())
    }
}

#[async_trait]
impl RequestQueueBackend for ApifySingleRequestQueue {
    async fn get_metadata(&self) -> StorageResult<RequestQueueInfo> {
        let (estimated_total, estimated_handled) = {
            let state = self.state.lock();
            (state.estimated_total, state.estimated_handled)
        };
        queue_metadata(&self.client, estimated_total, estimated_handled).await
    }

    async fn drop_storage(&self) -> StorageResult<()> {
        self.client.delete().await.map_err(backend_error)?;
        self.drop_hook.dropped();
        Ok(())
    }

    async fn purge(&self) -> StorageResult<()> {
        Err(unsupported(PURGE_UNSUPPORTED))
    }

    async fn add_batch_of_requests(
        &self,
        requests: Vec<Request>,
        forefront: bool,
    ) -> StorageResult<BatchAddRequestsResult> {
        self.caches_initialized.get_or_init(|| self.init_caches()).await;

        // Requests this client knows are deduplicated locally: a write costs an API call and a
        // paid write operation.
        let mut already_present = Vec::new();
        let mut new_requests = Vec::new();
        {
            let mut state = self.state.lock();
            for request in requests {
                let id = unique_key_to_request_id(&request.unique_key);
                if state.handled.contains(&id) {
                    already_present.push(ProcessedRequest {
                        unique_key: request.unique_key,
                        request_id: id,
                        was_already_present: true,
                        was_already_handled: true,
                    });
                } else if state.cached.get(&id).is_some() {
                    already_present.push(ProcessedRequest {
                        was_already_handled: request.handled_at.is_some(),
                        unique_key: request.unique_key,
                        request_id: id,
                        was_already_present: true,
                    });
                } else {
                    new_requests.push(request);
                }
            }
        }

        let mut result = BatchAddRequestsResult::default();
        if !new_requests.is_empty() {
            result = send_batch(&self.client, &new_requests, forefront).await?;

            // The platform's answer is authoritative: a request it reports as handled (by an
            // earlier run, beyond the prefetch limit) must not enter the head again.
            let processed_by_key: HashMap<&str, &ProcessedRequest> =
                result.processed_requests.iter().map(|processed| (processed.unique_key.as_str(), processed)).collect();
            let mut state = self.state.lock();
            for mut request in new_requests {
                let Some(processed) = processed_by_key.get(request.unique_key.as_str()) else { continue };
                if processed.was_already_handled {
                    state.handled.insert(processed.request_id.clone());
                    continue;
                }
                request.id = Some(processed.request_id.clone());
                state.cache(request);
                state.push_head(processed.request_id.clone(), forefront);
            }
        }

        result.processed_requests.extend(already_present);
        self.state.lock().estimated_total += newly_added(&result);
        Ok(result)
    }

    async fn get_request(&self, unique_key: &str) -> StorageResult<Option<Request>> {
        let id = unique_key_to_request_id(unique_key);
        if let Some(cached) = self.state.lock().cached.get(&id) {
            return Ok(Some(cached.clone()));
        }
        let Some(request) = self.get_request_by_id(&id).await? else { return Ok(None) };
        let mut state = self.state.lock();
        // Requests in progress are known to this client already.
        if !state.in_progress.contains(&id) {
            if request.handled_at.is_some() {
                state.handled.insert(id);
            } else {
                state.cache(request.clone());
            }
        }
        Ok(Some(request))
    }

    async fn fetch_next_request(&self) -> StorageResult<Option<Request>> {
        self.ensure_head_is_non_empty().await?;
        loop {
            let (id, cached) = {
                let mut state = self.state.lock();
                let Some(id) = state.head.pop_front() else { return Ok(None) };
                if state.in_progress.contains(&id) || state.handled.contains(&id) {
                    continue;
                }
                state.in_progress.insert(id.clone());
                let cached = state.cached.get(&id).cloned();
                (id, cached)
            };
            // Only requests added by another producer (found through the head listing) are fetched.
            let request = match cached {
                Some(request) => Some(request),
                None => self.get_request_by_id(&id).await?,
            };
            let mut state = self.state.lock();
            let Some(request) = request else {
                state.in_progress.remove(&id);
                continue;
            };
            if request.handled_at.is_some() {
                // Handled elsewhere in the meantime.
                state.in_progress.remove(&id);
                state.handled.insert(id.clone());
                state.cached.pop(&id);
                continue;
            }
            return Ok(Some(request));
        }
    }

    async fn mark_request_as_handled(&self, request: &Request) -> StorageResult<Option<QueueOperationInfo>> {
        let id = unique_key_to_request_id(&request.unique_key);
        // Marking a request the queue does not have is a no-op: the update endpoint would add it.
        if !self.is_known_or_exists(&id).await? {
            self.state.lock().in_progress.remove(&id);
            return Ok(None);
        }
        let mut handled = request.clone();
        handled.id = Some(id.clone());
        handled.handled_at.get_or_insert_with(crawlee::core::now_iso);
        let info = self.update_request(&handled, false).await?;

        let mut state = self.state.lock();
        state.in_progress.remove(&id);
        state.handled.insert(id.clone());
        state.cached.pop(&id);
        if !info.was_already_handled {
            state.estimated_handled += 1;
        }
        Ok(Some(info))
    }

    async fn reclaim_request(&self, request: &Request, forefront: bool) -> StorageResult<Option<QueueOperationInfo>> {
        let id = unique_key_to_request_id(&request.unique_key);
        if !self.is_known_or_exists(&id).await? {
            self.state.lock().in_progress.remove(&id);
            return Ok(None);
        }
        let mut reclaimed = request.clone();
        reclaimed.id = Some(id.clone());
        reclaimed.handled_at = None;
        let info = self.update_request(&reclaimed, forefront).await?;

        let mut state = self.state.lock();
        state.in_progress.remove(&id);
        state.handled.remove(&id);
        state.cache(reclaimed);
        // Back into the local head right away: the platform head lags behind the update, and
        // `is_finished` must not report true while a reclaimed request waits.
        if !state.head.contains(&id) {
            state.push_head(id, forefront);
        }
        if info.was_already_handled {
            state.estimated_handled = state.estimated_handled.saturating_sub(1);
        }
        Ok(Some(info))
    }

    async fn is_empty(&self) -> StorageResult<bool> {
        self.ensure_head_is_non_empty().await?;
        Ok(self.state.lock().head.is_empty())
    }

    async fn is_finished(&self) -> StorageResult<bool> {
        {
            let state = self.state.lock();
            if !state.head.is_empty() || !state.in_progress.is_empty() {
                return Ok(false);
            }
        }
        // Nothing left that this client knows of: a listing confirms it, and finds the requests
        // other clients added.
        self.list_head().await?;
        let state = self.state.lock();
        Ok(state.head.is_empty() && state.in_progress.is_empty())
    }
}
