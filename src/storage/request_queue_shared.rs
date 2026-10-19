//! A request queue consumed by any number of clients: fetched requests are locked on the
//! platform (`listAndLockHead`), a port of `ApifyRequestQueueSharedBackend` of the JS SDK.

use crawlee::core::local::CellExt as _;
use std::cell::Cell;
use std::collections::{HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::time::Duration;

use apify_client::clients::request_queue::RequestQueueClient;
use async_trait::async_trait;
use crawlee::Request;
use crawlee::core::StorageResult;
use crawlee::core::request::unique_key_to_request_id;
use crawlee::core::storage::backend::{
    BatchAddRequestsResult, ProcessedRequest, QueueOperationInfo, RequestQueueBackend, RequestQueueInfo,
};
use lru::LruCache;
use std::cell::RefCell;

use super::backend_error;
use super::request_queue::{
    PURGE_UNSUPPORTED, get_request_by_id, newly_added, queue_metadata, send_batch, update_request,
};
use super::unsupported;

const MAX_CACHED_REQUESTS: usize = 1_000_000;
/// How long a fetched request stays locked, unless a longer processing time is expected.
const DEFAULT_REQUEST_LOCK_SECS: u64 = 3 * 60;
/// How many requests one head listing locks.
const HEAD_LOCK_LIMIT: i64 = 25;

/// The local head, changed only under its lock.
#[derive(Default)]
struct Head {
    ids: VecDeque<String>,
    /// From the last listing: other clients hold locks.
    queue_has_locked_requests: bool,
}

struct State {
    /// Whether requests this client knows about were handled, by id.
    known: LruCache<String, bool>,
    in_progress: HashSet<String>,
    estimated_total: u64,
    estimated_handled: u64,
}

pub(crate) struct ApifySharedRequestQueue {
    client: RequestQueueClient,
    head: crawlee::core::local::Mutex<Head>,
    state: RefCell<State>,
    lock_secs: Cell<u64>,
}

impl ApifySharedRequestQueue {
    pub(crate) fn new(client: RequestQueueClient) -> Self {
        ApifySharedRequestQueue {
            client,
            head: crawlee::core::local::Mutex::default(),
            state: RefCell::new(State {
                known: LruCache::new(NonZeroUsize::new(MAX_CACHED_REQUESTS).expect("non-zero")),
                in_progress: HashSet::new(),
                estimated_total: 0,
                estimated_handled: 0,
            }),
            lock_secs: Cell::new(DEFAULT_REQUEST_LOCK_SECS),
        }
    }

    async fn ensure_head_is_non_empty(&self, head: &mut Head) -> StorageResult<()> {
        if head.ids.len() > 1 {
            return Ok(());
        }
        self.list_and_lock_head(head, HEAD_LOCK_LIMIT).await
    }

    async fn list_and_lock_head(&self, head: &mut Head, limit: i64) -> StorageResult<()> {
        let lock_secs = self.lock_secs.get() as i64;
        let listed = self.client.list_and_lock_head(lock_secs, Some(limit)).await.map_err(backend_error)?;
        head.queue_has_locked_requests = listed.queue_has_locked_requests.unwrap_or(false);
        let mut state = self.state.borrow_mut();
        for item in listed.items {
            let Some(id) = item.id else { continue };
            if state.in_progress.contains(&id) || head.ids.contains(&id) {
                continue;
            }
            state.known.put(id.clone(), false);
            head.ids.push_back(id);
        }
        Ok(())
    }

    async fn is_known_or_exists(&self, id: &str) -> StorageResult<bool> {
        {
            let mut state = self.state.borrow_mut();
            if state.in_progress.contains(id) || state.known.get(id).is_some() {
                return Ok(true);
            }
        }
        Ok(get_request_by_id(&self.client, id).await?.is_some())
    }
}

#[async_trait(?Send)]
impl RequestQueueBackend for ApifySharedRequestQueue {
    async fn get_metadata(&self) -> StorageResult<RequestQueueInfo> {
        let (total, handled) = {
            let state = self.state.borrow_mut();
            (state.estimated_total, state.estimated_handled)
        };
        queue_metadata(&self.client, total, handled).await
    }

    async fn drop_storage(&self) -> StorageResult<()> {
        self.client.delete().await.map_err(backend_error)
    }

    async fn purge(&self) -> StorageResult<()> {
        Err(unsupported(PURGE_UNSUPPORTED))
    }

    async fn add_batch_of_requests(
        &self,
        requests: Vec<Request>,
        forefront: bool,
    ) -> StorageResult<BatchAddRequestsResult> {
        // Requests known to be on the platform are not written again. Whether another client has
        // handled one since cannot be known locally; the last known state is reported.
        let mut already_present = Vec::new();
        let mut new_requests = Vec::new();
        {
            let mut state = self.state.borrow_mut();
            for request in requests {
                let id = unique_key_to_request_id(&request.unique_key);
                match state.known.get(&id) {
                    Some(&handled) => already_present.push(ProcessedRequest {
                        was_already_handled: handled || request.handled_at.is_some(),
                        unique_key: request.unique_key,
                        request_id: id,
                        was_already_present: true,
                    }),
                    None => new_requests.push(request),
                }
            }
        }

        let mut result = BatchAddRequestsResult::default();
        if !new_requests.is_empty() {
            result = send_batch(&self.client, &new_requests, forefront).await?;
            {
                let mut state = self.state.borrow_mut();
                for processed in &result.processed_requests {
                    state.known.put(processed.request_id.clone(), processed.was_already_handled);
                }
            }
        }
        result.processed_requests.extend(already_present);
        self.state.borrow_mut().estimated_total += newly_added(&result);
        Ok(result)
    }

    /// Always read from the platform: other clients may change requests at any time.
    async fn get_request(&self, unique_key: &str) -> StorageResult<Option<Request>> {
        get_request_by_id(&self.client, &unique_key_to_request_id(unique_key)).await
    }

    async fn fetch_next_request(&self) -> StorageResult<Option<Request>> {
        let id = {
            let mut head = self.head.lock().await;
            self.ensure_head_is_non_empty(&mut head).await?;
            head.ids.pop_front()
        };
        let Some(id) = id else { return Ok(None) };
        // Head items are partial (no user data, payload or headers).
        let Some(request) = get_request_by_id(&self.client, &id).await? else {
            // The head can list a request the main table does not serve yet; it comes back later.
            tracing::debug!("Request fetched from the queue head was not found (id: {id}), will be retried later");
            return Ok(None);
        };
        let mut state = self.state.borrow_mut();
        if request.handled_at.is_some() {
            // Handled by another client in the meantime.
            state.known.put(id, true);
            return Ok(None);
        }
        state.in_progress.insert(id);
        Ok(Some(request))
    }

    async fn mark_request_as_handled(&self, request: &Request) -> StorageResult<Option<QueueOperationInfo>> {
        let id = unique_key_to_request_id(&request.unique_key);
        if !self.is_known_or_exists(&id).await? {
            self.state.borrow_mut().in_progress.remove(&id);
            return Ok(None);
        }
        let mut handled = request.clone();
        handled.id = Some(id.clone());
        handled.handled_at.get_or_insert_with(crawlee::core::now_iso);
        let info = update_request(&self.client, &handled, false).await?;

        let mut state = self.state.borrow_mut();
        state.in_progress.remove(&id);
        state.known.put(id, true);
        if !info.was_already_handled {
            state.estimated_handled += 1;
        }
        Ok(Some(info))
    }

    async fn reclaim_request(&self, request: &Request, forefront: bool) -> StorageResult<Option<QueueOperationInfo>> {
        let id = unique_key_to_request_id(&request.unique_key);
        if !self.is_known_or_exists(&id).await? {
            self.state.borrow_mut().in_progress.remove(&id);
            return Ok(None);
        }
        let _head = self.head.lock().await;
        let mut reclaimed = request.clone();
        reclaimed.id = Some(id.clone());
        reclaimed.handled_at = None;
        let info = update_request(&self.client, &reclaimed, forefront).await?;
        // Unlocked, so that any consumer can fetch it right away instead of after the lock expires.
        if let Err(err) = self.client.delete_request_lock(&id, forefront).await {
            tracing::debug!("Failed to delete the lock of a reclaimed request (id: {id}): {err}");
        }
        let mut state = self.state.borrow_mut();
        state.in_progress.remove(&id);
        state.known.put(id, false);
        if info.was_already_handled {
            state.estimated_handled = state.estimated_handled.saturating_sub(1);
        }
        Ok(Some(info))
    }

    async fn is_empty(&self) -> StorageResult<bool> {
        let mut head = self.head.lock().await;
        if !head.ids.is_empty() {
            return Ok(false);
        }
        self.list_and_lock_head(&mut head, 1).await?;
        Ok(head.ids.is_empty())
    }

    async fn is_finished(&self) -> StorageResult<bool> {
        let mut head = self.head.lock().await;
        if !head.ids.is_empty() {
            return Ok(false);
        }
        // The listing also refreshes `queue_has_locked_requests`.
        self.list_and_lock_head(&mut head, 1).await?;
        Ok(head.ids.is_empty() && !head.queue_has_locked_requests)
    }

    /// Only ever raised: a short-lived consumer must not cut the locks of a long-running one short.
    async fn set_expected_request_processing_time(&self, duration: Duration) -> StorageResult<()> {
        self.lock_secs.fetch_max(duration.as_secs());
        Ok(())
    }

    /// Prolongs the lock of a request being processed (the JS SDK never does).
    async fn extend_request_processing_time(&self, request_id: &str, duration: Duration) -> StorageResult<bool> {
        if !self.state.borrow_mut().in_progress.contains(request_id) {
            return Ok(false);
        }
        let lock_secs = duration.as_secs().max(1) as i64;
        self.client.prolong_request_lock(request_id, lock_secs, false).await.map_err(backend_error)?;
        Ok(true)
    }
}
