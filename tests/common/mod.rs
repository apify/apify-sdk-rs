//! A fake Apify API: the storage endpoints the SDK uses, kept in memory. It can make the request
//! queue head lag behind writes, as it does on the platform, and answer with rate limits.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use crawlee::core::request::unique_key_to_request_id;
use parking_lot::Mutex;
use serde_json::{Value, json};

pub const DEFAULT_DATASET: &str = "default-dataset";
pub const DEFAULT_STORE: &str = "default-store";
pub const DEFAULT_QUEUE: &str = "default-queue";
pub const TOKEN: &str = "test-token";

#[derive(Default)]
struct Dataset {
    name: Option<String>,
    items: Vec<Value>,
}

#[derive(Default)]
struct Store {
    name: Option<String>,
    records: BTreeMap<String, (Vec<u8>, String)>,
}

struct QueuedRequest {
    body: Value,
    order: i64,
    /// The client holding the lock, and until when.
    lock: Option<(String, std::time::Instant)>,
    /// The head listing number from which the request shows in the head.
    visible_from: u64,
}

#[derive(Default)]
struct Queue {
    name: Option<String>,
    requests: HashMap<String, QueuedRequest>,
    next_order: i64,
    head_listings: u64,
}

impl Queue {
    fn order(&mut self, forefront: bool) -> i64 {
        self.next_order += 1;
        if forefront { -self.next_order } else { self.next_order }
    }

    fn info(&self, id: &str) -> Value {
        let handled = self.requests.values().filter(|r| !r.body["handledAt"].is_null()).count();
        json!({
            "id": id, "name": self.name, "userId": "user", "createdAt": NOW, "modifiedAt": NOW, "accessedAt": NOW,
            "totalRequestCount": self.requests.len(), "handledRequestCount": handled,
            "pendingRequestCount": self.requests.len() - handled,
        })
    }
}

#[derive(Default)]
struct Data {
    datasets: HashMap<String, Dataset>,
    stores: HashMap<String, Store>,
    queues: HashMap<String, Queue>,
    next_id: u64,
    calls: Vec<String>,
    /// Bodies of run updates, in order.
    run_updates: Vec<Value>,
    /// Starts, aborts, metamorphs and webhooks: `(call, query, body)`.
    platform_calls: Vec<(String, HashMap<String, String>, Value)>,
    /// Runs started through the API, by id. They finish at once.
    runs: HashMap<String, Value>,
}

impl Data {
    fn new_id(&mut self) -> String {
        self.next_id += 1;
        format!("storage-{}", self.next_id)
    }
}

const NOW: &str = "2026-01-01T00:00:00.000Z";

#[derive(Clone, Default)]
struct Shared {
    data: Arc<Mutex<Data>>,
    /// How many requests to answer with `429 Too Many Requests`.
    rate_limited: Arc<AtomicU32>,
    /// How many head listings a newly added request stays out of the head.
    head_lag: Arc<AtomicU32>,
}

/// A running fake API. Cheap to clone.
#[derive(Clone)]
pub struct FakeApi {
    pub url: String,
    shared: Shared,
}

impl FakeApi {
    /// A fake API with the default storages of a run.
    pub async fn start() -> FakeApi {
        let shared = Shared::default();
        {
            let mut data = shared.data.lock();
            data.datasets.insert(DEFAULT_DATASET.to_owned(), Dataset::default());
            data.stores.insert(DEFAULT_STORE.to_owned(), Store::default());
            data.queues.insert(DEFAULT_QUEUE.to_owned(), Queue::default());
        }
        let app = Router::new()
            .route("/v2/datasets", post(create_dataset))
            .route("/v2/datasets/{id}", get(get_dataset).delete(delete_dataset))
            .route("/v2/datasets/{id}/items", get(list_items).post(push_items))
            .route("/v2/key-value-stores", post(create_store))
            .route("/v2/key-value-stores/{id}", get(get_store).delete(delete_store))
            .route("/v2/key-value-stores/{id}/keys", get(list_keys))
            .route("/v2/key-value-stores/{id}/records/{key}", get(get_record).put(put_record).delete(delete_record))
            .route("/v2/request-queues", post(create_queue))
            .route("/v2/request-queues/{id}", get(get_queue).delete(delete_queue))
            .route("/v2/request-queues/{id}/head", get(list_head))
            .route("/v2/request-queues/{id}/head/lock", post(list_and_lock_head))
            .route(
                "/v2/request-queues/{id}/requests/{request_id}/lock",
                axum::routing::put(prolong_lock).delete(delete_lock),
            )
            .route("/v2/request-queues/{id}/requests", get(list_requests))
            .route("/v2/request-queues/{id}/requests/batch", post(batch_add))
            .route("/v2/request-queues/{id}/requests/{request_id}", get(get_request).put(update_request))
            .route("/v2/users/me", get(get_me))
            .route("/v2/actor-runs/{id}", axum::routing::put(update_run).get(get_run))
            .route("/v2/actor-runs/{id}/abort", post(run_action))
            .route("/v2/actor-runs/{id}/charge", post(run_action))
            .route("/v2/actor-runs/{id}/metamorph", post(run_action))
            .route("/v2/actors/{id}/runs", post(start_run))
            .route("/v2/actor-tasks/{id}/runs", post(start_run))
            .route("/v2/webhooks", post(create_webhook))
            .route("/v2/actor-runs/{id}/reboot", post(reboot_run))
            .route("/v2/actor-runs/{id}/resurrect", post(resurrect_run))
            .layer(tower_http::decompression::RequestDecompressionLayer::new())
            .layer(middleware::from_fn_with_state(shared.clone(), gate))
            .with_state(shared.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        FakeApi { url, shared }
    }

    /// The next `count` requests are answered with `429 Too Many Requests`.
    pub fn rate_limit_next(&self, count: u32) {
        self.shared.rate_limited.store(count, Ordering::SeqCst);
    }

    /// Requests added from now on show in the queue head only after `listings` head listings.
    pub fn set_head_lag(&self, listings: u32) {
        self.shared.head_lag.store(listings, Ordering::SeqCst);
    }

    /// The API calls made so far, as `"METHOD /path"`.
    pub fn calls(&self) -> Vec<String> {
        self.shared.data.lock().calls.clone()
    }

    pub fn count_calls(&self, prefix: &str) -> usize {
        self.calls().iter().filter(|call| call.starts_with(prefix)).count()
    }

    pub fn clear_calls(&self) {
        self.shared.data.lock().calls.clear();
    }

    pub fn dataset_items(&self, id: &str) -> Vec<Value> {
        self.shared.data.lock().datasets.get(id).map(|d| d.items.clone()).unwrap_or_default()
    }

    pub fn record(&self, store: &str, key: &str) -> Option<(Vec<u8>, String)> {
        self.shared.data.lock().stores.get(store)?.records.get(key).cloned()
    }

    pub fn put_record(&self, store: &str, key: &str, value: &[u8], content_type: &str) {
        let mut data = self.shared.data.lock();
        data.stores
            .entry(store.to_owned())
            .or_default()
            .records
            .insert(key.to_owned(), (value.to_vec(), content_type.to_owned()));
    }

    pub fn storage_count(&self) -> (usize, usize, usize) {
        let data = self.shared.data.lock();
        (data.datasets.len(), data.stores.len(), data.queues.len())
    }

    pub fn delete_dataset(&self, id: &str) {
        self.shared.data.lock().datasets.remove(id);
    }

    /// Adds a request as another client would.
    pub fn add_request(&self, queue: &str, url: &str) {
        let mut data = self.shared.data.lock();
        let lag = self.shared.head_lag.load(Ordering::SeqCst);
        let queue = data.queues.get_mut(queue).unwrap();
        let id = unique_key_to_request_id(url);
        let order = queue.order(false);
        let visible_from = queue.head_listings + u64::from(lag);
        let body = json!({ "id": id, "url": url, "uniqueKey": url, "method": "GET", "retryCount": 0 });
        queue.requests.insert(id, QueuedRequest { body, order, visible_from, lock: None });
    }

    pub fn platform_calls(&self) -> Vec<(String, HashMap<String, String>, Value)> {
        self.shared.data.lock().platform_calls.clone()
    }

    /// Sets the status of a run started through the API.
    pub fn set_run_status(&self, id: &str, status: &str) {
        self.shared.data.lock().runs.get_mut(id).expect("a started run")["status"] = json!(status);
    }

    pub fn run_updates(&self) -> Vec<Value> {
        self.shared.data.lock().run_updates.clone()
    }

    pub fn queue_request(&self, queue: &str, id: &str) -> Option<Value> {
        Some(self.shared.data.lock().queues.get(queue)?.requests.get(id)?.body.clone())
    }
}

async fn gate(State(shared): State<Shared>, request: Request, next: Next) -> Response {
    let call = format!("{} {}", request.method(), request.uri().path());
    shared.data.lock().calls.push(call);
    let authorized = request
        .headers()
        .get(header::AUTHORIZATION)
        .is_some_and(|value| value.to_str().is_ok_and(|v| v == format!("Bearer {TOKEN}")));
    if !authorized {
        return error(StatusCode::UNAUTHORIZED, "token-not-provided");
    }
    // Takes one of the remaining rate-limited answers, if any. Not `fetch_update`, deprecated in
    // newer Rust, nor `try_update`, newer than the MSRV.
    let mut remaining = shared.rate_limited.load(Ordering::SeqCst);
    let limited = loop {
        let Some(next) = remaining.checked_sub(1) else { break false };
        match shared.rate_limited.compare_exchange(remaining, next, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => break true,
            Err(current) => remaining = current,
        }
    };
    if limited {
        return error(StatusCode::TOO_MANY_REQUESTS, "rate-limit-exceeded");
    }
    next.run(request).await
}

fn error(status: StatusCode, error_type: &str) -> Response {
    (status, axum::Json(json!({ "error": { "type": error_type, "message": error_type } }))).into_response()
}

fn not_found() -> Response {
    error(StatusCode::NOT_FOUND, "record-not-found")
}

fn envelope(status: StatusCode, value: Value) -> Response {
    (status, axum::Json(json!({ "data": value }))).into_response()
}

fn flag(query: &HashMap<String, String>, name: &str) -> bool {
    query.get(name).is_some_and(|value| value == "1" || value == "true")
}

fn number(query: &HashMap<String, String>, name: &str) -> Option<usize> {
    query.get(name).and_then(|value| value.parse().ok())
}

// ─── Datasets ───────────────────────────────────────────────────────────────

fn dataset_info(id: &str, dataset: &Dataset) -> Value {
    json!({
        "id": id, "name": dataset.name, "userId": "user", "createdAt": NOW, "modifiedAt": NOW, "accessedAt": NOW,
        "itemCount": dataset.items.len(), "cleanItemCount": dataset.items.len(),
    })
}

async fn create_dataset(State(shared): State<Shared>, Query(query): Query<HashMap<String, String>>) -> Response {
    let mut data = shared.data.lock();
    let name = query.get("name").cloned();
    if let Some(name) = &name
        && let Some((id, dataset)) = data.datasets.iter().find(|(_, d)| d.name.as_ref() == Some(name))
    {
        return data_created(dataset_info(id, dataset));
    }
    let id = data.new_id();
    let dataset = Dataset { name, ..Dataset::default() };
    let info = dataset_info(&id, &dataset);
    data.datasets.insert(id, dataset);
    data_created(info)
}

fn data_created(value: Value) -> Response {
    envelope(StatusCode::CREATED, value)
}

fn find_dataset<'a>(data: &'a mut Data, id: &str) -> Option<(String, &'a mut Dataset)> {
    let id = if data.datasets.contains_key(id) {
        id.to_owned()
    } else {
        data.datasets.iter().find(|(_, d)| d.name.as_deref() == Some(id))?.0.clone()
    };
    let dataset = data.datasets.get_mut(&id)?;
    Some((id, dataset))
}

async fn get_dataset(State(shared): State<Shared>, Path(id): Path<String>) -> Response {
    let mut data = shared.data.lock();
    match find_dataset(&mut data, &id) {
        Some((id, dataset)) => envelope(StatusCode::OK, dataset_info(&id, dataset)),
        None => not_found(),
    }
}

async fn delete_dataset(State(shared): State<Shared>, Path(id): Path<String>) -> Response {
    shared.data.lock().datasets.remove(&id);
    StatusCode::NO_CONTENT.into_response()
}

async fn push_items(State(shared): State<Shared>, Path(id): Path<String>, body: Bytes) -> Response {
    let mut data = shared.data.lock();
    let Some((_, dataset)) = find_dataset(&mut data, &id) else { return not_found() };
    match serde_json::from_slice::<Value>(&body) {
        Ok(Value::Array(items)) => dataset.items.extend(items),
        Ok(item @ Value::Object(_)) => dataset.items.push(item),
        _ => return error(StatusCode::BAD_REQUEST, "invalid-input"),
    }
    StatusCode::CREATED.into_response()
}

async fn list_items(
    State(shared): State<Shared>,
    Path(id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let mut data = shared.data.lock();
    let Some((_, dataset)) = find_dataset(&mut data, &id) else { return not_found() };
    let mut items = dataset.items.clone();
    if flag(&query, "desc") {
        items.reverse();
    }
    let total = items.len();
    let offset = number(&query, "offset").unwrap_or(0);
    let limit = number(&query, "limit").unwrap_or(usize::MAX);
    let page: Vec<Value> = items.into_iter().skip(offset).take(limit).collect();
    let mut headers = HeaderMap::new();
    headers.insert("x-apify-pagination-total", total.into());
    headers.insert("x-apify-pagination-offset", offset.into());
    headers.insert("x-apify-pagination-limit", limit.min(u32::MAX as usize).into());
    headers.insert("x-apify-pagination-count", page.len().into());
    (headers, axum::Json(page)).into_response()
}

// ─── Key-value stores ───────────────────────────────────────────────────────

fn store_info(id: &str, store: &Store) -> Value {
    json!({ "id": id, "name": store.name, "userId": "user", "createdAt": NOW, "modifiedAt": NOW, "accessedAt": NOW })
}

async fn create_store(State(shared): State<Shared>, Query(query): Query<HashMap<String, String>>) -> Response {
    let mut data = shared.data.lock();
    let name = query.get("name").cloned();
    if let Some(name) = &name
        && let Some((id, store)) = data.stores.iter().find(|(_, s)| s.name.as_ref() == Some(name))
    {
        return data_created(store_info(id, store));
    }
    let id = data.new_id();
    let store = Store { name, ..Store::default() };
    let info = store_info(&id, &store);
    data.stores.insert(id, store);
    data_created(info)
}

async fn get_store(State(shared): State<Shared>, Path(id): Path<String>) -> Response {
    let data = shared.data.lock();
    match data.stores.get(&id) {
        Some(store) => envelope(StatusCode::OK, store_info(&id, store)),
        None => not_found(),
    }
}

async fn delete_store(State(shared): State<Shared>, Path(id): Path<String>) -> Response {
    shared.data.lock().stores.remove(&id);
    StatusCode::NO_CONTENT.into_response()
}

async fn list_keys(
    State(shared): State<Shared>,
    Path(id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let data = shared.data.lock();
    let Some(store) = data.stores.get(&id) else { return not_found() };
    let limit = number(&query, "limit").unwrap_or(1000);
    let start = query.get("exclusiveStartKey");
    let prefix = query.get("prefix").map(String::as_str).unwrap_or("");
    let keys: Vec<(&String, usize)> = store
        .records
        .iter()
        .filter(|(key, _)| start.is_none_or(|start| key.as_str() > start.as_str()) && key.starts_with(prefix))
        .map(|(key, (value, _))| (key, value.len()))
        .collect();
    let page: Vec<Value> = keys.iter().take(limit).map(|(key, size)| json!({ "key": key, "size": size })).collect();
    let truncated = keys.len() > limit;
    envelope(
        StatusCode::OK,
        json!({
            "items": page, "count": page.len(), "limit": limit, "exclusiveStartKey": start, "isTruncated": truncated,
            "nextExclusiveStartKey": if truncated { page.last().map(|item| item["key"].clone()) } else { None },
        }),
    )
}

async fn get_record(State(shared): State<Shared>, Path((id, key)): Path<(String, String)>, method: Method) -> Response {
    let data = shared.data.lock();
    let Some((value, content_type)) = data.stores.get(&id).and_then(|store| store.records.get(&key)) else {
        return if method == Method::HEAD { StatusCode::NOT_FOUND.into_response() } else { not_found() };
    };
    ([(header::CONTENT_TYPE, content_type.clone())], value.clone()).into_response()
}

async fn put_record(
    State(shared): State<Shared>,
    Path((id, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mut data = shared.data.lock();
    let Some(store) = data.stores.get_mut(&id) else { return not_found() };
    let content_type = headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or("").to_owned();
    store.records.insert(key, (body.to_vec(), content_type));
    StatusCode::CREATED.into_response()
}

async fn delete_record(State(shared): State<Shared>, Path((id, key)): Path<(String, String)>) -> Response {
    if let Some(store) = shared.data.lock().stores.get_mut(&id) {
        store.records.remove(&key);
    }
    StatusCode::NO_CONTENT.into_response()
}

// ─── Request queues ─────────────────────────────────────────────────────────

async fn create_queue(State(shared): State<Shared>, Query(query): Query<HashMap<String, String>>) -> Response {
    let mut data = shared.data.lock();
    let name = query.get("name").cloned();
    if let Some(name) = &name
        && let Some((id, queue)) = data.queues.iter().find(|(_, q)| q.name.as_ref() == Some(name))
    {
        return data_created(queue.info(id));
    }
    let id = data.new_id();
    let queue = Queue { name, ..Queue::default() };
    let info = queue.info(&id);
    data.queues.insert(id, queue);
    data_created(info)
}

async fn get_queue(State(shared): State<Shared>, Path(id): Path<String>) -> Response {
    let data = shared.data.lock();
    match data.queues.get(&id) {
        Some(queue) => envelope(StatusCode::OK, queue.info(&id)),
        None => not_found(),
    }
}

async fn delete_queue(State(shared): State<Shared>, Path(id): Path<String>) -> Response {
    shared.data.lock().queues.remove(&id);
    StatusCode::NO_CONTENT.into_response()
}

async fn list_head(
    State(shared): State<Shared>,
    Path(id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let mut data = shared.data.lock();
    let Some(queue) = data.queues.get_mut(&id) else { return not_found() };
    queue.head_listings += 1;
    let listing = queue.head_listings;
    let limit = number(&query, "limit").unwrap_or(100);
    let mut pending: Vec<&QueuedRequest> =
        queue.requests.values().filter(|r| r.body["handledAt"].is_null() && r.visible_from < listing).collect();
    pending.sort_by_key(|r| r.order);
    let items: Vec<Value> = pending
        .into_iter()
        .take(limit)
        .map(|r| {
            let b = &r.body;
            json!({ "id": b["id"], "uniqueKey": b["uniqueKey"], "url": b["url"], "method": b["method"], "retryCount": b["retryCount"] })
        })
        .collect();
    envelope(
        StatusCode::OK,
        json!({ "limit": limit, "queueModifiedAt": NOW, "hadMultipleClients": false, "items": items }),
    )
}

async fn list_requests(
    State(shared): State<Shared>,
    Path(id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let data = shared.data.lock();
    let Some(queue) = data.queues.get(&id) else { return not_found() };
    let limit = number(&query, "limit").unwrap_or(1000);
    let mut requests: Vec<&QueuedRequest> = queue.requests.values().collect();
    requests.sort_by_key(|r| r.order);
    let items: Vec<Value> = requests.into_iter().take(limit).map(|r| r.body.clone()).collect();
    envelope(StatusCode::OK, json!({ "limit": limit, "items": items }))
}

async fn batch_add(
    State(shared): State<Shared>,
    Path(id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    let lag = shared.head_lag.load(Ordering::SeqCst);
    let mut data = shared.data.lock();
    let Some(queue) = data.queues.get_mut(&id) else { return not_found() };
    let Ok(Value::Array(requests)) = serde_json::from_slice::<Value>(&body) else {
        return error(StatusCode::BAD_REQUEST, "invalid-input");
    };
    let forefront = flag(&query, "forefront");
    let mut processed = Vec::new();
    for mut request in requests {
        if request.get("id").is_some() {
            return error(StatusCode::BAD_REQUEST, "invalid-input: id must not be set");
        }
        let unique_key = request["uniqueKey"].as_str().unwrap().to_owned();
        let request_id = unique_key_to_request_id(&unique_key);
        let (present, handled) = match queue.requests.get(&request_id) {
            Some(existing) => (true, !existing.body["handledAt"].is_null()),
            None => {
                request["id"] = json!(request_id);
                let order = queue.order(forefront);
                let visible_from = queue.head_listings + u64::from(lag);
                queue
                    .requests
                    .insert(request_id.clone(), QueuedRequest { body: request, order, visible_from, lock: None });
                (false, false)
            }
        };
        processed.push(json!({
            "requestId": request_id, "uniqueKey": unique_key, "wasAlreadyPresent": present, "wasAlreadyHandled": handled,
        }));
    }
    data_created(json!({ "processedRequests": processed, "unprocessedRequests": [] }))
}

async fn get_request(State(shared): State<Shared>, Path((id, request_id)): Path<(String, String)>) -> Response {
    let data = shared.data.lock();
    match data.queues.get(&id).and_then(|queue| queue.requests.get(&request_id)) {
        Some(request) => envelope(StatusCode::OK, request.body.clone()),
        None => not_found(),
    }
}

async fn update_request(
    State(shared): State<Shared>,
    Path((id, request_id)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    let mut data = shared.data.lock();
    let Some(queue) = data.queues.get_mut(&id) else { return not_found() };
    let Ok(request) = serde_json::from_slice::<Value>(&body) else {
        return error(StatusCode::BAD_REQUEST, "invalid-input");
    };
    let forefront = flag(&query, "forefront");
    let order = queue.order(forefront);
    let listing = queue.head_listings;
    let (present, handled) = match queue.requests.get_mut(&request_id) {
        Some(existing) => {
            let handled = !existing.body["handledAt"].is_null();
            if !request["handledAt"].is_null() {
                existing.lock = None;
            }
            existing.body = request;
            existing.order = order;
            existing.visible_from = listing;
            (true, handled)
        }
        None => {
            queue
                .requests
                .insert(request_id.clone(), QueuedRequest { body: request, order, visible_from: listing, lock: None });
            (false, false)
        }
    };
    envelope(
        StatusCode::OK,
        json!({ "requestId": request_id, "wasAlreadyPresent": present, "wasAlreadyHandled": handled }),
    )
}

// ─── Runs ───────────────────────────────────────────────────────────────────

async fn update_run(State(shared): State<Shared>, Path(id): Path<String>, body: Bytes) -> Response {
    let Ok(update) = serde_json::from_slice::<Value>(&body) else {
        return error(StatusCode::BAD_REQUEST, "invalid-input");
    };
    let mut run = json!({ "id": id, "status": "RUNNING" });
    if let (Value::Object(run), Value::Object(update)) = (&mut run, &update) {
        run.extend(update.clone());
    }
    shared.data.lock().run_updates.push(update);
    envelope(StatusCode::OK, run)
}

async fn reboot_run(Path(id): Path<String>) -> Response {
    envelope(StatusCode::OK, json!({ "id": id, "status": "RUNNING" }))
}

/// A websocket server that sends `messages` to the first client after `delay`, then keeps the
/// connection open until the client closes it.
pub async fn serve_events(delay: std::time::Duration, messages: Vec<Value>) -> String {
    use futures_util::{SinkExt as _, StreamExt as _};
    use tokio_tungstenite::tungstenite::Message;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        tokio::time::sleep(delay).await;
        for message in messages {
            socket.send(Message::text(message.to_string())).await.unwrap();
        }
        while let Some(Ok(_)) = socket.next().await {}
    });
    url
}

async fn get_me() -> Response {
    envelope(StatusCode::OK, json!({ "id": "user", "username": "tester", "proxy": { "password": "proxy-secret" } }))
}

fn record_platform_call(shared: &Shared, call: String, query: HashMap<String, String>, body: &[u8]) {
    let body = serde_json::from_slice(body).unwrap_or(Value::Null);
    shared.data.lock().platform_calls.push((call, query, body));
}

async fn start_run(
    State(shared): State<Shared>,
    uri: axum::http::Uri,
    Query(query): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    record_platform_call(&shared, uri.path().to_owned(), query, &body);
    let mut data = shared.data.lock();
    let id = format!("child-{}", data.runs.len() + 1);
    data.runs.insert(id.clone(), json!({ "id": id, "status": "SUCCEEDED", "startedAt": NOW }));
    envelope(StatusCode::CREATED, json!({ "id": id, "status": "READY", "startedAt": NOW }))
}

async fn get_run(State(shared): State<Shared>, Path(id): Path<String>) -> Response {
    let run = shared.data.lock().runs.get(&id).cloned();
    envelope(StatusCode::OK, run.unwrap_or_else(|| json!({ "id": id, "status": "SUCCEEDED" })))
}

async fn resurrect_run(
    State(shared): State<Shared>,
    uri: axum::http::Uri,
    Path(id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    record_platform_call(&shared, uri.path().to_owned(), query, &[]);
    let mut data = shared.data.lock();
    let run = data.runs.entry(id.clone()).or_insert_with(|| json!({ "id": id, "startedAt": NOW }));
    run["status"] = json!("RUNNING");
    envelope(StatusCode::OK, run.clone())
}

async fn run_action(
    State(shared): State<Shared>,
    uri: axum::http::Uri,
    Query(query): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    record_platform_call(&shared, uri.path().to_owned(), query, &body);
    if uri.path().ends_with("/charge") {
        // As slow as a real API call, so that charges waiting for each other show.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }
    envelope(StatusCode::OK, json!({ "id": "run", "status": "ABORTING" }))
}

async fn create_webhook(State(shared): State<Shared>, body: Bytes) -> Response {
    record_platform_call(&shared, "/v2/webhooks".to_owned(), HashMap::new(), &body);
    envelope(StatusCode::CREATED, json!({ "id": "webhook-1" }))
}

impl QueuedRequest {
    fn locked_by_other(&self, client_key: &str) -> bool {
        self.lock.as_ref().is_some_and(|(key, until)| key != client_key && *until > std::time::Instant::now())
    }

    fn is_locked(&self) -> bool {
        self.lock.as_ref().is_some_and(|(_, until)| *until > std::time::Instant::now())
    }
}

async fn list_and_lock_head(
    State(shared): State<Shared>,
    Path(id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let mut data = shared.data.lock();
    let Some(queue) = data.queues.get_mut(&id) else { return not_found() };
    let client_key = query.get("clientKey").cloned().unwrap_or_default();
    let limit = number(&query, "limit").unwrap_or(25);
    let lock_secs = number(&query, "lockSecs").unwrap_or(60) as u64;
    let until = std::time::Instant::now() + std::time::Duration::from_secs(lock_secs);
    let mut pending: Vec<&mut QueuedRequest> =
        queue.requests.values_mut().filter(|r| r.body["handledAt"].is_null() && !r.is_locked()).collect();
    pending.sort_by_key(|r| r.order);
    let mut items = Vec::new();
    for request in pending.into_iter().take(limit) {
        request.lock = Some((client_key.clone(), until));
        let b = &request.body;
        items.push(json!({ "id": b["id"], "uniqueKey": b["uniqueKey"], "url": b["url"], "method": b["method"] }));
    }
    let has_locked = queue.requests.values().any(|r| r.body["handledAt"].is_null() && r.is_locked());
    envelope(
        StatusCode::OK,
        json!({ "limit": limit, "lockSecs": lock_secs, "queueHasLockedRequests": has_locked, "clientKey": client_key,
                "hadMultipleClients": true, "items": items }),
    )
}

async fn prolong_lock(
    State(shared): State<Shared>,
    Path((id, request_id)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let mut data = shared.data.lock();
    let client_key = query.get("clientKey").cloned().unwrap_or_default();
    let lock_secs = number(&query, "lockSecs").unwrap_or(60) as u64;
    let Some(request) = data.queues.get_mut(&id).and_then(|q| q.requests.get_mut(&request_id)) else {
        return not_found();
    };
    if request.locked_by_other(&client_key) {
        return error(StatusCode::FORBIDDEN, "request-locked");
    }
    let until = std::time::Instant::now() + std::time::Duration::from_secs(lock_secs);
    request.lock = Some((client_key, until));
    envelope(StatusCode::OK, json!({ "lockExpiresAt": NOW }))
}

async fn delete_lock(
    State(shared): State<Shared>,
    Path((id, request_id)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let mut data = shared.data.lock();
    let client_key = query.get("clientKey").cloned().unwrap_or_default();
    let Some(request) = data.queues.get_mut(&id).and_then(|q| q.requests.get_mut(&request_id)) else {
        return not_found();
    };
    if request.locked_by_other(&client_key) {
        return error(StatusCode::FORBIDDEN, "request-locked");
    }
    request.lock = None;
    StatusCode::NO_CONTENT.into_response()
}
