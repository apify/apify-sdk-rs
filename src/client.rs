//! The Apify API client of the SDK: `apify-client` on a reqwest transport that counts
//! rate-limited calls as the JS client does (`ApifyClient.stats.rateLimitErrors`).

use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use apify_client::http_client::{HttpBackend, HttpMethod, HttpRequest, HttpResponse};
use apify_client::{ApifyClient, ApifyClientError, ApifyClientResult};
use async_trait::async_trait;
use lru::LruCache;
use parking_lot::Mutex;
use serde::Deserialize;

use crate::configuration::Configuration;

/// Which attempt of a call reads as rate-limited: the storage load signal of Crawlee for JS reads
/// `rateLimitErrors[2]` (`RATE_LIMIT_ERROR_RETRY_COUNT`), the calls still limited on their third.
const RATE_LIMIT_ERROR_ATTEMPT: usize = 3;
/// How many calls being retried are tracked at once.
const MAX_TRACKED_CALLS: usize = 10_000;

/// Rate-limited API calls by attempt, as `Statistics.rateLimitErrors` of the JS client: a `429`
/// on the n-th attempt of a call counts at index n - 1, and so does a batch add whose n-th try
/// left requests unprocessed. Clones share the counts.
#[derive(Clone, Debug, Default)]
pub struct RateLimitCounter(Arc<Mutex<Vec<u64>>>);

impl RateLimitCounter {
    /// The calls still rate-limited on their third attempt (`rateLimitErrors[2]`), the count the
    /// storage load signal of Crawlee reads.
    pub fn get(&self) -> u64 {
        self.by_attempt().get(RATE_LIMIT_ERROR_ATTEMPT - 1).copied().unwrap_or(0)
    }

    /// The counts by attempt, the first attempts at index 0.
    pub fn by_attempt(&self) -> Vec<u64> {
        self.0.lock().clone()
    }

    /// Counts a rate limit on `attempt` (from 1).
    fn add(&self, attempt: usize) {
        let mut counts = self.0.lock();
        if counts.len() < attempt {
            counts.resize(attempt, 0);
        }
        counts[attempt - 1] += 1;
    }
}

/// Which attempt a call is on. The client retries a call by sending it again unchanged, so the
/// sends of a call are told apart from other calls by their method, URL and body. Identical
/// calls in flight at the same time count as one.
#[derive(Debug)]
struct Attempts {
    hasher: RandomState,
    /// Calls whose last attempt is to be retried: attempts made so far.
    retried: Mutex<LruCache<u64, usize>>,
    /// Batch adds that left requests unprocessed, by the unique keys left: tries made so far.
    unprocessed: Mutex<LruCache<u64, usize>>,
}

impl Default for Attempts {
    fn default() -> Self {
        let capacity = NonZeroUsize::new(MAX_TRACKED_CALLS).expect("non-zero");
        Attempts {
            hasher: RandomState::new(),
            retried: Mutex::new(LruCache::new(capacity)),
            unprocessed: Mutex::new(LruCache::new(capacity)),
        }
    }
}

impl Attempts {
    fn call_key(&self, request: &HttpRequest) -> u64 {
        self.hasher.hash_one((request.method.as_str(), &request.url, &request.body))
    }

    /// The set of unique keys, whatever their order.
    fn keys_key<'a>(&self, unique_keys: impl Iterator<Item = &'a str>) -> u64 {
        let mut keys: Vec<&str> = unique_keys.collect();
        keys.sort_unstable();
        self.hasher.hash_one(keys)
    }
}

/// The part of a batch add response that tells what was left unprocessed.
#[derive(Deserialize)]
struct BatchAddResponse {
    data: BatchAddResult,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BatchAddResult {
    #[serde(default)]
    processed_requests: Vec<UniqueKey>,
    #[serde(default)]
    unprocessed_requests: Vec<UniqueKey>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UniqueKey {
    unique_key: Option<String>,
}

/// An [`HttpBackend`] on reqwest that counts rate-limited calls (see [`RateLimitCounter`]).
#[derive(Clone, Debug)]
pub struct ReqwestBackend {
    client: reqwest::Client,
    rate_limits: RateLimitCounter,
    attempts: Arc<Attempts>,
}

impl ReqwestBackend {
    /// A backend on the HTTP client the SDK's API clients share, so that they share its
    /// connections (and the root certificates are loaded once).
    pub fn new(rate_limits: RateLimitCounter) -> Self {
        static SHARED: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
        ReqwestBackend::with_client(SHARED.get_or_init(reqwest::Client::new).clone(), rate_limits)
    }

    pub fn with_client(client: reqwest::Client, rate_limits: RateLimitCounter) -> Self {
        ReqwestBackend { client, rate_limits, attempts: Arc::default() }
    }

    /// The client retries a batch add with the requests left unprocessed, and counts each try
    /// that leaves some as rate-limited (`addRequestBatchWithRetries` in JS).
    fn count_unprocessed(&self, body: &[u8]) {
        let Ok(BatchAddResponse { data }) = serde_json::from_slice(body) else { return };
        // A try sends the requests the previous one left unprocessed.
        let sent = self
            .attempts
            .keys_key(unique_keys(&data.processed_requests).chain(unique_keys(&data.unprocessed_requests)));
        let mut tries = self.attempts.unprocessed.lock();
        let attempt = tries.pop(&sent).unwrap_or(0) + 1;
        if !data.unprocessed_requests.is_empty() {
            self.rate_limits.add(attempt);
            tries.put(self.attempts.keys_key(unique_keys(&data.unprocessed_requests)), attempt);
        }
    }
}

fn unique_keys(items: &[UniqueKey]) -> impl Iterator<Item = &str> {
    items.iter().filter_map(|item| item.unique_key.as_deref())
}

fn transport_error(err: reqwest::Error) -> ApifyClientError {
    if err.is_timeout() {
        return ApifyClientError::Timeout;
    }
    let mut message = err.to_string();
    let mut source = std::error::Error::source(&err);
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    ApifyClientError::Http(message)
}

#[async_trait]
impl HttpBackend for ReqwestBackend {
    async fn send(&self, request: HttpRequest) -> ApifyClientResult<HttpResponse> {
        let method = match request.method {
            HttpMethod::Get => reqwest::Method::GET,
            HttpMethod::Post => reqwest::Method::POST,
            HttpMethod::Put => reqwest::Method::PUT,
            HttpMethod::Delete => reqwest::Method::DELETE,
            HttpMethod::Head => reqwest::Method::HEAD,
        };
        let call = self.attempts.call_key(&request);
        let attempt = {
            let mut retried = self.attempts.retried.lock();
            let attempt = retried.pop(&call).unwrap_or(0) + 1;
            // Until the outcome is known, the call counts as one to retry: a transport error is.
            retried.put(call, attempt);
            attempt
        };
        let is_batch_add = request.method == HttpMethod::Post
            && request.url.split('?').next().is_some_and(|path| path.ends_with("/requests/batch"));

        let mut builder = self.client.request(method, &request.url).timeout(request.timeout);
        for (name, value) in &request.headers {
            builder = builder.header(name, value);
        }
        if let Some(body) = request.body {
            builder = builder.body(body);
        }

        let started = Instant::now();
        let response = match builder.send().await {
            Ok(response) => response,
            Err(err) => {
                log_call(&request.method, &request.url, None, started, attempt);
                return Err(transport_error(err));
            }
        };
        let status = response.status().as_u16();
        if status == 429 {
            self.rate_limits.add(attempt);
        }
        // The statuses the client retries.
        if status != 429 && status < 500 {
            self.attempts.retried.lock().pop(&call);
        }
        let headers: HashMap<String, String> = response
            .headers()
            .iter()
            .filter_map(|(name, value)| Some((name.as_str().to_owned(), value.to_str().ok()?.to_owned())))
            .collect();
        let body = response.bytes().await.map_err(transport_error);
        log_call(&request.method, &request.url, Some(status), started, attempt);
        let body = body?.to_vec();
        if is_batch_add && status < 300 {
            self.count_unprocessed(&body);
        }
        Ok(HttpResponse { status, headers, body })
    }
}

/// Logs an API call at debug level: its method, path, status (none for a transport error),
/// duration and attempt.
fn log_call(method: &HttpMethod, url: &str, status: Option<u16>, started: Instant, attempt: usize) {
    if !tracing::enabled!(tracing::Level::DEBUG) {
        return;
    }
    let status = status.map_or_else(|| "transport error".to_owned(), |status| status.to_string());
    tracing::debug!(
        "API {} {} {status} in {} ms (attempt {attempt})",
        method.as_str(),
        logged_path(url),
        started.elapsed().as_millis()
    );
}

/// The path of `url`, without the scheme, host and query (which may hold secrets).
fn logged_path(url: &str) -> &str {
    let url = url.split(['?', '#']).next().unwrap_or_default();
    match url.find("://") {
        Some(scheme) => url[scheme + 3..].find('/').map_or("/", |host| &url[scheme + 3 + host..]),
        None => url,
    }
}

/// The `User-Agent` suffix of the SDK's requests, as in the JS SDK.
pub fn user_agent_suffix() -> String {
    format!("SDK/{}; Crawlee/{}", env!("CARGO_PKG_VERSION"), crawlee::VERSION)
}

/// A client for the API `configuration` points to, authenticated with `token` or else with
/// [`Configuration::token`]. Its rate-limited responses are counted in `rate_limits`.
pub fn new_client(configuration: &Configuration, token: Option<&str>, rate_limits: RateLimitCounter) -> ApifyClient {
    let started = Instant::now();
    let backend = Arc::new(ReqwestBackend::new(rate_limits));
    tracing::debug!("Created an API client for {} in {} ms", configuration.api_base_url, started.elapsed().as_millis());
    let mut builder = ApifyClient::builder()
        .base_url(configuration.api_base_url.as_str())
        .public_base_url(configuration.api_public_base_url.as_str())
        .user_agent_suffix(user_agent_suffix())
        .http_backend(backend);
    if let Some(token) = token.or(configuration.token.as_deref()) {
        builder = builder.token(token);
    }
    builder.build()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use apify_client::clients::request_queue::BatchAddRequestsOptions;
    use apify_client::models::RequestQueueRequest;
    use axum::Router;
    use axum::body::Bytes;
    use axum::extract::Path;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::{get, post};
    use serde_json::{Value, json};

    use super::*;

    #[tokio::test]
    async fn requests_are_authenticated_and_rate_limits_retried() {
        let hits = Arc::new(AtomicU64::new(0));
        let seen = hits.clone();
        let app = Router::new().route(
            "/v2/datasets/{id}",
            get(move |headers: HeaderMap| {
                let seen = seen.clone();
                async move {
                    assert_eq!(headers["authorization"], "Bearer secret");
                    let user_agent = headers["user-agent"].to_str().unwrap().to_owned();
                    assert!(user_agent.ends_with(&user_agent_suffix()), "{user_agent}");
                    if seen.fetch_add(1, Ordering::SeqCst) < 2 {
                        return (StatusCode::TOO_MANY_REQUESTS, String::new());
                    }
                    let dataset = serde_json::json!({ "data": {
                        "id": "abc", "name": null, "userId": "u", "createdAt": "2026-01-01T00:00:00.000Z",
                        "modifiedAt": "2026-01-01T00:00:00.000Z", "accessedAt": "2026-01-01T00:00:00.000Z",
                        "itemCount": 3, "cleanItemCount": 3
                    }});
                    (StatusCode::OK, dataset.to_string())
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let configuration = Configuration { api_base_url: base, ..Configuration::default() };
        let rate_limits = RateLimitCounter::default();
        let client = new_client(&configuration, Some("secret"), rate_limits.clone());
        let dataset = client.dataset("abc").get().await.unwrap().unwrap();
        assert_eq!(dataset.item_count, Some(3));
        assert_eq!(hits.load(Ordering::SeqCst), 3, "both rate-limited attempts were retried");
        assert_eq!(rate_limits.get(), 0, "a call that got through on its third attempt is not counted");
    }

    #[test]
    fn logged_paths_have_no_query() {
        assert_eq!(
            logged_path("https://api.apify.com/v2/key-value-stores/s/records/INPUT?token=secret"),
            "/v2/key-value-stores/s/records/INPUT"
        );
        assert_eq!(logged_path("http://localhost:8080/v2/actor-runs/r#x"), "/v2/actor-runs/r");
        assert_eq!(logged_path("https://api.apify.com"), "/");
    }

    /// A client of the API at `base` that retries without waiting.
    fn fast_client(base: &str, rate_limits: RateLimitCounter) -> ApifyClient {
        ApifyClient::builder()
            .base_url(base)
            .token("secret")
            .min_delay_between_retries(std::time::Duration::from_millis(1))
            .http_backend(Arc::new(ReqwestBackend::new(rate_limits)))
            .build()
    }

    async fn serve(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        base
    }

    #[tokio::test]
    async fn only_calls_rate_limited_on_their_third_attempt_count() {
        // Each record is rate-limited as many times as its key says.
        let hits: Arc<parking_lot::Mutex<HashMap<String, u32>>> = Arc::default();
        let app = Router::new().route(
            "/v2/key-value-stores/{id}/records/{key}",
            get(move |Path((_, key)): Path<(String, String)>| {
                let hits = hits.clone();
                async move {
                    let limited: u32 = key.parse().unwrap();
                    let mut hits = hits.lock();
                    let seen = hits.entry(key).or_default();
                    *seen += 1;
                    if *seen <= limited {
                        return (StatusCode::TOO_MANY_REQUESTS, String::new());
                    }
                    (StatusCode::OK, "{}".to_owned())
                }
            }),
        );
        let base = serve(app).await;
        let rate_limits = RateLimitCounter::default();
        let store = fast_client(&base, rate_limits.clone()).key_value_store("s");
        for limited in ["0", "1", "2"] {
            store.get_record(limited).await.unwrap();
        }
        assert_eq!(rate_limits.get(), 0);
        store.get_record("3").await.unwrap();
        assert_eq!(rate_limits.get(), 1, "rate-limited on its third attempt");
        store.get_record("5").await.unwrap();
        assert_eq!(rate_limits.get(), 2, "a call counts once, however often it is retried after");
    }

    #[tokio::test]
    async fn batches_still_unprocessed_on_their_third_try_count() {
        // Each try processes the first request and leaves the others unprocessed.
        let app = Router::new().route(
            "/v2/request-queues/{id}/requests/batch",
            post(|body: Bytes| async move {
                let requests: Vec<Value> = serde_json::from_slice(&body).unwrap();
                let processed: Vec<Value> = requests[..1]
                    .iter()
                    .map(|r| json!({ "uniqueKey": r["uniqueKey"], "requestId": r["uniqueKey"], "wasAlreadyPresent": false, "wasAlreadyHandled": false }))
                    .collect();
                let unprocessed: Vec<Value> = requests[1..]
                    .iter()
                    .map(|r| json!({ "uniqueKey": r["uniqueKey"], "url": r["url"], "method": "GET" }))
                    .collect();
                let body = json!({ "data": { "processedRequests": processed, "unprocessedRequests": unprocessed } });
                (StatusCode::CREATED, body.to_string())
            }),
        );
        let base = serve(app).await;
        let rate_limits = RateLimitCounter::default();
        let queue = fast_client(&base, rate_limits.clone()).request_queue("q");
        let options = BatchAddRequestsOptions {
            min_delay_between_unprocessed_requests_retries: Some(std::time::Duration::from_millis(1)),
            ..BatchAddRequestsOptions::default()
        };
        let requests = |count: usize| -> Vec<RequestQueueRequest> {
            (0..count)
                .map(|n| {
                    let url = format!("https://a.test/{count}/{n}");
                    serde_json::from_value(json!({ "url": url, "uniqueKey": url, "method": "GET" })).unwrap()
                })
                .collect()
        };
        // All processed on the third try: only the first two left requests unprocessed.
        queue.batch_add_requests(&requests(3), options.clone()).await.unwrap();
        assert_eq!(rate_limits.get(), 0);
        let result = queue.batch_add_requests(&requests(5), options).await.unwrap();
        assert_eq!(result.unprocessed_requests.len(), 1, "given up after the last retry");
        assert_eq!(rate_limits.get(), 1, "the third try left requests unprocessed");
    }
}
