//! The platform storages through the crawlee-rs frontends, against a fake Apify API.

mod common;

use std::sync::Arc;

use apify::{ApifyStorageBackend, Configuration, SmartStorageBackend};
use crawlee::core::storage::backend::StorageKind;
use crawlee::{RequestManager, Services, StorageIdentifier};
use serde_json::{Value, json};

use common::{DEFAULT_DATASET, DEFAULT_QUEUE, DEFAULT_STORE, FakeApi, TOKEN};

fn configuration(api: &FakeApi, is_at_home: bool) -> Arc<Configuration> {
    Arc::new(Configuration {
        is_at_home,
        token: Some(TOKEN.to_owned()),
        api_base_url: api.url.clone(),
        api_public_base_url: api.url.clone(),
        default_dataset_id: DEFAULT_DATASET.to_owned(),
        default_key_value_store_id: DEFAULT_STORE.to_owned(),
        default_request_queue_id: DEFAULT_QUEUE.to_owned(),
        actor_run_id: Some("run-1".to_owned()),
        ..Configuration::default()
    })
}

fn services(api: &FakeApi) -> Services {
    Services::new(Arc::new(ApifyStorageBackend::new(configuration(api, true))))
}

#[tokio::test]
async fn dataset_items_are_pushed_in_order() {
    let api = FakeApi::start().await;
    let dataset = services(&api).open_dataset(&StorageIdentifier::Default).await.unwrap();

    dataset.push_data(&json!({ "n": 0 })).await.unwrap();
    // Large enough to be sent compressed.
    let many: Vec<Value> = (1..=100).map(|n| json!({ "n": n, "text": "lorem ipsum ".repeat(10) })).collect();
    dataset.push_data(&many).await.unwrap();

    let stored = api.dataset_items(DEFAULT_DATASET);
    assert_eq!(stored.len(), 101);
    assert!(stored.iter().enumerate().all(|(n, item)| item["n"] == n));

    let items: Vec<Value> = dataset.get_all().await.unwrap();
    assert_eq!(items, stored);
    let info = dataset.get_info().await.unwrap();
    assert_eq!((info.id.as_str(), info.item_count), (DEFAULT_DATASET, 101));
    assert!(dataset.backend().purge().await.is_err(), "purging is not supported on the platform");
}

#[tokio::test]
async fn key_value_store_records_keep_their_bytes_and_content_type() {
    let api = FakeApi::start().await;
    let store = services(&api).open_key_value_store(&StorageIdentifier::Default).await.unwrap();

    store.set_value("OUTPUT", &json!({ "ok": true })).await.unwrap();
    store.set_text("notes", "hello").await.unwrap();
    let (bytes, content_type) = api.record(DEFAULT_STORE, "OUTPUT").unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), json!({ "ok": true }));
    assert!(content_type.starts_with("application/json"), "{content_type}");

    assert_eq!(store.get_value::<Value>("OUTPUT").await.unwrap(), Some(json!({ "ok": true })));
    assert_eq!(store.get_text("notes").await.unwrap().as_deref(), Some("hello"));
    assert_eq!(store.get_value::<Value>("missing").await.unwrap(), None);
    let keys: Vec<String> = store.keys(None).await.unwrap().into_iter().map(|item| item.key).collect();
    assert_eq!(keys, ["OUTPUT", "notes"]);

    store.delete_value("notes").await.unwrap();
    assert!(!store.record_exists("notes").await.unwrap());
    assert!(store.record_exists("OUTPUT").await.unwrap());
    let url = store.get_public_url("OUTPUT").await.unwrap().unwrap();
    assert_eq!(url, format!("{}/v2/key-value-stores/{DEFAULT_STORE}/records/OUTPUT", api.url));
}

#[tokio::test]
async fn names_and_aliases_resolve_to_platform_storages() {
    let api = FakeApi::start().await;
    let backend = ApifyStorageBackend::new(configuration(&api, true));
    let services = Services::new(Arc::new(backend));

    let named = services.open_dataset(&StorageIdentifier::name("results")).await.unwrap();
    let again = services.open_dataset(&StorageIdentifier::name("results")).await.unwrap();
    assert_eq!(named.get_info().await.unwrap().id, again.get_info().await.unwrap().id);
    assert_eq!(named.get_info().await.unwrap().name.as_deref(), Some("results"));

    let scratch = services.open_dataset(&StorageIdentifier::alias("scratch")).await.unwrap();
    let scratch_id = scratch.get_info().await.unwrap().id;
    assert_eq!(scratch.get_info().await.unwrap().name, None, "aliased storages are unnamed");
    let calls = api.count_calls("POST /v2/datasets");
    let reopened = services.open_dataset(&StorageIdentifier::alias("scratch")).await.unwrap();
    assert_eq!(reopened.get_info().await.unwrap().id, scratch_id);
    assert_eq!(api.count_calls("POST /v2/datasets"), calls, "resolved once per process");

    // The mapping survives a migration: a new process reopens the same storage.
    let (mapping, _) = api.record(DEFAULT_STORE, "__STORAGE_ALIASES_MAPPING").unwrap();
    let mapping: Value = serde_json::from_slice(&mapping).unwrap();
    assert_eq!(mapping.as_object().unwrap().len(), 1);
    let migrated = Services::new(Arc::new(ApifyStorageBackend::new(configuration(&api, true))));
    let after = migrated.open_dataset(&StorageIdentifier::alias("scratch")).await.unwrap();
    assert_eq!(after.get_info().await.unwrap().id, scratch_id);

    // Unless the storage was deleted in the meantime.
    api.delete_dataset(&scratch_id);
    let fresh = Services::new(Arc::new(ApifyStorageBackend::new(configuration(&api, true))));
    let recreated = fresh.open_dataset(&StorageIdentifier::alias("scratch")).await.unwrap();
    assert_ne!(recreated.get_info().await.unwrap().id, scratch_id);

    // `__default__` is the default storage.
    let default = fresh.open_dataset(&StorageIdentifier::alias("__default__")).await.unwrap();
    assert_eq!(default.get_info().await.unwrap().id, DEFAULT_DATASET);
}

#[tokio::test]
async fn a_storage_opened_again_is_the_same_storage() {
    let api = FakeApi::start().await;
    api.set_head_lag(3);
    let services = services(&api);

    // Start URLs added through one handle are fetched through another, before the platform head shows them.
    let start = services.open_request_queue(&StorageIdentifier::Default).await.unwrap();
    start.add_requests(vec!["https://a.test/start".into()], false).await.unwrap();
    let crawler = services.open_request_queue(&StorageIdentifier::id(DEFAULT_QUEUE)).await.unwrap();
    assert!(Arc::ptr_eq(start.backend(), crawler.backend()));
    assert_eq!(crawler.fetch_next_request().await.unwrap().unwrap().url, "https://a.test/start");

    // A named storage is looked up once.
    let named = services.open_dataset(&StorageIdentifier::name("results")).await.unwrap();
    let again = services.open_dataset(&StorageIdentifier::name("results")).await.unwrap();
    assert!(named.same_storage(&again));
    assert_eq!(api.count_calls("POST /v2/datasets"), 1);
    let id = named.get_info().await.unwrap().id;
    let by_id = services.open_dataset(&StorageIdentifier::id(id.clone())).await.unwrap();
    assert!(named.same_storage(&by_id));

    // Until it is dropped.
    named.drop_storage().await.unwrap();
    let recreated = services.open_dataset(&StorageIdentifier::name("results")).await.unwrap();
    assert!(!recreated.same_storage(&again));
    assert_eq!(api.count_calls("POST /v2/datasets"), 2);
    assert_ne!(recreated.get_info().await.unwrap().id, id);
}

#[tokio::test]
async fn aliases_declared_in_the_actor_schema_and_off_the_platform() {
    let api = FakeApi::start().await;
    let declared = Arc::new(Configuration {
        actor_storages_json: Some(format!(r#"{{"datasets": {{"products": "{DEFAULT_DATASET}"}}}}"#)),
        ..(*configuration(&api, true)).clone()
    });
    let services = Services::new(Arc::new(ApifyStorageBackend::new(declared)));
    let products = services.open_dataset(&StorageIdentifier::alias("products")).await.unwrap();
    assert_eq!(products.get_info().await.unwrap().id, DEFAULT_DATASET);

    // Off the platform (a `force_cloud` storage), aliases are not remembered.
    let local = Services::new(Arc::new(ApifyStorageBackend::new(configuration(&api, false))));
    local.open_dataset(&StorageIdentifier::alias("scratch")).await.unwrap();
    assert!(api.record(DEFAULT_STORE, "__STORAGE_ALIASES_MAPPING").is_none());

    let backend = ApifyStorageBackend::new(configuration(&api, false));
    use crawlee::core::StorageBackend as _;
    assert!(backend.storage_exists(DEFAULT_DATASET, StorageKind::Dataset).await.unwrap());
    assert!(!backend.storage_exists("nope", StorageKind::Dataset).await.unwrap());
}

#[tokio::test]
async fn request_queue_serves_its_own_requests_from_the_local_cache() {
    let api = FakeApi::start().await;
    let queue = services(&api).open_request_queue(&StorageIdentifier::Default).await.unwrap();

    let urls = ["https://a.test/1", "https://a.test/2", "https://a.test/3"];
    queue.add_requests(urls.iter().map(|url| (*url).into()).collect(), false).await.unwrap();
    api.clear_calls();

    let mut fetched = Vec::new();
    while let Some(mut request) = queue.fetch_next_request().await.unwrap() {
        fetched.push(request.url.clone());
        queue.mark_request_as_handled(&mut request).await.unwrap();
    }
    assert_eq!(fetched, urls);
    assert!(queue.is_finished().await.unwrap());
    assert_eq!(api.count_calls("GET /v2/request-queues/default-queue/requests/"), 0, "no request was fetched by id");
    assert_eq!(api.count_calls("PUT /v2/request-queues/default-queue/requests/"), 3);

    let stored = api.queue_request(DEFAULT_QUEUE, fetched_id(&queue, urls[0]).await.as_str()).unwrap();
    assert!(stored["handledAt"].is_string());
    let info = queue.get_info().await.unwrap();
    assert_eq!((info.total_request_count, info.handled_request_count, info.pending_request_count), (3, 3, 0));

    // Adding a handled request again costs no write.
    api.clear_calls();
    let again = queue.add_requests(vec!["https://a.test/1".into()], true).await.unwrap();
    assert!(again.processed_requests[0].was_already_handled);
    assert_eq!(api.count_calls("POST"), 0);
}

async fn fetched_id(queue: &crawlee::RequestQueue, unique_key: &str) -> String {
    queue.get_request(unique_key).await.unwrap().unwrap().id.unwrap()
}

#[tokio::test]
async fn a_lagging_head_does_not_finish_the_queue_early() {
    let api = FakeApi::start().await;
    api.set_head_lag(3);
    let queue = services(&api).open_request_queue(&StorageIdentifier::Default).await.unwrap();

    queue.add_requests(vec!["https://a.test/1".into(), "https://a.test/2".into()], false).await.unwrap();
    let mut first = queue.fetch_next_request().await.unwrap().unwrap();
    // The platform head does not show the requests yet, the local estimate does.
    assert!(!queue.is_finished().await.unwrap());
    queue.mark_request_as_handled(&mut first).await.unwrap();

    // A reclaimed request goes back to the local head right away.
    let second = queue.fetch_next_request().await.unwrap().unwrap();
    queue.reclaim_request(&second, false).await.unwrap();
    assert!(!queue.is_finished().await.unwrap());
    let mut second = queue.fetch_next_request().await.unwrap().unwrap();
    assert_eq!(second.url, "https://a.test/2");
    queue.mark_request_as_handled(&mut second).await.unwrap();
    assert!(queue.is_finished().await.unwrap());
}

#[tokio::test]
async fn requests_of_other_producers_come_through_the_head() {
    let api = FakeApi::start().await;
    let queue = services(&api).open_request_queue(&StorageIdentifier::Default).await.unwrap();
    assert!(queue.is_finished().await.unwrap());

    api.add_request(DEFAULT_QUEUE, "https://other.test/1");
    let mut request = queue.fetch_next_request().await.unwrap().unwrap();
    assert_eq!(request.url, "https://other.test/1");
    assert_eq!(api.count_calls("GET /v2/request-queues/default-queue/requests/"), 1, "fetched by id once");
    queue.mark_request_as_handled(&mut request).await.unwrap();
    assert!(queue.is_finished().await.unwrap());

    // Marking a request the queue does not have does not add it.
    let mut unknown = crawlee::Request::new("https://unknown.test");
    assert_eq!(queue.mark_request_as_handled(&mut unknown).await.unwrap(), None);
    assert!(
        api.queue_request(DEFAULT_QUEUE, &crawlee::core::request::unique_key_to_request_id("https://unknown.test"))
            .is_none()
    );
}

#[tokio::test]
async fn a_resurrected_run_deduplicates_from_the_prefetched_queue() {
    let api = FakeApi::start().await;
    {
        let queue = services(&api).open_request_queue(&StorageIdentifier::Default).await.unwrap();
        queue.add_requests(vec!["https://a.test/1".into(), "https://a.test/2".into()], false).await.unwrap();
        let mut first = queue.fetch_next_request().await.unwrap().unwrap();
        queue.mark_request_as_handled(&mut first).await.unwrap();
    }

    // The next run adds the same start URLs.
    api.clear_calls();
    let queue = services(&api).open_request_queue(&StorageIdentifier::Default).await.unwrap();
    let result = queue.add_requests(vec!["https://a.test/1".into(), "https://a.test/2".into()], false).await.unwrap();
    assert!(result.processed_requests.iter().all(|processed| processed.was_already_present));
    assert!(result.processed_requests[0].was_already_handled);
    assert_eq!(api.count_calls("POST"), 0, "deduplicated without writes");

    let mut request = queue.fetch_next_request().await.unwrap().unwrap();
    assert_eq!(request.url, "https://a.test/2");
    queue.mark_request_as_handled(&mut request).await.unwrap();
    assert!(queue.is_finished().await.unwrap());
}

#[tokio::test]
async fn rate_limited_calls_are_retried_and_counted_by_attempt() {
    let api = FakeApi::start().await;
    let backend = Arc::new(ApifyStorageBackend::new(configuration(&api, true)));
    let services = Services::new(backend.clone());
    let dataset = services.open_dataset(&StorageIdentifier::Default).await.unwrap();

    api.rate_limit_next(2);
    dataset.push_data(&json!({ "a": 1 })).await.unwrap();
    assert_eq!(api.dataset_items(DEFAULT_DATASET).len(), 1);
    assert_eq!(backend.rate_limits().by_attempt(), [1, 1]);
    // As in the JS SDK, whose `ApifyStorageBackend` has no `stats`: the storage load signal of
    // autoscaling sees no rate limits.
    use crawlee::core::StorageBackend as _;
    assert_eq!(backend.rate_limit_errors(), 0);
}

#[tokio::test]
async fn the_smart_backend_is_local_off_the_platform() {
    let api = FakeApi::start().await;
    let dir = tempfile::tempdir().unwrap();
    let default_store = dir.path().join("key_value_stores/default");
    std::fs::create_dir_all(&default_store).unwrap();
    std::fs::write(default_store.join("INPUT.json"), r#"{"a": 1}"#).unwrap();

    let mut local = (*configuration(&api, false)).clone();
    local.crawlee.storage_dir = dir.path().to_owned();
    let local = Arc::new(local);
    let smart = SmartStorageBackend::for_configuration(local.clone());
    let services = Services::new(Arc::new(smart));
    services.purge_on_start().await.unwrap();
    let store = services.open_key_value_store(&StorageIdentifier::Default).await.unwrap();
    assert_eq!(
        store.get_value::<Value>("INPUT").await.unwrap(),
        Some(json!({ "a": 1 })),
        "the input survives the purge"
    );
    store.set_value("OUTPUT", &json!(1)).await.unwrap();
    assert!(dir.path().join("key_value_stores/default/OUTPUT").exists());
    assert!(api.record(DEFAULT_STORE, "OUTPUT").is_none());

    let smart = SmartStorageBackend::for_configuration(local.clone());
    assert!(smart.suitable(true).is_ok());
    let no_token = Arc::new(Configuration { token: None, ..(*local).clone() });
    let smart = SmartStorageBackend::for_configuration(no_token);
    assert!(smart.suitable(true).err().unwrap().to_string().contains("APIFY_TOKEN"));

    let at_home = SmartStorageBackend::for_configuration(configuration(&api, true));
    let services = Services::new(Arc::new(at_home));
    let store = services.open_key_value_store(&StorageIdentifier::Default).await.unwrap();
    store.set_value("OUTPUT", &json!(2)).await.unwrap();
    assert!(api.record(DEFAULT_STORE, "OUTPUT").is_some());
}

fn shared_services(api: &FakeApi, run_id: &str) -> Services {
    let configuration =
        Arc::new(Configuration { actor_run_id: Some(run_id.to_owned()), ..(*configuration(api, true)).clone() });
    let backend = ApifyStorageBackend::new(configuration).request_queue_access(apify::RequestQueueAccess::Shared);
    Services::new(Arc::new(backend))
}

#[tokio::test]
async fn shared_queues_hand_each_request_to_one_consumer() {
    let api = FakeApi::start().await;
    let first = shared_services(&api, "run-a").open_request_queue(&StorageIdentifier::Default).await.unwrap();
    let second = shared_services(&api, "run-b").open_request_queue(&StorageIdentifier::Default).await.unwrap();
    let urls: Vec<crawlee::Request> = (0..60).map(|n| crawlee::Request::new(format!("https://a.test/{n}"))).collect();
    first.add_requests(urls, false).await.unwrap();

    let consume = |queue: crawlee::RequestQueue| async move {
        let mut handled = Vec::new();
        while !queue.is_finished().await.unwrap() {
            let Some(mut request) = queue.fetch_next_request().await.unwrap() else {
                tokio::task::yield_now().await;
                continue;
            };
            handled.push(request.url.clone());
            queue.mark_request_as_handled(&mut request).await.unwrap();
        }
        handled
    };
    let (a, b) = tokio::join!(consume(first.clone()), consume(second.clone()));
    assert!(!a.is_empty() && !b.is_empty(), "both consumers got requests: {} + {}", a.len(), b.len());
    let mut all: Vec<String> = a.into_iter().chain(b).collect();
    all.sort();
    all.dedup();
    assert_eq!(all.len(), 60, "every request handled exactly once");
    assert_eq!(first.get_info().await.unwrap().handled_request_count, 60);
}

#[tokio::test]
async fn a_reclaimed_shared_request_is_unlocked_for_others() {
    let api = FakeApi::start().await;
    let first = shared_services(&api, "run-a").open_request_queue(&StorageIdentifier::Default).await.unwrap();
    let second = shared_services(&api, "run-b").open_request_queue(&StorageIdentifier::Default).await.unwrap();
    first.add_requests(vec!["https://a.test/only".into()], false).await.unwrap();

    let request = first.fetch_next_request().await.unwrap().unwrap();
    assert!(second.fetch_next_request().await.unwrap().is_none(), "locked by the first consumer");
    assert!(!second.is_finished().await.unwrap(), "another consumer holds a lock");
    let id = request.id.clone().unwrap();
    assert!(first.backend().extend_request_processing_time(&id, std::time::Duration::from_secs(60)).await.unwrap());

    first.reclaim_request(&request, false).await.unwrap();
    let mut again = second.fetch_next_request().await.unwrap().expect("unlocked by the reclaim");
    assert_eq!(again.url, "https://a.test/only");
    second.mark_request_as_handled(&mut again).await.unwrap();
    assert!(second.is_finished().await.unwrap());
    assert!(first.is_finished().await.unwrap());
}

#[tokio::test]
async fn a_shared_queue_leaves_forefront_ordering_to_the_platform() {
    let api = FakeApi::start().await;
    let queue = shared_services(&api, "run-a").open_request_queue(&StorageIdentifier::Default).await.unwrap();
    let requests: Vec<crawlee::Request> = (0..3).map(|n| format!("https://a.test/{n}").into()).collect();
    queue.add_requests(requests, false).await.unwrap();

    let first = queue.fetch_next_request().await.unwrap().unwrap();
    assert_eq!(first.url, "https://a.test/0");
    queue.add_requests(vec!["https://a.test/front".into()], true).await.unwrap();

    // The locked head is drained before the forefront request is looked at: the SDK does not re-read the head.
    let next = queue.fetch_next_request().await.unwrap().unwrap();
    assert_eq!(next.url, "https://a.test/1");
}
