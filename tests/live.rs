//! The platform storages against the real Apify API. Skipped unless `APIFY_TOKEN` is set; every
//! test creates named storages of its own and deletes them.
//!
//! ```sh
//! APIFY_TOKEN=... cargo test --test live -- --nocapture
//! ```

use std::rc::Rc;

use apify::{ApifyStorageBackend, Configuration, RequestQueueAccess};
use crawlee::{Request, RequestManager, Services, StorageIdentifier};
use serde_json::{Value, json};

/// The configuration of a local run with the token of the environment, or `None` to skip.
fn configuration() -> Option<Rc<Configuration>> {
    let configuration = Configuration { is_at_home: false, ..Configuration::from_env() };
    if configuration.token.is_none() {
        eprintln!("APIFY_TOKEN is not set; skipping the live test");
        return None;
    }
    Some(Rc::new(configuration))
}

fn unique_name(kind: &str) -> String {
    let millis = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
    format!("apify-sdk-rs-test-{kind}-{millis}-{}", std::process::id())
}

fn services(configuration: &Rc<Configuration>, access: RequestQueueAccess) -> Services {
    Services::new(Rc::new(ApifyStorageBackend::new(configuration.clone()).request_queue_access(access)))
}

#[tokio::test(flavor = "local")]
async fn dataset_and_key_value_store() {
    let Some(configuration) = configuration() else { return };
    let services = services(&configuration, RequestQueueAccess::Single);

    let dataset = services.open_dataset(&StorageIdentifier::name(unique_name("ds"))).await.unwrap();
    let items: Vec<Value> = (0..250).map(|n| json!({ "n": n, "text": "x".repeat(100) })).collect();
    dataset.push_data(&items).await.unwrap();
    // The item count lags behind on the platform; the items do not.
    let stored: Vec<Value> = dataset.get_all().await.unwrap();
    assert_eq!(stored, items);
    dataset.drop_storage().await.unwrap();

    let store = services.open_key_value_store(&StorageIdentifier::name(unique_name("kvs"))).await.unwrap();
    store.set_value("OUTPUT", &json!({ "ok": true })).await.unwrap();
    store.set_text("notes", "hello").await.unwrap();
    assert_eq!(store.get_value::<Value>("OUTPUT").await.unwrap(), Some(json!({ "ok": true })));
    assert_eq!(store.get_text("notes").await.unwrap().as_deref(), Some("hello"));
    assert!(store.record_exists("notes").await.unwrap());
    let url = store.get_public_url("OUTPUT").await.unwrap().unwrap();
    assert!(url.contains("/records/OUTPUT"), "{url}");
    let keys: Vec<String> = store.keys(None).await.unwrap().into_iter().map(|item| item.key).collect();
    assert_eq!(keys, ["OUTPUT", "notes"]);
    store.drop_storage().await.unwrap();
}

#[tokio::test(flavor = "local")]
async fn single_consumer_request_queue() {
    let Some(configuration) = configuration() else { return };
    let services = services(&configuration, RequestQueueAccess::Single);
    let queue = services.open_request_queue(&StorageIdentifier::name(unique_name("rq"))).await.unwrap();

    let requests: Vec<Request> = (0..30).map(|n| Request::new(format!("https://example.com/{n}"))).collect();
    queue.add_requests(requests, false).await.unwrap();
    queue.add_requests(vec![Request::new("https://example.com/first")], true).await.unwrap();

    let mut handled = Vec::new();
    while !queue.is_finished().await.unwrap() {
        let Some(mut request) = queue.fetch_next_request().await.unwrap() else { continue };
        handled.push(request.url.clone());
        queue.mark_request_as_handled(&mut request).await.unwrap();
    }
    assert_eq!(handled.len(), 31);
    assert_eq!(handled[0], "https://example.com/first", "forefront first");
    queue.drop_storage().await.unwrap();
}

#[tokio::test(flavor = "local")]
async fn shared_request_queue_with_two_consumers() {
    let Some(configuration) = configuration() else { return };
    let name = unique_name("rq-shared");
    let first = services(&configuration, RequestQueueAccess::Shared);
    // Another client key, as another run would have.
    let other = Rc::new(Configuration { actor_run_id: Some(unique_name("run")), ..(*configuration).clone() });
    let second = services(&other, RequestQueueAccess::Shared);
    let a = first.open_request_queue(&StorageIdentifier::name(name.clone())).await.unwrap();
    let b = second.open_request_queue(&StorageIdentifier::name(name)).await.unwrap();

    let requests: Vec<Request> = (0..40).map(|n| Request::new(format!("https://example.com/{n}"))).collect();
    a.add_requests(requests, false).await.unwrap();

    let consume = |queue: crawlee::RequestQueue| async move {
        let mut handled = Vec::new();
        while !queue.is_finished().await.unwrap() {
            let Some(mut request) = queue.fetch_next_request().await.unwrap() else {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                continue;
            };
            handled.push(request.url.clone());
            queue.mark_request_as_handled(&mut request).await.unwrap();
        }
        handled
    };
    let (from_a, from_b) = tokio::join!(consume(a.clone()), consume(b));
    let mut all: Vec<String> = from_a.iter().chain(&from_b).cloned().collect();
    all.sort();
    all.dedup();
    assert_eq!(all.len(), 40, "each request handled once: {} + {}", from_a.len(), from_b.len());
    a.drop_storage().await.unwrap();
}
