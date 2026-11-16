//! Pay-per-event charges made with idempotency keys, against a fake Apify API.

mod common;

use std::sync::Arc;
use std::time::Duration;

use apify::Configuration;
use apify::charging::{ChargeOptions, ChargingError, ChargingManager};
use axum::http::StatusCode;
use serde_json::{Value, json};

use common::{FakeApi, TOKEN};

const CHARGE: &str = "POST /v2/actor-runs/run-1/charge";

async fn manager(api: &FakeApi) -> ChargingManager {
    let pricing = json!({ "pricingModel": "PAY_PER_EVENT", "pricingPerEvent": { "actorChargeEvents": {
        "page": { "eventTitle": "Page", "eventPriceUsd": 1.0 },
    }}});
    let configuration = Configuration {
        is_at_home: true,
        token: Some(TOKEN.to_owned()),
        api_base_url: api.url.clone(),
        api_public_base_url: api.url.clone(),
        actor_run_id: Some("run-1".to_owned()),
        max_total_charge_usd: 10.0,
        actor_pricing_info: Some(pricing.to_string()),
        charged_event_counts: Some("{}".to_owned()),
        ..Configuration::default()
    };
    let client = apify::new_client(&configuration, None, Default::default());
    let manager = ChargingManager::new(Arc::new(configuration), client);
    manager.init(&crawlee::core::MemoryStorageBackend::new()).await.unwrap();
    manager
}

fn keyed(count: u64) -> ChargeOptions {
    ChargeOptions { idempotency_key: Some("key".to_owned()), ..ChargeOptions::new("page", count) }
}

#[tokio::test]
async fn a_repeat_under_a_key_in_flight_reports_what_the_charge_did() {
    let api = FakeApi::start().await;
    let manager = manager(&api).await;
    api.delay(CHARGE, Duration::from_millis(200));
    api.fail_next(CHARGE, 1, StatusCode::BAD_REQUEST);

    let first = manager.charge(keyed(2));
    tokio::pin!(first);
    // Polled once: the charge is counted and its call sent, but not answered.
    tokio::select! {
        biased;
        _ = &mut first => panic!("the charge was answered at once"),
        () = std::future::ready(()) => {}
    }
    let (first, repeat) = tokio::join!(first, manager.charge(keyed(2)));

    assert!(first.is_err(), "the platform refused the first charge");
    // The repeat must not report the failed charge as made: it is made again under the key.
    assert_eq!(repeat.unwrap().charged_count, 2);
    assert_eq!(api.count_calls(CHARGE), 2, "the repeat was not sent");
    assert_eq!(manager.charged_event_count("page"), 2, "what was charged in the end");
}

#[tokio::test]
async fn concurrent_charges_under_one_key_make_one_call() {
    let api = FakeApi::start().await;
    let manager = manager(&api).await;

    let results = futures_util::future::join_all((0..5).map(|_| manager.charge(keyed(3)))).await;

    for result in results {
        assert_eq!(result.unwrap().charged_count, 3);
    }
    assert_eq!(api.count_calls(CHARGE), 1);
    assert_eq!(manager.charged_event_count("page"), 3);
}

#[tokio::test]
async fn the_idempotency_key_is_sent_with_the_charge() {
    let api = FakeApi::start().await;
    let manager = manager(&api).await;

    manager.charge(keyed(1)).await.unwrap();

    assert_eq!(api.charge_idempotency_keys(), [Some("key".to_owned())]);
}

/// A multi-tenant Server Actor run whose Actor has the pricing history `pricing_infos`.
async fn multi_tenant_manager(api: &FakeApi, pricing_infos: Value, charged_event_counts: Value) -> ChargingManager {
    api.put_run(json!({
        "id": "run-1",
        "standby": { "deployment": "MULTI_TENANT_PAYING_USERS" },
        "options": {},
        "chargedEventCounts": charged_event_counts,
    }));
    api.put_actor(json!({ "id": "actor-1", "pricingInfos": pricing_infos }));
    let configuration = Configuration {
        is_at_home: true,
        token: Some(TOKEN.to_owned()),
        api_base_url: api.url.clone(),
        api_public_base_url: api.url.clone(),
        actor_run_id: Some("run-1".to_owned()),
        actor_id: Some("actor-1".to_owned()),
        meta_origin: Some("STANDBY".to_owned()),
        ..Configuration::default()
    };
    let client = apify::new_client(&configuration, None, Default::default());
    let manager = ChargingManager::new(Arc::new(configuration), client);
    manager.init(&crawlee::core::MemoryStorageBackend::new()).await.unwrap();
    manager
}

fn ppe(price: f64, started_at: &str) -> Value {
    json!({
        "pricingModel": "PAY_PER_EVENT",
        "startedAt": started_at,
        "pricingPerEvent": { "actorChargeEvents": { "test-event": { "eventTitle": "Test Event", "eventPriceUsd": price } } },
    })
}

fn for_request(event: &str, request_id: Option<&str>) -> ChargeOptions {
    ChargeOptions { request_id: request_id.map(str::to_owned), ..ChargeOptions::new(event, 1) }
}

/// The bodies of the charge calls, in order.
fn charge_bodies(api: &FakeApi) -> Vec<Value> {
    api.platform_calls().into_iter().filter(|(path, _, _)| path.ends_with("/charge")).map(|(_, _, body)| body).collect()
}

#[tokio::test]
async fn a_multi_tenant_run_charges_by_the_actor_pricing_and_requires_the_request_id() {
    let api = FakeApi::start().await;
    // Shared-pool runs carry no pricing snapshot: the pricing comes from the Actor's history.
    let history = json!([
        ppe(1.0, "2020-01-01T00:00:00.000Z"),
        ppe(2.0, "2021-01-01T00:00:00.000Z"),
        ppe(3.0, "2999-01-01T00:00:00.000Z"),
    ]);
    let manager = multi_tenant_manager(&api, history, json!({})).await;

    assert!(manager.is_multi_tenant());
    // The latest entry with `startedAt <= now`, not the last one (a scheduled change).
    let pricing = manager.pricing_info().unwrap();
    assert!(pricing.is_pay_per_event);
    assert_eq!(pricing.max_total_charge_usd, f64::INFINITY);
    assert_eq!(pricing.per_event_prices.into_iter().collect::<Vec<_>>(), [("test-event".to_owned(), 2.0)]);

    for request_id in [None, Some("")] {
        let err = manager.charge(for_request("test-event", request_id)).await.unwrap_err();
        assert!(matches!(err, ChargingError::MissingRequestId(_)), "{err}");
        assert!(err.to_string().contains("request_id"), "{err}");
    }
    assert_eq!(api.count_calls(CHARGE), 0);

    let err = manager.charge(for_request("apify-default-dataset-item", Some("some-request-id"))).await.unwrap_err();
    assert!(err.to_string().contains("synthetic"), "{err}");

    let result = manager.charge(for_request("test-event", Some("some-request-id"))).await.unwrap();
    assert_eq!(result.charged_count, 1);
    assert_eq!(charge_bodies(&api), [json!({ "eventName": "test-event", "count": 1, "requestId": "some-request-id" })]);
    // Fetched once, at init.
    assert_eq!(api.count_calls("GET /v2/actor-runs/run-1"), 1);
    assert_eq!(api.count_calls("GET /v2/actors/actor-1"), 1);
    assert_eq!(manager.current_run().await.unwrap().unwrap().id, "run-1");
    assert_eq!(api.count_calls("GET /v2/actor-runs/run-1"), 1, "the run is kept");
}

#[tokio::test]
async fn a_multi_tenant_run_leaves_counting_and_deduplication_to_the_platform() {
    let api = FakeApi::start().await;
    // The run-wide counts mix all callers, so they are not this run's counts.
    let manager =
        multi_tenant_manager(&api, json!([ppe(1.0, "2020-01-01T00:00:00.000Z")]), json!({ "test-event": 7 })).await;

    for request_id in ["request-a", "request-b"] {
        let options = ChargeOptions {
            idempotency_key: Some("shared-key".to_owned()),
            ..for_request("test-event", Some(request_id))
        };
        assert_eq!(manager.charge(options).await.unwrap().charged_count, 1);
    }
    assert_eq!(api.count_calls(CHARGE), 2);
    assert_eq!(api.charge_idempotency_keys(), [Some("shared-key".to_owned()), Some("shared-key".to_owned())]);
    assert_eq!(manager.charged_event_count("test-event"), 0);
    // Nothing charged is nothing sent.
    let nothing = ChargeOptions { count: 0, ..for_request("test-event", Some("request-c")) };
    assert_eq!(manager.charge(nothing).await.unwrap().charged_count, 0);
    assert_eq!(api.count_calls(CHARGE), 2);
}

#[tokio::test]
async fn a_multi_tenant_run_without_pay_per_event_pricing_ignores_charges() {
    let api = FakeApi::start().await;
    let manager = multi_tenant_manager(&api, json!([]), json!({})).await;

    assert!(!manager.is_pay_per_event());
    assert_eq!(manager.charge(for_request("test-event", None)).await.unwrap().charged_count, 0);
    assert_eq!(api.count_calls(CHARGE), 0);
}

#[tokio::test]
async fn the_request_id_is_sent_with_the_charge_of_a_run_of_one_tenant() {
    let api = FakeApi::start().await;
    let manager = manager(&api).await;
    assert!(!manager.is_multi_tenant());

    assert_eq!(manager.charge(ChargeOptions::new("page", 1)).await.unwrap().charged_count, 1);
    let options = ChargeOptions { request_id: Some("some-request-id".to_owned()), ..ChargeOptions::new("page", 1) };
    assert_eq!(manager.charge(options).await.unwrap().charged_count, 1);
    assert_eq!(
        charge_bodies(&api),
        [
            json!({ "eventName": "page", "count": 1 }),
            json!({ "eventName": "page", "count": 1, "requestId": "some-request-id" }),
        ]
    );
}
