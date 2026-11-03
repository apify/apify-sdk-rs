//! Pay-per-event charges made with idempotency keys, against a fake Apify API.

mod common;

use std::sync::Arc;
use std::time::Duration;

use apify::Configuration;
use apify::charging::{ChargeOptions, ChargingManager};
use axum::http::StatusCode;
use serde_json::json;

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
