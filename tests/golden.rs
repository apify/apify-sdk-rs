//! Replays the cases recorded from the JS SDK (`conformance/golden`, see
//! `conformance/oracle/generate-golden.mts`).

mod common;

use std::collections::HashMap;
use std::rc::Rc;

use apify::charging::{ChargeOptions, ChargeResult, ChargingManager};
use apify::{Configuration, ProxyConfiguration, ProxyConfigurationOptions};
use serde_json::{Value, json};

fn golden(name: &str) -> Vec<Value> {
    let path = format!("{}/conformance/golden/{name}.json", env!("CARGO_MANIFEST_DIR"));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn configuration_from(env: &Value) -> Configuration {
    let env: HashMap<String, String> =
        env.as_object().unwrap().iter().map(|(k, v)| (k.clone(), v.as_str().unwrap().to_owned())).collect();
    Configuration::from_sources(|name| env.get(name).cloned(), None)
}

/// The fields the JS `Configuration` has, from the Rust one.
fn configuration_fields(config: &Configuration) -> serde_json::Map<String, Value> {
    let infinite_as_null = |x: f64| if x.is_finite() { json!(x) } else { Value::Null };
    let value = json!({
        "token": config.token, "apiBaseUrl": config.api_base_url, "apiPublicBaseUrl": config.api_public_base_url,
        "defaultDatasetId": config.default_dataset_id, "defaultKeyValueStoreId": config.default_key_value_store_id,
        "defaultRequestQueueId": config.default_request_queue_id, "inputKey": config.input_key,
        "metamorphAfterSleepMillis": config.metamorph_after_sleep.as_millis() as u64,
        "actorEventsWsUrl": config.actor_events_ws_url, "actorId": config.actor_id, "actorRunId": config.actor_run_id,
        "actorTaskId": config.actor_task_id, "containerPort": config.container_port, "containerUrl": config.container_url,
        "standbyPort": config.standby_port, "standbyUrl": config.standby_url, "proxyHostname": config.proxy_hostname,
        "proxyPassword": config.proxy_password, "proxyPort": config.proxy_port, "proxyStatusUrl": config.proxy_status_url,
        "isAtHome": config.is_at_home, "userId": config.user_id, "userIsPaying": config.user_is_paying,
        "actorPermissionLevel": config.actor_permission_level,
        "maxTotalChargeUsd": infinite_as_null(config.max_total_charge_usd), "metaOrigin": config.meta_origin,
        "testPayPerEvent": config.test_pay_per_event, "useChargingLogDataset": config.use_charging_log_dataset,
        "actorPricingInfo": config.actor_pricing_info, "chargedEventCounts": config.charged_event_counts,
        "actorStoragesJson": config.actor_storages_json,
        "inputSecretsPrivateKeyFile": config.input_secrets_private_key_file,
        "inputSecretsPrivateKeyPassphrase": config.input_secrets_private_key_passphrase,
        "memoryMbytes": config.crawlee.memory_mbytes, "availableMemoryRatio": config.crawlee.available_memory_ratio,
        "persistStateIntervalMillis": config.crawlee.persist_state_interval.as_millis() as u64,
        "purgeOnStart": config.crawlee.purge_on_start, "persistStorage": config.crawlee.persist_storage,
        "storageDir": config.crawlee.storage_dir.to_string_lossy(),
    });
    value.as_object().unwrap().clone()
}

/// Numbers compare by value (`1` in JS is `1.0` here).
fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(a), Value::Number(b)) => a.as_f64() == b.as_f64(),
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len() && a.iter().all(|(k, v)| b.get(k).is_some_and(|w| same(v, w)))
        }
        (Value::Array(a), Value::Array(b)) => a.len() == b.len() && a.iter().zip(b).all(|(v, w)| same(v, w)),
        _ => a == b,
    }
}

#[test]
fn configuration() {
    for case in golden("configuration") {
        let actual = configuration_fields(&configuration_from(&case["env"]));
        for (field, expected) in case["expected"].as_object().unwrap() {
            // Allowed difference: the JS SDK reads `APIFY_IS_AT_HOME` by presence for this default.
            if field == "availableMemoryRatio" && case["env"]["APIFY_IS_AT_HOME"] == "false" {
                assert_eq!(actual[field], 0.25);
                continue;
            }
            assert!(same(&actual[field], expected), "{field} with {}: {} != {expected}", case["env"], actual[field]);
        }
    }
}

fn charge_result(result: ChargeResult) -> Value {
    json!({
        "eventChargeLimitReached": result.event_charge_limit_reached,
        "chargedCount": result.charged_count,
        "chargeableWithinLimit": result.chargeable_within_limit,
    })
}

#[tokio::test(flavor = "local")]
async fn charging() {
    let api = common::FakeApi::start().await;
    for case in golden("charging") {
        let mut configuration = configuration_from(&case["env"]);
        configuration.api_base_url = api.url.clone();
        configuration.token = Some(common::TOKEN.to_owned());
        let client = apify::new_client(&configuration, None, Default::default());
        let manager = ChargingManager::new(Rc::new(configuration), client);
        manager.init(&crawlee::core::MemoryStorageBackend::new()).await.unwrap();

        let operations = case["operations"].as_array().unwrap();
        for (operation, expected) in operations.iter().zip(case["expected"].as_array().unwrap()) {
            let event = operation["eventName"].as_str();
            let actual = match operation["op"].as_str().unwrap() {
                "charge" => {
                    let count = operation["count"].as_u64().unwrap();
                    charge_result(manager.charge(ChargeOptions::new(event.unwrap(), count)).await.unwrap())
                }
                "pushDataLimit" => {
                    let items = operation["itemsCount"].as_u64().unwrap() as usize;
                    let is_default = operation["isDefaultDataset"].as_bool().unwrap();
                    json!(manager.push_data_limit(items, event, is_default))
                }
                _ => json!(manager.max_event_charge_count_within_limit(event.unwrap())),
            };
            assert!(same(&actual, expected), "{}: {operation} gave {actual}, expected {expected}", case["name"]);
        }
    }
}

#[tokio::test(flavor = "local")]
async fn proxy_urls() {
    for case in golden("proxy") {
        let configuration = configuration_from(&json!({ "APIFY_PROXY_PASSWORD": "env-password" }));
        let client = apify::new_client(&configuration, None, Default::default());
        let mut options: ProxyConfigurationOptions = serde_json::from_value(case["options"].clone()).unwrap();
        options.check_access = Some(false);
        let proxy = ProxyConfiguration::new(&configuration, &client, options).await.unwrap().unwrap();
        let url = proxy.url_for_session(case["sessionId"].as_str().unwrap()).unwrap();
        assert_eq!(url.as_str().trim_end_matches('/'), case["expected"], "{}", case["options"]);
    }
}

#[test]
fn pseudo_urls() {
    for case in golden("pseudo_urls") {
        let regex = apify::url_filters::purl_to_regex(case["purl"].as_str().unwrap()).unwrap();
        for pair in case["matches"].as_array().unwrap() {
            let (url, expected) = (pair[0].as_str().unwrap(), pair[1].as_bool().unwrap());
            assert_eq!(regex.is_match(url), expected, "{} on {url}", case["purl"]);
        }
    }
}
