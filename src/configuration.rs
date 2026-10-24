//! [`Configuration`]: the settings of an Actor, from the environment variables the Apify platform
//! and the Apify CLI set, with the precedence and defaults of the JS SDK.
//!
//! Every value is resolved in this order: the first set environment variable of the field
//! (`ACTOR_*` before `APIFY_*` before `CRAWLEE_*`), then `./crawlee.json` (camelCase keys), then
//! the default. Empty variables count as unset. Values set in code go on top: the fields are public.

use std::time::Duration;

use serde_json::{Map, Value};

/// Settings of an Actor run. [`crawlee`](Self::crawlee) holds the crawlee-rs settings, resolved
/// with the SDK's aliases of the `CRAWLEE_*` variables.
#[derive(Clone, Debug, PartialEq)]
pub struct Configuration {
    pub crawlee: crawlee::Configuration,
    /// `APIFY_IS_AT_HOME`: running on the Apify platform. The one source of truth for it in this
    /// SDK (the JS SDK also reads the variable directly in places).
    pub is_at_home: bool,
    /// `APIFY_TOKEN`.
    pub token: Option<String>,
    /// `APIFY_API_BASE_URL`, default `https://api.apify.com/`.
    pub api_base_url: String,
    /// `APIFY_API_PUBLIC_BASE_URL`, default `https://api.apify.com`.
    pub api_public_base_url: String,
    /// `ACTOR_DEFAULT_DATASET_ID`, `APIFY_DEFAULT_DATASET_ID`, default `default`.
    pub default_dataset_id: String,
    /// `ACTOR_DEFAULT_KEY_VALUE_STORE_ID`, `APIFY_DEFAULT_KEY_VALUE_STORE_ID`, default `default`.
    pub default_key_value_store_id: String,
    /// `ACTOR_DEFAULT_REQUEST_QUEUE_ID`, `APIFY_DEFAULT_REQUEST_QUEUE_ID`, default `default`.
    pub default_request_queue_id: String,
    /// `ACTOR_INPUT_KEY`, `APIFY_INPUT_KEY`, `CRAWLEE_INPUT_KEY`, default `INPUT`.
    pub input_key: String,
    /// `APIFY_METAMORPH_AFTER_SLEEP_MILLIS`, default 5 minutes: how long `metamorph` and `reboot`
    /// wait for the platform to stop the container.
    pub metamorph_after_sleep: Duration,
    /// `ACTOR_EVENTS_WEBSOCKET_URL`, `APIFY_ACTOR_EVENTS_WS_URL`.
    pub actor_events_ws_url: Option<String>,
    /// `ACTOR_ID`, `APIFY_ACTOR_ID`.
    pub actor_id: Option<String>,
    /// `ACTOR_RUN_ID`, `APIFY_ACTOR_RUN_ID`.
    pub actor_run_id: Option<String>,
    /// `ACTOR_TASK_ID`, `APIFY_ACTOR_TASK_ID`.
    pub actor_task_id: Option<String>,
    /// `ACTOR_WEB_SERVER_PORT`, `APIFY_CONTAINER_PORT`, default 4321.
    pub container_port: u16,
    /// `ACTOR_WEB_SERVER_URL`, `APIFY_CONTAINER_URL`, default `http://localhost:4321`.
    pub container_url: String,
    /// `ACTOR_STANDBY_PORT`, default 4321. Deprecated in the JS SDK in favor of `container_port`.
    pub standby_port: u16,
    /// `ACTOR_STANDBY_URL`.
    pub standby_url: Option<String>,
    /// `APIFY_PROXY_HOSTNAME`, default `proxy.apify.com`.
    pub proxy_hostname: String,
    /// `APIFY_PROXY_PASSWORD`.
    pub proxy_password: Option<String>,
    /// `APIFY_PROXY_PORT`, default 8000.
    pub proxy_port: u16,
    /// `APIFY_PROXY_STATUS_URL`, default `http://proxy.apify.com`.
    pub proxy_status_url: String,
    /// `APIFY_USER_ID`.
    pub user_id: Option<String>,
    /// `APIFY_USER_IS_PAYING`: whether the user who started the run is paying.
    pub user_is_paying: bool,
    /// `ACTOR_PERMISSION_LEVEL`.
    pub actor_permission_level: Option<String>,
    /// `APIFY_INPUT_SECRETS_PRIVATE_KEY_PASSPHRASE`.
    pub input_secrets_private_key_passphrase: Option<String>,
    /// `APIFY_INPUT_SECRETS_PRIVATE_KEY_FILE`: the base64-encoded PEM of the key (not a path).
    pub input_secrets_private_key_file: Option<String>,
    /// `ACTOR_MAX_TOTAL_CHARGE_USD`. 0 and unset mean no limit (infinity).
    pub max_total_charge_usd: f64,
    /// `APIFY_META_ORIGIN`.
    pub meta_origin: Option<String>,
    /// `ACTOR_TEST_PAY_PER_EVENT`: simulate pay-per-event charging locally.
    pub test_pay_per_event: bool,
    /// `ACTOR_USE_CHARGING_LOG_DATASET`: log simulated charges to a dataset.
    pub use_charging_log_dataset: bool,
    /// `APIFY_ACTOR_PRICING_INFO`: JSON set by the platform.
    pub actor_pricing_info: Option<String>,
    /// `APIFY_CHARGED_ACTOR_EVENT_COUNTS`: JSON set by the platform.
    pub charged_event_counts: Option<String>,
    /// `ACTOR_STORAGES_JSON`: the storages of the Actor's schema, by alias.
    pub actor_storages_json: Option<String>,
}

impl Default for Configuration {
    /// The configuration of an empty environment.
    fn default() -> Self {
        Configuration::from_sources(|_| None, None)
    }
}

/// `'0'` and `'false'` (any case) are false; any other value is true (`coerceBoolean` in JS).
pub(crate) fn parse_bool(value: &str) -> bool {
    !matches!(value.to_ascii_lowercase().as_str(), "0" | "false")
}

/// Aliases of crawlee-rs's variables: the SDK reads these first, in this order.
fn crawlee_aliases(name: &str) -> &'static [&'static str] {
    match name {
        "CRAWLEE_MEMORY_MBYTES" => &["ACTOR_MEMORY_MBYTES", "APIFY_MEMORY_MBYTES"],
        "CRAWLEE_AVAILABLE_MEMORY_RATIO" => &["APIFY_AVAILABLE_MEMORY_RATIO"],
        "CRAWLEE_PERSIST_STATE_INTERVAL_MILLIS" => {
            &["APIFY_PERSIST_STATE_INTERVAL_MILLIS", "APIFY_TEST_PERSIST_INTERVAL_MILLIS"]
        }
        "CRAWLEE_PURGE_ON_START" => &["APIFY_PURGE_ON_START"],
        _ => &[],
    }
}

/// Where values come from: environment variables and the parsed `crawlee.json`.
struct Sources<E> {
    env: E,
    file: Map<String, Value>,
}

impl<E: Fn(&str) -> Option<String>> Sources<E> {
    fn var(&self, names: &[&str]) -> Option<String> {
        names.iter().find_map(|name| (self.env)(name).filter(|value| !value.is_empty()))
    }

    fn file_string(&self, key: &str) -> Option<String> {
        match self.file.get(key)? {
            Value::String(value) => Some(value.clone()),
            Value::Number(value) => Some(value.to_string()),
            Value::Bool(value) => Some(value.to_string()),
            _ => None,
        }
    }

    fn string(&self, names: &[&str], key: &str) -> Option<String> {
        self.var(names).or_else(|| self.file_string(key))
    }

    fn string_or(&self, names: &[&str], key: &str, default: &str) -> String {
        self.string(names, key).unwrap_or_else(|| default.to_owned())
    }

    fn bool_or(&self, names: &[&str], key: &str, default: bool) -> bool {
        self.string(names, key).map_or(default, |value| parse_bool(&value))
    }

    /// Unparsable numbers fall back to the default with a warning (the JS SDK throws).
    fn number<T: std::str::FromStr>(&self, names: &[&str], key: &str) -> Option<T> {
        let value = self.string(names, key)?;
        match value.trim().parse() {
            Ok(number) => Some(number),
            Err(_) => {
                tracing::warn!("Ignoring the invalid value {value:?} of the '{key}' configuration option");
                None
            }
        }
    }
}

impl Configuration {
    /// The configuration of this process: its environment variables and `./crawlee.json`.
    pub fn from_env() -> Self {
        Configuration::from_sources(|name| std::env::var(name).ok(), std::fs::read_to_string("crawlee.json").ok())
    }

    /// The configuration from the variables `env` returns and `file`, the text of a `crawlee.json`.
    pub fn from_sources(env: impl Fn(&str) -> Option<String>, file: Option<String>) -> Self {
        let file_map =
            file.as_deref().and_then(|text| serde_json::from_str::<Map<String, Value>>(text).ok()).unwrap_or_default();
        let sources = Sources { env: &env, file: file_map };

        let aliased_env = |name: &str| {
            let mut names = crawlee_aliases(name).to_vec();
            names.push(name);
            sources.var(&names)
        };
        let mut crawlee = crawlee::Configuration::from_sources(aliased_env, file);

        let is_at_home = sources.bool_or(&["APIFY_IS_AT_HOME"], "isAtHome", false);
        // The whole machine belongs to the Actor on the platform.
        let memory_ratio_set = sources
            .string(&["APIFY_AVAILABLE_MEMORY_RATIO", "CRAWLEE_AVAILABLE_MEMORY_RATIO"], "availableMemoryRatio")
            .is_some();
        if is_at_home && !memory_ratio_set {
            crawlee.available_memory_ratio = 1.0;
        }

        let max_total_charge_usd = match sources.number::<f64>(&["ACTOR_MAX_TOTAL_CHARGE_USD"], "maxTotalChargeUsd") {
            Some(value) if value != 0.0 => value,
            _ => f64::INFINITY,
        };

        Configuration {
            crawlee,
            is_at_home,
            token: sources.string(&["APIFY_TOKEN"], "token"),
            api_base_url: sources.string_or(&["APIFY_API_BASE_URL"], "apiBaseUrl", "https://api.apify.com/"),
            api_public_base_url: sources.string_or(
                &["APIFY_API_PUBLIC_BASE_URL"],
                "apiPublicBaseUrl",
                "https://api.apify.com",
            ),
            default_dataset_id: sources.string_or(
                &["ACTOR_DEFAULT_DATASET_ID", "APIFY_DEFAULT_DATASET_ID"],
                "defaultDatasetId",
                "default",
            ),
            default_key_value_store_id: sources.string_or(
                &["ACTOR_DEFAULT_KEY_VALUE_STORE_ID", "APIFY_DEFAULT_KEY_VALUE_STORE_ID"],
                "defaultKeyValueStoreId",
                "default",
            ),
            default_request_queue_id: sources.string_or(
                &["ACTOR_DEFAULT_REQUEST_QUEUE_ID", "APIFY_DEFAULT_REQUEST_QUEUE_ID"],
                "defaultRequestQueueId",
                "default",
            ),
            input_key: sources.string_or(
                &["ACTOR_INPUT_KEY", "APIFY_INPUT_KEY", "CRAWLEE_INPUT_KEY"],
                "inputKey",
                "INPUT",
            ),
            metamorph_after_sleep: Duration::from_millis(
                sources.number(&["APIFY_METAMORPH_AFTER_SLEEP_MILLIS"], "metamorphAfterSleepMillis").unwrap_or(300_000),
            ),
            actor_events_ws_url: sources
                .string(&["ACTOR_EVENTS_WEBSOCKET_URL", "APIFY_ACTOR_EVENTS_WS_URL"], "actorEventsWsUrl"),
            actor_id: sources.string(&["ACTOR_ID", "APIFY_ACTOR_ID"], "actorId"),
            actor_run_id: sources.string(&["ACTOR_RUN_ID", "APIFY_ACTOR_RUN_ID"], "actorRunId"),
            actor_task_id: sources.string(&["ACTOR_TASK_ID", "APIFY_ACTOR_TASK_ID"], "actorTaskId"),
            container_port: sources
                .number(&["ACTOR_WEB_SERVER_PORT", "APIFY_CONTAINER_PORT"], "containerPort")
                .unwrap_or(4321),
            container_url: sources.string_or(
                &["ACTOR_WEB_SERVER_URL", "APIFY_CONTAINER_URL"],
                "containerUrl",
                "http://localhost:4321",
            ),
            standby_port: sources.number(&["ACTOR_STANDBY_PORT"], "standbyPort").unwrap_or(4321),
            standby_url: sources.string(&["ACTOR_STANDBY_URL"], "standbyUrl"),
            proxy_hostname: sources.string_or(&["APIFY_PROXY_HOSTNAME"], "proxyHostname", "proxy.apify.com"),
            proxy_password: sources.string(&["APIFY_PROXY_PASSWORD"], "proxyPassword"),
            proxy_port: sources.number(&["APIFY_PROXY_PORT"], "proxyPort").unwrap_or(8000),
            proxy_status_url: sources.string_or(
                &["APIFY_PROXY_STATUS_URL"],
                "proxyStatusUrl",
                "http://proxy.apify.com",
            ),
            user_id: sources.string(&["APIFY_USER_ID"], "userId"),
            user_is_paying: sources.bool_or(&["APIFY_USER_IS_PAYING"], "userIsPaying", false),
            actor_permission_level: sources.string(&["ACTOR_PERMISSION_LEVEL"], "actorPermissionLevel"),
            input_secrets_private_key_passphrase: sources
                .string(&["APIFY_INPUT_SECRETS_PRIVATE_KEY_PASSPHRASE"], "inputSecretsPrivateKeyPassphrase"),
            input_secrets_private_key_file: sources
                .string(&["APIFY_INPUT_SECRETS_PRIVATE_KEY_FILE"], "inputSecretsPrivateKeyFile"),
            max_total_charge_usd,
            meta_origin: sources.string(&["APIFY_META_ORIGIN"], "metaOrigin"),
            test_pay_per_event: sources.bool_or(&["ACTOR_TEST_PAY_PER_EVENT"], "testPayPerEvent", false),
            use_charging_log_dataset: sources.bool_or(
                &["ACTOR_USE_CHARGING_LOG_DATASET"],
                "useChargingLogDataset",
                false,
            ),
            actor_pricing_info: sources.string(&["APIFY_ACTOR_PRICING_INFO"], "actorPricingInfo"),
            charged_event_counts: sources.string(&["APIFY_CHARGED_ACTOR_EVENT_COUNTS"], "chargedEventCounts"),
            actor_storages_json: sources.string(&["ACTOR_STORAGES_JSON"], "actorStoragesJson"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use super::*;

    fn configuration(vars: &[(&str, &str)], file: Option<&str>) -> Configuration {
        let vars: HashMap<String, String> = vars.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect();
        Configuration::from_sources(|name| vars.get(name).cloned(), file.map(str::to_owned))
    }

    #[test]
    fn defaults() {
        let config = Configuration::default();
        assert!(!config.is_at_home);
        assert_eq!(config.api_base_url, "https://api.apify.com/");
        assert_eq!(config.api_public_base_url, "https://api.apify.com");
        assert_eq!(config.default_dataset_id, "default");
        assert_eq!(config.input_key, "INPUT");
        assert_eq!(config.metamorph_after_sleep, Duration::from_secs(300));
        assert_eq!((config.proxy_hostname.as_str(), config.proxy_port), ("proxy.apify.com", 8000));
        assert_eq!(config.container_url, "http://localhost:4321");
        assert_eq!(config.max_total_charge_usd, f64::INFINITY);
        assert_eq!(config.crawlee, crawlee::Configuration::default());
    }

    #[test]
    fn actor_variables_win_over_apify_and_crawlee_ones() {
        let config = configuration(
            &[
                ("ACTOR_DEFAULT_DATASET_ID", "actor-ds"),
                ("APIFY_DEFAULT_DATASET_ID", "apify-ds"),
                ("ACTOR_INPUT_KEY", ""),
                ("APIFY_INPUT_KEY", "APIFY_INPUT"),
                ("CRAWLEE_INPUT_KEY", "CRAWLEE_INPUT"),
                ("APIFY_MEMORY_MBYTES", "2048"),
                ("CRAWLEE_MEMORY_MBYTES", "1024"),
                ("APIFY_PURGE_ON_START", "0"),
                ("CRAWLEE_STORAGE_DIR", "/tmp/storage"),
            ],
            None,
        );
        assert_eq!(config.default_dataset_id, "actor-ds");
        assert_eq!(config.input_key, "APIFY_INPUT", "empty variables are skipped");
        assert_eq!(config.crawlee.memory_mbytes, Some(2048));
        assert!(!config.crawlee.purge_on_start);
        assert_eq!(config.crawlee.storage_dir, PathBuf::from("/tmp/storage"));
    }

    #[test]
    fn the_platform_gets_all_memory_unless_configured() {
        let at_home = configuration(&[("APIFY_IS_AT_HOME", "1")], None);
        assert!(at_home.is_at_home);
        assert_eq!(at_home.crawlee.available_memory_ratio, 1.0);

        let configured = configuration(&[("APIFY_IS_AT_HOME", "1"), ("CRAWLEE_AVAILABLE_MEMORY_RATIO", "0.5")], None);
        assert_eq!(configured.crawlee.available_memory_ratio, 0.5);

        let from_file = configuration(&[("APIFY_IS_AT_HOME", "true")], Some(r#"{"availableMemoryRatio": 0.3}"#));
        assert_eq!(from_file.crawlee.available_memory_ratio, 0.3);

        let not_at_home = configuration(&[("APIFY_IS_AT_HOME", "false")], None);
        assert!(!not_at_home.is_at_home, "coerced like a boolean, not by presence");
        assert_eq!(not_at_home.crawlee.available_memory_ratio, 0.25);
    }

    #[test]
    fn crawlee_json_is_below_the_environment() {
        let file = r#"{ "token": "from-file", "proxyPort": 9000, "testPayPerEvent": true, "inputKey": "FILE_INPUT" }"#;
        let config = configuration(&[("APIFY_TOKEN", "from-env")], Some(file));
        assert_eq!(config.token.as_deref(), Some("from-env"));
        assert_eq!(config.proxy_port, 9000);
        assert!(config.test_pay_per_event);
        assert_eq!(config.input_key, "FILE_INPUT");
    }

    #[test]
    fn zero_max_total_charge_means_no_limit() {
        assert_eq!(configuration(&[("ACTOR_MAX_TOTAL_CHARGE_USD", "0")], None).max_total_charge_usd, f64::INFINITY);
        assert_eq!(configuration(&[("ACTOR_MAX_TOTAL_CHARGE_USD", "1.5")], None).max_total_charge_usd, 1.5);
        assert_eq!(configuration(&[("ACTOR_MAX_TOTAL_CHARGE_USD", "lots")], None).max_total_charge_usd, f64::INFINITY);
    }
}
