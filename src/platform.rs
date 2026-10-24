//! The platform API of an Actor: running other Actors and tasks, metamorphing, webhooks, the run
//! environment and the persisted state.

use std::sync::Arc;
use std::time::Duration;

use apify_client::models::{ActorRun, Webhook};
use apify_client::{ActorStartOptions, ApifyClient, ApifyClientError, ApifyClientResult};
use chrono::{DateTime, Utc};
use crawlee::core::{SerdeState, StorageResult};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::actor::Actor;

/// The shortest run timeout `RunTimeout::Inherit` gives, as in JS.
const MINIMUM_API_TIMEOUT_SECS: i64 = 1;
/// The key of [`Actor::use_state`] when none is given.
const DEFAULT_STATE_KEY: &str = "APIFY_GLOBAL_STATE";

/// The timeout of a run started by this one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunTimeout {
    Duration(Duration),
    /// What remains of this run's timeout (`timeout: 'inherit'` in JS).
    Inherit,
}

/// Options of [`Actor::call`], [`Actor::start`] and [`Actor::call_task`].
#[derive(Clone, Debug, Default)]
pub struct CallOptions {
    /// Run with another token than the Actor's.
    pub token: Option<String>,
    pub timeout: Option<RunTimeout>,
    /// Build, memory and the other run options of the API.
    pub start: ActorStartOptions,
    /// How long `call` waits for the run to finish; `None` waits until it does.
    pub wait: Option<Duration>,
}

#[derive(Clone, Debug, Default)]
pub struct AbortOptions {
    pub token: Option<String>,
    /// The terminal status message of the aborted run.
    pub status_message: Option<String>,
    /// Let the run save its state before it ends.
    pub gracefully: Option<bool>,
}

#[derive(Clone, Debug, Default)]
pub struct MetamorphOptions {
    /// The build of the target Actor.
    pub build: Option<String>,
    /// How long to wait for the platform to stop the container; default
    /// [`Configuration::metamorph_after_sleep`](crate::Configuration::metamorph_after_sleep).
    pub after_sleep: Option<Duration>,
}

/// An ad-hoc webhook of the current run (see [`Actor::add_webhook`]).
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WebhookOptions {
    pub event_types: Vec<String>,
    pub request_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_template: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers_template: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ignore_ssl_errors: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub do_not_retry: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub should_interpolate_strings: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_apify_integration: Option<bool>,
}

/// The environment of a run, from the variables the platform sets (`Actor.getEnv()` in JS).
/// `ACTOR_*` variables are read before their `APIFY_*` predecessors.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ApifyEnv {
    pub actor_id: Option<String>,
    pub actor_run_id: Option<String>,
    pub actor_task_id: Option<String>,
    pub actor_build_id: Option<String>,
    pub actor_build_number: Option<String>,
    pub actor_build_tags: Option<String>,
    pub actor_events_websocket_url: Option<String>,
    pub actor_max_paid_dataset_items: Option<i64>,
    pub actor_max_total_charge_usd: Option<f64>,
    pub actor_permission_level: Option<String>,
    pub actor_standby_url: Option<String>,
    pub actor_web_server_port: Option<i64>,
    pub actor_web_server_url: Option<String>,
    pub api_base_url: Option<String>,
    pub api_public_base_url: Option<String>,
    pub default_dataset_id: Option<String>,
    pub default_key_value_store_id: Option<String>,
    pub default_request_queue_id: Option<String>,
    pub input_key: Option<String>,
    pub is_at_home: bool,
    pub memory_mbytes: Option<i64>,
    pub meta_origin: Option<String>,
    pub proxy_hostname: Option<String>,
    pub proxy_password: Option<String>,
    pub proxy_port: Option<i64>,
    pub proxy_status_url: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub timeout_at: Option<DateTime<Utc>>,
    pub token: Option<String>,
    pub user_id: Option<String>,
    /// Whether the user who started the run is paying. `false` when not set.
    pub user_is_paying: bool,
}

impl ApifyEnv {
    pub fn from_env() -> Self {
        Self::from_vars(|name| std::env::var(name).ok())
    }

    pub fn from_vars(env: impl Fn(&str) -> Option<String>) -> Self {
        let var = |names: &[&str]| names.iter().find_map(|name| env(name).filter(|value| !value.is_empty()));
        let int = |names: &[&str]| var(names).and_then(|value| value.trim().parse().ok());
        let date = |names: &[&str]| {
            var(names).and_then(|value| DateTime::parse_from_rfc3339(&value).ok()).map(|date| date.with_timezone(&Utc))
        };
        ApifyEnv {
            actor_id: var(&["ACTOR_ID", "APIFY_ACTOR_ID"]),
            actor_run_id: var(&["ACTOR_RUN_ID", "APIFY_ACTOR_RUN_ID"]),
            actor_task_id: var(&["ACTOR_TASK_ID", "APIFY_ACTOR_TASK_ID"]),
            actor_build_id: var(&["ACTOR_BUILD_ID", "APIFY_ACTOR_BUILD_ID"]),
            actor_build_number: var(&["ACTOR_BUILD_NUMBER", "APIFY_ACTOR_BUILD_NUMBER"]),
            actor_build_tags: var(&["ACTOR_BUILD_TAGS"]),
            actor_events_websocket_url: var(&["ACTOR_EVENTS_WEBSOCKET_URL", "APIFY_ACTOR_EVENTS_WS_URL"]),
            actor_max_paid_dataset_items: int(&["ACTOR_MAX_PAID_DATASET_ITEMS"]),
            actor_max_total_charge_usd: var(&["ACTOR_MAX_TOTAL_CHARGE_USD"]).and_then(|v| v.trim().parse().ok()),
            actor_permission_level: var(&["ACTOR_PERMISSION_LEVEL"]),
            actor_standby_url: var(&["ACTOR_STANDBY_URL"]),
            actor_web_server_port: int(&["ACTOR_WEB_SERVER_PORT", "APIFY_CONTAINER_PORT"]),
            actor_web_server_url: var(&["ACTOR_WEB_SERVER_URL", "APIFY_CONTAINER_URL"]),
            api_base_url: var(&["APIFY_API_BASE_URL"]),
            api_public_base_url: var(&["APIFY_API_PUBLIC_BASE_URL"]),
            default_dataset_id: var(&["ACTOR_DEFAULT_DATASET_ID", "APIFY_DEFAULT_DATASET_ID"]),
            default_key_value_store_id: var(&["ACTOR_DEFAULT_KEY_VALUE_STORE_ID", "APIFY_DEFAULT_KEY_VALUE_STORE_ID"]),
            default_request_queue_id: var(&["ACTOR_DEFAULT_REQUEST_QUEUE_ID", "APIFY_DEFAULT_REQUEST_QUEUE_ID"]),
            input_key: var(&["ACTOR_INPUT_KEY", "APIFY_INPUT_KEY"]),
            is_at_home: var(&["APIFY_IS_AT_HOME"])
                .is_some_and(|value| !matches!(value.to_ascii_lowercase().as_str(), "0" | "false")),
            memory_mbytes: int(&["ACTOR_MEMORY_MBYTES", "APIFY_MEMORY_MBYTES"]),
            meta_origin: var(&["APIFY_META_ORIGIN"]),
            proxy_hostname: var(&["APIFY_PROXY_HOSTNAME"]),
            proxy_password: var(&["APIFY_PROXY_PASSWORD"]),
            proxy_port: int(&["APIFY_PROXY_PORT"]),
            proxy_status_url: var(&["APIFY_PROXY_STATUS_URL"]),
            started_at: date(&["ACTOR_STARTED_AT", "APIFY_STARTED_AT"]),
            timeout_at: date(&["ACTOR_TIMEOUT_AT", "APIFY_TIMEOUT_AT"]),
            token: var(&["APIFY_TOKEN"]),
            user_id: var(&["APIFY_USER_ID"]),
            user_is_paying: var(&["APIFY_USER_IS_PAYING"])
                .is_some_and(|value| crate::configuration::parse_bool(&value)),
        }
    }
}

impl Actor {
    /// The environment of the run.
    pub fn get_env() -> ApifyEnv {
        ApifyEnv::from_env()
    }

    fn client_for(token: Option<&str>) -> ApifyClient {
        match token {
            Some(token) => Self::new_client(token),
            None => Self::client().clone(),
        }
    }

    /// What remains of this run's timeout, on the platform.
    fn remaining_time_secs() -> Option<i64> {
        let timeout_at = Self::get_env().timeout_at.filter(|_| Self::is_at_home());
        let Some(timeout_at) = timeout_at else {
            tracing::warn!(
                "Using `inherit` argument is only possible when the Actor is running on the Apify platform and when \
                 the timeout for the Actor run is set."
            );
            return None;
        };
        let remaining_millis = (timeout_at - Utc::now()).num_milliseconds();
        Some(((remaining_millis + 999).div_euclid(1000)).max(MINIMUM_API_TIMEOUT_SECS))
    }

    fn start_options(options: &CallOptions) -> ActorStartOptions {
        let mut start = options.start.clone();
        match options.timeout {
            Some(RunTimeout::Duration(timeout)) => start.timeout_secs = Some(timeout.as_secs() as i64),
            Some(RunTimeout::Inherit) => start.timeout_secs = Self::remaining_time_secs(),
            None => {}
        }
        start
    }

    /// Starts an Actor (`username/actor-name` or its id) and waits for its run to finish.
    pub async fn call<T: Serialize>(
        actor_id: &str,
        input: Option<&T>,
        options: CallOptions,
    ) -> ApifyClientResult<ActorRun> {
        let client = Self::client_for(options.token.as_deref());
        let wait = options.wait.map(|wait| wait.as_secs() as i64);
        client.actor(actor_id).call(input, Self::start_options(&options), wait).await
    }

    /// Starts an Actor without waiting for its run to finish.
    pub async fn start<T: Serialize>(
        actor_id: &str,
        input: Option<&T>,
        options: CallOptions,
    ) -> ApifyClientResult<ActorRun> {
        let client = Self::client_for(options.token.as_deref());
        client.actor(actor_id).start(input, Self::start_options(&options)).await
    }

    /// Runs a task, with `input` overriding the task's input, and waits for the run to finish.
    pub async fn call_task<T: Serialize>(
        task_id: &str,
        input: Option<&T>,
        options: CallOptions,
    ) -> ApifyClientResult<ActorRun> {
        let client = Self::client_for(options.token.as_deref());
        let wait = options.wait.map(|wait| wait.as_secs() as i64);
        client.task(task_id).call(input, Self::start_options(&options), wait).await
    }

    /// Aborts a run. The status message goes to the aborted run.
    pub async fn abort(run_id: &str, options: AbortOptions) -> ApifyClientResult<ActorRun> {
        let client = Self::client_for(options.token.as_deref());
        if let Some(message) = &options.status_message {
            let body = serde_json::json!({ "statusMessage": message, "isStatusMessageTerminal": true });
            client.run(run_id).update(&body).await?;
        }
        client.run(run_id).abort(options.gracefully).await
    }

    /// Replaces this run's Actor with `target_actor_id`, keeping the run (and its storages). On
    /// the platform the process is then stopped; this waits for it. Does nothing off the platform.
    pub async fn metamorph<T: Serialize>(
        target_actor_id: &str,
        input: Option<&T>,
        options: MetamorphOptions,
    ) -> ApifyClientResult<()> {
        if !Self::is_at_home() {
            tracing::warn!("Actor::metamorph() is only supported when running on the Apify platform.");
            return Ok(());
        }
        let run_id = Self::run_id()?;
        let metamorph = apify_client::RunMetamorphOptions { build: options.build, ..Default::default() };
        Self::client().run(run_id).metamorph(target_actor_id, input, metamorph).await?;
        tokio::time::sleep(options.after_sleep.unwrap_or(Self::configuration().metamorph_after_sleep)).await;
        Ok(())
    }

    /// Adds a webhook for events of the current run. Off the platform, it is not created and
    /// `None` is returned.
    pub async fn add_webhook(options: WebhookOptions) -> ApifyClientResult<Option<Webhook>> {
        if !Self::is_at_home() {
            tracing::warn!(
                "Actor::add_webhook() is only supported when running on the Apify platform. The webhook will not be \
                 invoked."
            );
            return Ok(None);
        }
        let run_id = Self::run_id()?;
        let mut webhook = serde_json::to_value(&options)?;
        webhook["isAdHoc"] = serde_json::Value::Bool(true);
        webhook["condition"] = serde_json::json!({ "actorRunId": run_id });
        Ok(Some(Self::client().webhooks().create(&webhook).await?))
    }

    /// A state saved in the default key-value store on every `PersistState` event and restored
    /// when the run restarts (after a migration, for example). `key` defaults to
    /// `APIFY_GLOBAL_STATE`. Calls with the same key share the state.
    pub async fn use_state<T>(
        key: Option<&str>,
        default: impl Fn() -> T + Send + Sync + 'static,
    ) -> StorageResult<Arc<SerdeState<T>>>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        Self::services().auto_saved_value(key.unwrap_or(DEFAULT_STATE_KEY), default).await
    }

    fn run_id() -> ApifyClientResult<String> {
        Self::configuration().actor_run_id.clone().ok_or_else(|| {
            ApifyClientError::InvalidArgument("Environment variable ACTOR_RUN_ID is not set!".to_owned())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_reads_actor_variables_first() {
        let env = ApifyEnv::from_vars(|name| {
            match name {
                "ACTOR_RUN_ID" => Some("actor-run"),
                "APIFY_ACTOR_RUN_ID" => Some("apify-run"),
                "APIFY_DEFAULT_DATASET_ID" => Some("legacy-dataset"),
                "ACTOR_MEMORY_MBYTES" => Some("4096"),
                "ACTOR_TIMEOUT_AT" => Some("2026-01-01T10:00:00.000Z"),
                "APIFY_IS_AT_HOME" => Some("1"),
                "APIFY_USER_IS_PAYING" => Some("true"),
                _ => None,
            }
            .map(str::to_owned)
        });
        assert_eq!(env.actor_run_id.as_deref(), Some("actor-run"));
        assert_eq!(env.default_dataset_id.as_deref(), Some("legacy-dataset"), "the APIFY_ variable is the fallback");
        assert_eq!(env.memory_mbytes, Some(4096));
        assert_eq!(env.timeout_at.unwrap().to_rfc3339(), "2026-01-01T10:00:00+00:00");
        assert!(env.is_at_home);
        assert!(env.user_is_paying);
        assert_eq!(env.token, None);
        assert!(!ApifyEnv::from_vars(|_| None).user_is_paying, "false when not set");
        assert!(!ApifyEnv::from_vars(|name| (name == "APIFY_USER_IS_PAYING").then(|| "0".to_owned())).user_is_paying);
    }
}
