//! Pay-per-event charging: [`ChargingManager`] keeps track of what the run has charged and what
//! its budget (`ACTOR_MAX_TOTAL_CHARGE_USD`) still allows, a port of the JS SDK's.
//!
//! Items pushed to the default dataset are charged as the synthetic `apify-default-dataset-item`
//! event, by the storage backend at the moment they are stored.
//!
//! A charge is counted against the budget at once, in one short critical section, and then sent
//! to the platform without holding anything: concurrent charges and pushes do not wait for each
//! other's API calls, and cannot together spend more than the budget.
//!
//! A multi-tenant Server Actor run (a standby deployment shared by its callers) charges each
//! caller per request: [`ChargeOptions::request_id`] names the request, and the platform prices,
//! limits and deduplicates the charges. Nothing is counted locally there, and pushed items are
//! not charged.

use std::collections::HashMap;
use std::sync::Arc;

use apify_client::models::{Actor as ActorResource, ActorRun};
use apify_client::{ApifyClient, ApifyClientError, RunChargeOptions};
use async_trait::async_trait;
use crawlee::core::StorageResult;
use crawlee::core::errors::StorageError;
use crawlee::core::storage::backend::{
    DatasetBackend, DatasetInfo, DatasetItem, DatasetListOptions, KeyValueStoreBackend, PaginatedList,
    RequestQueueBackend, StorageBackend, StorageKind,
};
use crawlee::{Dataset, StorageIdentifier};
use indexmap::IndexMap;
use parking_lot::Mutex;
use serde_json::Value;
use tokio::sync::watch;
use tokio::time::{Duration, Instant};

use crate::configuration::Configuration;
use crate::js::number::js_to_fixed;
use crate::storage::DEFAULT_STORAGE_ALIAS;

/// The synthetic event of an item stored in the default dataset.
pub const DEFAULT_DATASET_ITEM_EVENT: &str = "apify-default-dataset-item";
const LOCAL_CHARGING_LOG_DATASET_NAME: &str = "charging_log";
const PAY_PER_EVENT: &str = "PAY_PER_EVENT";

/// A charge to make.
#[derive(Clone, Debug)]
pub struct ChargeOptions {
    pub event_name: String,
    pub count: u64,
    /// Keeps a retried operation from being charged twice: a charge repeated under the same key
    /// within three minutes is not made again (see [`ChargingManager::charge`]).
    ///
    /// The key is scoped to the whole run, not to `request_id`. In a multi-tenant Server Actor
    /// run, a key reused across requests charges only the first caller, so it must be globally
    /// unique (for example, derived from `request_id`).
    pub idempotency_key: Option<String>,
    /// ID of the Server Actor request being charged, from the `X-Actor-Request-ID` header of the
    /// incoming request. Required in multi-tenant Server Actor runs, where it maps the charge to
    /// the caller's account instead of the account that started the run. Sent along elsewhere.
    pub request_id: Option<String>,
}

impl ChargeOptions {
    pub fn new(event_name: impl Into<String>, count: u64) -> Self {
        ChargeOptions { event_name: event_name.into(), count, idempotency_key: None, request_id: None }
    }
}

/// What a charge did. Counts of `None` mean no limit.
#[derive(Clone, Debug, PartialEq)]
pub struct ChargeResult {
    /// The budget allows no more events of this kind.
    pub event_charge_limit_reached: bool,
    pub charged_count: u64,
    /// How many more events of each priced kind the budget allows.
    pub chargeable_within_limit: IndexMap<String, Option<u64>>,
}

/// The pricing of the run.
#[derive(Clone, Debug, PartialEq)]
pub struct ActorPricingInfo {
    pub pricing_model: Option<String>,
    pub is_pay_per_event: bool,
    pub max_total_charge_usd: f64,
    pub per_event_prices: IndexMap<String, f64>,
}

#[derive(Debug, thiserror::Error)]
pub enum ChargingError {
    #[error("ChargingManager is not initialized")]
    NotInitialized,
    #[error("Cannot charge for synthetic event '{0}' manually")]
    SyntheticEvent(String),
    #[error("Cannot charge for synthetic event '{0}' in a multi-tenant Server Actor run")]
    SyntheticEventMultiTenant(String),
    #[error(
        "Charging for '{0}' in a multi-tenant Server Actor run requires `request_id` - pass the value of the incoming \
         request's X-Actor-Request-ID header to Actor::charge()."
    )]
    MissingRequestId(String),
    #[error(
        "Charging for pushed items is not supported in a multi-tenant Server Actor run - use Actor::charge() with the \
         `request_id` of the incoming request instead."
    )]
    PushInMultiTenantRun,
    #[error("{0}")]
    Invalid(String),
    #[error(transparent)]
    Api(#[from] ApifyClientError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("invalid pricing information: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Clone, Debug)]
struct EventPrice {
    price: f64,
    title: String,
}

/// A charge made with an idempotency key.
struct IdempotentCharge {
    event_name: String,
    charged_count: u64,
    /// When the key is forgotten.
    expires_at: Instant,
}

/// How long a charge is remembered by its idempotency key, as long as the platform does.
const IDEMPOTENCY_KEY_TTL: Duration = Duration::from_secs(3 * 60);

#[derive(Default)]
struct State {
    initialized: bool,
    /// A multi-tenant Server Actor run: no pricing snapshot on the run, and the platform prices,
    /// limits and deduplicates per caller.
    multi_tenant: bool,
    pricing_model: Option<String>,
    max_total_charge_usd: f64,
    prices: IndexMap<String, EventPrice>,
    /// Charged count and amount by event.
    charged: IndexMap<String, (u64, f64)>,
    not_ppe_warning_printed: bool,
    /// Charges made with an idempotency key, by key, in the order they expire.
    idempotent: IndexMap<String, IdempotentCharge>,
    /// Charges with an idempotency key being made, by key, closed once done.
    idempotent_in_flight: HashMap<String, watch::Receiver<()>>,
}

impl State {
    /// The charge made under `key`, unless it expired.
    fn idempotent_charge(&mut self, key: &str) -> Option<&IdempotentCharge> {
        if self.idempotent.get(key).is_some_and(|charge| charge.expires_at <= Instant::now()) {
            self.idempotent.shift_remove(key);
        }
        self.idempotent.get(key)
    }

    fn remember_idempotent_charge(&mut self, key: String, event_name: &str, charged_count: u64) {
        let now = Instant::now();
        let expired = self.idempotent.values().take_while(|charge| charge.expires_at <= now).count();
        self.idempotent.drain(..expired);
        let charge = IdempotentCharge {
            event_name: event_name.to_owned(),
            charged_count,
            expires_at: now + IDEMPOTENCY_KEY_TTL,
        };
        self.idempotent.insert(key, charge);
    }
}

/// A charge with an idempotency key being made: a repeat under its key waits for it. Once dropped,
/// the key is remembered, unless the charge failed. A charge cancelled is remembered, as it stays
/// counted.
struct IdempotentChargeInFlight<'a> {
    manager: &'a ChargingManager,
    key: String,
    event_name: String,
    charged_count: u64,
    failed: bool,
    /// Dropped after the key is remembered, which wakes the repeats waiting.
    _done: watch::Sender<()>,
}

impl Drop for IdempotentChargeInFlight<'_> {
    fn drop(&mut self) {
        let mut state = self.manager.state.lock();
        state.idempotent_in_flight.remove(&self.key);
        if !self.failed {
            state.remember_idempotent_charge(std::mem::take(&mut self.key), &self.event_name, self.charged_count);
        }
    }
}

/// The standby deployment of a multi-tenant Server Actor run starts with this.
const MULTI_TENANT_DEPLOYMENT_PREFIX: &str = "MULTI_TENANT";

/// The pricing in effect now: the latest entry of the Actor's pricing history with
/// `startedAt <= now`, as the history includes scheduled changes. An entry without a valid
/// `startedAt` is never in effect, as `new Date(...) <= now` is false in JS.
///
/// Resolved once at init on purpose. A change kicking in mid-run only affects local event titles
/// and "unknown event" warnings; the platform prices each charge from its own current pricing.
fn effective_pricing_info(pricing_infos: &[Value], now: chrono::DateTime<chrono::Utc>) -> Option<&Value> {
    let started_at = |info: &Value| {
        let started_at = info.get("startedAt")?.as_str()?;
        chrono::DateTime::parse_from_rfc3339(started_at).ok().map(|at| at.with_timezone(&chrono::Utc))
    };
    // `max_by_key` keeps the last of equal entries, as JS's stable sort and `.at(-1)` do.
    pricing_infos
        .iter()
        .filter_map(|info| Some((started_at(info)?, info)))
        .filter(|(started_at, _)| *started_at <= now)
        .max_by_key(|(started_at, _)| *started_at)
        .map(|(_, info)| info)
}

/// `f64` counts (possibly infinite) as optional integers.
fn limit(count: f64) -> Option<u64> {
    count.is_finite().then(|| count.max(0.0) as u64)
}

tokio::task_local! {
    /// Set while the Actor stores items whose charges it already reserved, so that the default
    /// dataset does not reserve them again.
    static ITEMS_RESERVED: ();
}

/// Tracks the charges of a run and makes them.
pub struct ChargingManager {
    configuration: Arc<Configuration>,
    client: ApifyClient,
    state: Mutex<State>,
    log_dataset: tokio::sync::OnceCell<Dataset>,
    /// The run and the Actor from the API, fetched once. A failed fetch is tried again next time.
    current_run: tokio::sync::OnceCell<Option<ActorRun>>,
    current_actor: tokio::sync::OnceCell<Option<ActorResource>>,
}

impl std::fmt::Debug for ChargingManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChargingManager").finish_non_exhaustive()
    }
}

impl ChargingManager {
    pub fn new(configuration: Arc<Configuration>, client: ApifyClient) -> Self {
        let max_total_charge_usd = configuration.max_total_charge_usd;
        ChargingManager {
            configuration,
            client,
            state: Mutex::new(State { max_total_charge_usd, ..State::default() }),
            log_dataset: tokio::sync::OnceCell::new(),
            current_run: tokio::sync::OnceCell::new(),
            current_actor: tokio::sync::OnceCell::new(),
        }
    }

    /// The current Actor run from the API, fetched once (see [`Actor::get_current_run`](crate::Actor::get_current_run)).
    /// `None` when not running on the Apify platform.
    pub async fn current_run(&self) -> Result<Option<ActorRun>, ApifyClientError> {
        let Some(run_id) = self.configuration.actor_run_id.clone() else { return Ok(None) };
        self.current_run.get_or_try_init(|| async { self.client.run(run_id).get().await }).await.cloned()
    }

    /// The current Actor from the API, fetched once. `None` without an Actor ID.
    async fn current_actor(&self) -> Result<Option<ActorResource>, ApifyClientError> {
        let Some(actor_id) = self.configuration.actor_id.clone() else { return Ok(None) };
        self.current_actor.get_or_try_init(|| async { self.client.actor(actor_id).get().await }).await.cloned()
    }

    /// True in a multi-tenant Server Actor run, which charges per request with
    /// [`ChargeOptions::request_id`] only.
    pub fn is_multi_tenant(&self) -> bool {
        self.state.lock().multi_tenant
    }

    /// Loads the pricing and what was charged so far (by an earlier incarnation of the run), from
    /// the environment or the API. `storage` holds the local charging log dataset.
    pub async fn init(&self, storage: &dyn StorageBackend) -> Result<(), ChargingError> {
        let configuration = &self.configuration;
        if configuration.use_charging_log_dataset && configuration.is_at_home {
            return Err(ChargingError::Invalid(
                "Using the ACTOR_USE_CHARGING_LOG_DATASET environment variable is only supported in a local \
                 development environment"
                    .to_owned(),
            ));
        }
        if configuration.test_pay_per_event && configuration.is_at_home {
            return Err(ChargingError::Invalid(
                "Using the ACTOR_TEST_PAY_PER_EVENT environment variable is only supported in a local development \
                 environment"
                    .to_owned(),
            ));
        }

        let (pricing_info, charged_counts, max_total_charge_usd, multi_tenant) = self.fetch_pricing_info().await?;
        let is_ppe = {
            let mut state = self.state.lock();
            state.multi_tenant = multi_tenant;
            let run_model = pricing_info.get("pricingModel").and_then(Value::as_str).map(str::to_owned);
            state.pricing_model =
                if configuration.test_pay_per_event { Some(PAY_PER_EVENT.to_owned()) } else { run_model.clone() };
            if run_model.as_deref() == Some(PAY_PER_EVENT) {
                let events = pricing_info.pointer("/pricingPerEvent/actorChargeEvents").and_then(Value::as_object);
                for (name, pricing) in events.into_iter().flatten() {
                    let price = pricing.get("eventPriceUsd").and_then(Value::as_f64).unwrap_or(0.0);
                    let title = pricing.get("eventTitle").and_then(Value::as_str).unwrap_or_default().to_owned();
                    state.prices.insert(name.clone(), EventPrice { price, title });
                }
                state.max_total_charge_usd = max_total_charge_usd;
            }
            state.charged.clear();
            for (name, count) in charged_counts {
                let price = state.prices.get(&name).map_or(0.0, |p| p.price);
                state.charged.insert(name, (count, count as f64 * price));
            }
            state.initialized = true;
            state.pricing_model.as_deref() == Some(PAY_PER_EVENT)
        };

        if is_ppe && configuration.use_charging_log_dataset {
            let name = StorageIdentifier::name(LOCAL_CHARGING_LOG_DATASET_NAME);
            if configuration.crawlee.purge_on_start {
                Dataset::open(storage, &name).await?.drop_storage().await?;
            }
            let dataset = Dataset::open(storage, &name).await?;
            let _ = self.log_dataset.set(dataset);
        }
        Ok(())
    }

    /// Pricing info (JSON), charged counts, the budget, and whether the run is a multi-tenant
    /// Server Actor run.
    async fn fetch_pricing_info(&self) -> Result<(Value, IndexMap<String, u64>, f64, bool), ChargingError> {
        let configuration = &self.configuration;
        let counts = |value: Option<&Value>| -> IndexMap<String, u64> {
            value
                .and_then(Value::as_object)
                .map(|counts| counts.iter().filter_map(|(k, v)| Some((k.clone(), v.as_u64()?))).collect())
                .unwrap_or_default()
        };
        if let (Some(pricing), Some(charged)) = (&configuration.actor_pricing_info, &configuration.charged_event_counts)
        {
            let pricing: Value = serde_json::from_str(pricing)?;
            let charged: Value = serde_json::from_str(charged)?;
            return Ok((pricing, counts(Some(&charged)), configuration.max_total_charge_usd, false));
        }
        if configuration.is_at_home {
            if configuration.actor_run_id.is_none() {
                return Err(ChargingError::Invalid(
                    "Actor run ID not found even though the Actor is running on Apify".to_owned(),
                ));
            }
            // A Server Actor run lives long enough to make fetching the Actor worth it up front:
            // a multi-tenant one needs it, and it is fetched at the same time as the run.
            let is_standby = configuration.meta_origin.as_deref() == Some("STANDBY");
            let (run, actor) = tokio::join!(self.current_run(), async {
                if is_standby { Some(self.current_actor().await) } else { None }
            });
            let run = run?.ok_or_else(|| ChargingError::Invalid("Actor run not found".to_owned()))?;
            let deployment = run.extra.get("standby").and_then(|standby| standby.get("deployment"));
            if deployment.and_then(Value::as_str).is_some_and(|d| d.starts_with(MULTI_TENANT_DEPLOYMENT_PREFIX)) {
                let actor = match actor {
                    Some(actor) => actor,
                    None => self.current_actor().await,
                };
                let actor = actor?.ok_or_else(|| ChargingError::Invalid("Actor not found".to_owned()))?;
                let pricing_infos = actor.extra.get("pricingInfos").and_then(Value::as_array);
                let pricing =
                    effective_pricing_info(pricing_infos.map_or(&[], Vec::as_slice), chrono::Utc::now()).cloned();
                // The run's counts mix all callers, and the platform prices and limits per caller.
                return Ok((pricing.unwrap_or(Value::Null), IndexMap::new(), f64::INFINITY, true));
            }
            let max = run
                .extra
                .get("options")
                .and_then(|options| options.get("maxTotalChargeUsd"))
                .and_then(Value::as_f64)
                .filter(|&max| max != 0.0)
                .unwrap_or(f64::INFINITY);
            let pricing = run.extra.get("pricingInfo").cloned().unwrap_or(Value::Null);
            return Ok((pricing, counts(run.extra.get("chargedEventCounts")), max, false));
        }
        Ok((Value::Null, IndexMap::new(), configuration.max_total_charge_usd, false))
    }

    pub fn is_initialized(&self) -> bool {
        self.state.lock().initialized
    }

    pub fn is_pay_per_event(&self) -> bool {
        self.state.lock().pricing_model.as_deref() == Some(PAY_PER_EVENT)
    }

    pub fn pricing_info(&self) -> Result<ActorPricingInfo, ChargingError> {
        let state = self.state.lock();
        if !state.initialized {
            return Err(ChargingError::NotInitialized);
        }
        Ok(ActorPricingInfo {
            is_pay_per_event: state.pricing_model.as_deref() == Some(PAY_PER_EVENT),
            pricing_model: state.pricing_model.clone(),
            max_total_charge_usd: state.max_total_charge_usd,
            per_event_prices: state.prices.iter().map(|(name, p)| (name.clone(), p.price)).collect(),
        })
    }

    pub fn charged_event_count(&self, event_name: &str) -> u64 {
        self.state.lock().charged.get(event_name).map_or(0, |(count, _)| *count)
    }

    /// Charges for `count` events, as far as the budget allows. When it allows fewer, one more is
    /// charged, so that the platform notices and stops the run.
    ///
    /// A charge repeated under the same idempotency key within three minutes is not made again: it
    /// reports the count of the original charge, or 0 when that was of another event. A repeat
    /// while the original is being made waits for it, and is made if the original failed.
    pub async fn charge(&self, options: ChargeOptions) -> Result<ChargeResult, ChargingError> {
        if options.idempotency_key.as_deref().is_some_and(|key| key.trim().is_empty()) {
            return Err(ChargingError::Invalid("idempotencyKey must not be blank".to_owned()));
        }
        if !self.is_pay_per_event() {
            let mut state = self.state.lock();
            if !state.not_ppe_warning_printed {
                tracing::warn!(
                    "Ignored attempt to charge for an event - the Actor does not use the pay-per-event pricing"
                );
                state.not_ppe_warning_printed = true;
            }
            return Ok(ChargeResult {
                event_charge_limit_reached: false,
                charged_count: 0,
                chargeable_within_limit: self.chargeable_within_limit_of(&state),
            });
        }
        if self.is_multi_tenant() {
            return self.charge_server_request(options).await;
        }
        // As the Python SDK: no charge of nothing reaches the platform or the charging state, and
        // its idempotency key is not remembered.
        if options.count == 0 {
            return Ok(ChargeResult {
                event_charge_limit_reached: false,
                charged_count: 0,
                chargeable_within_limit: self.chargeable_within_limit(),
            });
        }
        if !self.is_initialized() {
            return Err(ChargingError::NotInitialized);
        }
        let ChargeOptions { event_name, count, idempotency_key, request_id } = options;
        let (charged_count, mut in_flight) = 'charge: loop {
            let mut in_flight_done = 'wait: {
                let mut state = self.state.lock();
                if let Some(key) = &idempotency_key
                    && let Some(in_flight_done) = state.idempotent_in_flight.get(key)
                {
                    break 'wait in_flight_done.clone();
                }
                if let Some(key) = &idempotency_key
                    && let Some(previous) = state.idempotent_charge(key)
                {
                    // A repeat the platform would discard: counting it would spend the budget early.
                    let repeated_count = if previous.event_name == event_name {
                        tracing::debug!("Skipped a repeated charge of event '{event_name}' under key '{key}'.");
                        previous.charged_count
                    } else {
                        tracing::warn!(
                            "Idempotency key '{key}' was already used to charge for event '{}', so the charge of \
                             event '{event_name}' is skipped.",
                            previous.event_name
                        );
                        0
                    };
                    return Ok(ChargeResult {
                        event_charge_limit_reached: self.is_event_charge_limit_reached_of(&state, &event_name),
                        charged_count: repeated_count,
                        chargeable_within_limit: self.chargeable_within_limit_of(&state),
                    });
                }
                let charged_count = self.reserve(&mut state, &event_name, count);
                let in_flight = idempotency_key.clone().map(|key| {
                    let (done, in_flight_done) = watch::channel(());
                    state.idempotent_in_flight.insert(key.clone(), in_flight_done);
                    IdempotentChargeInFlight {
                        manager: self,
                        key,
                        event_name: event_name.clone(),
                        charged_count,
                        failed: false,
                        _done: done,
                    }
                });
                break 'charge (charged_count, in_flight);
            };
            // A repeat waits for the charge under its key, to go by what that charge did.
            let _ = in_flight_done.changed().await;
        };
        if charged_count > 0
            && let Err(err) = self.send_charge(&event_name, charged_count, idempotency_key.clone(), request_id).await
        {
            // Counted only once made, so that a failed charge retried under its key is counted once.
            self.add_charged(&mut self.state.lock(), &event_name, -(charged_count as i64));
            if let Some(in_flight) = &mut in_flight {
                in_flight.failed = true;
            }
            return Err(err);
        }
        // Remembered once made.
        drop(in_flight);
        if charged_count < count && charged_count > 0 {
            let subject = if count == 1 { "instance" } else { "instances" };
            tracing::info!(
                "Charging {count} {subject} of '{event_name}' event would exceed maxTotalChargeUsd - only \
                 {charged_count} events were charged"
            );
        }
        let state = self.state.lock();
        Ok(ChargeResult {
            event_charge_limit_reached: if charged_count == 0 {
                count > 0
            } else {
                self.is_event_charge_limit_reached_of(&state, &event_name)
            },
            charged_count,
            chargeable_within_limit: self.chargeable_within_limit_of(&state),
        })
    }

    /// A charge in a multi-tenant Server Actor run. The platform prices, limits and deduplicates
    /// each caller's charges itself, so nothing is counted or enforced locally, and every call is
    /// sent: the platform discards a repeat under an idempotency key used earlier in the run, even
    /// by another caller.
    async fn charge_server_request(&self, options: ChargeOptions) -> Result<ChargeResult, ChargingError> {
        let ChargeOptions { event_name, count, idempotency_key, request_id } = options;
        if event_name.starts_with("apify-") {
            return Err(ChargingError::SyntheticEventMultiTenant(event_name));
        }
        let Some(request_id) = request_id.filter(|id| !id.is_empty()) else {
            return Err(ChargingError::MissingRequestId(event_name));
        };
        let known = self.state.lock().prices.contains_key(&event_name);
        let mut charged_count = 0;
        if count == 0 {
            // Nothing to charge.
        } else if !known {
            tracing::warn!("Attempting to charge for an unknown event '{event_name}'");
        } else {
            let run_id = self.configuration.actor_run_id.clone().unwrap_or_default();
            let charge = RunChargeOptions {
                event_name,
                count: Some(count as i64),
                idempotency_key,
                request_id: Some(request_id),
            };
            self.client.run(run_id).charge(charge).await?;
            charged_count = count;
        }
        Ok(ChargeResult {
            event_charge_limit_reached: false,
            charged_count,
            chargeable_within_limit: self.chargeable_within_limit(),
        })
    }

    /// Counts up to `count` events as charged, as far as the budget allows (one more when it
    /// allows fewer, unless already over it), and returns how many.
    fn reserve(&self, state: &mut State, event_name: &str, count: u64) -> u64 {
        let max = self.max_event_charge_count_of(state, event_name);
        let charged_count = if (count as f64) <= max {
            count
        } else if self.total_charged_amount_of(state) <= state.max_total_charge_usd {
            // Over budget: one more than allowed, so that the platform stops the run. Not when
            // already strictly over it.
            max as u64 + 1
        } else {
            0
        };
        self.add_charged(state, event_name, charged_count as i64);
        charged_count
    }

    /// Adds `count` (or takes it back, when negative) to what was charged for `event_name`.
    fn add_charged(&self, state: &mut State, event_name: &str, count: i64) {
        if count == 0 {
            return;
        }
        // Unknown events cost 1 locally, so that the budget can be reached in development.
        let price =
            state.prices.get(event_name).map_or(if self.configuration.is_at_home { 0.0 } else { 1.0 }, |p| p.price);
        let entry = state.charged.entry(event_name.to_owned()).or_insert((0, 0.0));
        entry.0 = entry.0.saturating_add_signed(count);
        entry.1 += count as f64 * price;
    }

    /// Reserves the charges of as many of `items_count` items as the budget allows, each charged
    /// one of every event of `events` (an event listed twice, twice), plus the default dataset item event if
    /// `is_default_dataset`. Returns how many items to store. All the charges are counted at
    /// once, so that concurrent pushes cannot together spend more than the budget.
    pub(crate) fn reserve_items(&self, items_count: usize, events: &[&str], is_default_dataset: bool) -> usize {
        let mut state = self.state.lock();
        let limit = self.push_data_limit_of(&state, items_count, events, is_default_dataset);
        if state.pricing_model.as_deref() == Some(PAY_PER_EVENT) && !state.multi_tenant {
            for event in Self::item_events(events, is_default_dataset) {
                self.add_charged(&mut state, event, limit as i64);
            }
        }
        limit
    }

    /// Takes back a reservation of [`reserve_items`](Self::reserve_items) for items that were
    /// not stored.
    pub(crate) fn unreserve_items(&self, events: &[&str], is_default_dataset: bool, items_count: usize) {
        let mut state = self.state.lock();
        if state.pricing_model.as_deref() == Some(PAY_PER_EVENT) && !state.multi_tenant {
            for event in Self::item_events(events, is_default_dataset) {
                self.add_charged(&mut state, event, -(items_count as i64));
            }
        }
    }

    /// Sends the charges reserved by [`reserve_items`](Self::reserve_items).
    pub(crate) async fn send_item_charges(
        &self,
        events: &[&str],
        is_default_dataset: bool,
        items_count: usize,
    ) -> Result<(), ChargingError> {
        if !self.is_pay_per_event() || self.is_multi_tenant() || items_count == 0 {
            return Ok(());
        }
        // An event listed more than once goes in one call, as many times per item.
        let mut counts: IndexMap<&str, u64> = IndexMap::new();
        for event in Self::item_events(events, is_default_dataset) {
            *counts.entry(event).or_default() += items_count as u64;
        }
        for (event, count) in counts {
            self.send_charge(event, count, None, None).await?;
        }
        Ok(())
    }

    fn item_events<'a>(events: &'a [&'a str], is_default_dataset: bool) -> impl Iterator<Item = &'a str> {
        events.iter().copied().chain(is_default_dataset.then_some(DEFAULT_DATASET_ITEM_EVENT))
    }

    /// Stores items with `store`, whose charges were reserved already: the default dataset does
    /// not charge them again.
    pub(crate) async fn with_items_reserved<F: Future>(store: F) -> F::Output {
        ITEMS_RESERVED.scope((), store).await
    }

    /// Sends a charge counted already: to the platform (unless synthetic, charged by the platform
    /// itself), and to the local charging log.
    async fn send_charge(
        &self,
        event_name: &str,
        count: u64,
        idempotency_key: Option<String>,
        request_id: Option<String>,
    ) -> Result<(), ChargingError> {
        let (known, title, price) = {
            let state = self.state.lock();
            match state.prices.get(event_name) {
                Some(pricing) => (true, pricing.title.clone(), pricing.price),
                None => (
                    false,
                    format!("Unknown event '{event_name}'"),
                    if self.configuration.is_at_home { 0.0 } else { 1.0 },
                ),
            }
        };
        if self.configuration.is_at_home {
            if event_name.starts_with("apify-") {
                // Synthetic events are charged by the platform itself, from the dataset writes.
            } else if known {
                let run_id = self.configuration.actor_run_id.clone().unwrap_or_default();
                let charge = RunChargeOptions {
                    event_name: event_name.to_owned(),
                    count: Some(count as i64),
                    idempotency_key: idempotency_key.clone(),
                    request_id: request_id.clone(),
                };
                self.client.run(run_id).charge(charge).await?;
            } else {
                tracing::warn!("Attempting to charge for an unknown event '{event_name}'");
            }
        }
        if let Some(dataset) = self.log_dataset.get() {
            let mut entry = serde_json::json!({
                "eventName": event_name,
                "eventTitle": title,
                "eventPriceUsd": price,
                "chargedCount": count,
            });
            if let Some(key) = idempotency_key {
                entry["idempotencyKey"] = Value::String(key);
            }
            if let Some(request_id) = request_id {
                entry["requestId"] = Value::String(request_id);
            }
            entry["timestamp"] = Value::String(crawlee::core::now_iso());
            dataset.push_data(&entry).await?;
        }
        Ok(())
    }

    fn total_charged_amount_of(&self, state: &State) -> f64 {
        js_to_fixed(state.charged.values().map(|(_, amount)| amount).sum(), 6)
    }

    /// The price used for limits: the real one on the platform, 1 locally.
    fn event_price_of(&self, state: &State, event_name: &str) -> Option<f64> {
        if self.configuration.is_at_home { state.prices.get(event_name).map(|p| p.price) } else { Some(1.0) }
    }

    fn max_charges_by_price_of(&self, state: &State, price: f64) -> f64 {
        let unrounded = (state.max_total_charge_usd - self.total_charged_amount_of(state)) / price;
        // Rounded first: 4.9999999999999999 must not floor to 4.
        js_to_fixed(unrounded, 4).floor().max(0.0)
    }

    fn max_event_charge_count_of(&self, state: &State, event_name: &str) -> f64 {
        match self.event_price_of(state, event_name) {
            Some(price) if price != 0.0 => self.max_charges_by_price_of(state, price),
            _ => f64::INFINITY,
        }
    }

    fn is_event_charge_limit_reached_of(&self, state: &State, event_name: &str) -> bool {
        state.pricing_model.as_deref() == Some(PAY_PER_EVENT)
            && self.max_event_charge_count_of(state, event_name) <= 0.0
    }

    fn chargeable_within_limit_of(&self, state: &State) -> IndexMap<String, Option<u64>> {
        state.prices.keys().map(|name| (name.clone(), limit(self.max_event_charge_count_of(state, name)))).collect()
    }

    /// How many more `event_name` events the budget allows; `None` is no limit.
    pub fn max_event_charge_count_within_limit(&self, event_name: &str) -> Option<u64> {
        limit(self.max_event_charge_count_of(&self.state.lock(), event_name))
    }

    pub fn is_event_charge_limit_reached(&self, event_name: &str) -> bool {
        self.is_event_charge_limit_reached_of(&self.state.lock(), event_name)
    }

    pub fn chargeable_within_limit(&self) -> IndexMap<String, Option<u64>> {
        self.chargeable_within_limit_of(&self.state.lock())
    }

    /// How many of `items_count` items can be pushed within the budget, each charged as
    /// `event_name` (if set) plus the default dataset item event (if `is_default_dataset`).
    pub fn push_data_limit(&self, items_count: usize, event_name: Option<&str>, is_default_dataset: bool) -> usize {
        let events: Vec<&str> = event_name.into_iter().collect();
        self.push_data_limit_of(&self.state.lock(), items_count, &events, is_default_dataset)
    }

    fn push_data_limit_of(
        &self,
        state: &State,
        items_count: usize,
        events: &[&str],
        is_default_dataset: bool,
    ) -> usize {
        if state.pricing_model.as_deref() != Some(PAY_PER_EVENT) || items_count == 0 {
            return items_count;
        }
        let item_price: f64 = Self::item_events(events, is_default_dataset)
            .map(|event| self.event_price_of(state, event).unwrap_or(0.0))
            .sum();
        if item_price == 0.0 {
            return items_count;
        }
        let max = self.max_charges_by_price_of(state, item_price);
        if max >= items_count as f64 {
            return items_count;
        }
        if max > 0.0 {
            return max as usize;
        }
        usize::from(self.total_charged_amount_of(state) <= state.max_total_charge_usd)
    }
}

/// The default dataset, charging the default dataset item event for what it stores.
struct ChargingDataset {
    inner: Arc<dyn DatasetBackend>,
    charging: Arc<ChargingManager>,
}

#[async_trait]
impl DatasetBackend for ChargingDataset {
    async fn get_metadata(&self) -> StorageResult<DatasetInfo> {
        self.inner.get_metadata().await
    }

    async fn drop_storage(&self) -> StorageResult<()> {
        self.inner.drop_storage().await
    }

    async fn purge(&self) -> StorageResult<()> {
        self.inner.purge().await
    }

    async fn push_data(&self, mut items: Vec<DatasetItem>) -> StorageResult<()> {
        let charging = &self.charging;
        // Datasets also work before the Actor is initialized, with nothing to charge. Items the
        // Actor reserved the charges of are stored as they are.
        // Multi-tenant runs charge per request only, so dataset writes charge nothing there.
        if items.is_empty()
            || !charging.is_initialized()
            || !charging.is_pay_per_event()
            || charging.is_multi_tenant()
            || ITEMS_RESERVED.try_with(|_| ()).is_ok()
        {
            return self.inner.push_data(items).await;
        }
        let limit = charging.reserve_items(items.len(), &[], true);
        if limit == 0 {
            return Ok(());
        }
        items.truncate(limit);
        if let Err(err) = self.inner.push_data(items).await {
            charging.unreserve_items(&[], true, limit);
            return Err(err);
        }
        charging.send_item_charges(&[], true, limit).await.map_err(|err| StorageError::Backend(Box::new(err)))
    }

    async fn get_data(&self, options: DatasetListOptions) -> StorageResult<PaginatedList<DatasetItem>> {
        self.inner.get_data(options).await
    }
}

/// A storage backend whose default dataset charges for its items.
pub(crate) struct ChargingStorageBackend {
    pub(crate) inner: Arc<dyn StorageBackend>,
    pub(crate) charging: Arc<ChargingManager>,
    pub(crate) default_dataset_id: String,
}

impl ChargingStorageBackend {
    fn is_default_dataset(&self, id: &StorageIdentifier) -> bool {
        match id {
            StorageIdentifier::Default => true,
            StorageIdentifier::Alias(alias) => alias == DEFAULT_STORAGE_ALIAS,
            StorageIdentifier::Id(id) => *id == self.default_dataset_id,
            StorageIdentifier::Name(_) => false,
        }
    }
}

#[async_trait]
impl StorageBackend for ChargingStorageBackend {
    async fn create_dataset_backend(&self, id: &StorageIdentifier) -> StorageResult<Arc<dyn DatasetBackend>> {
        let backend = self.inner.create_dataset_backend(id).await?;
        if !self.is_default_dataset(id) {
            return Ok(backend);
        }
        Ok(Arc::new(ChargingDataset { inner: backend, charging: self.charging.clone() }))
    }

    async fn create_key_value_store_backend(
        &self,
        id: &StorageIdentifier,
    ) -> StorageResult<Arc<dyn KeyValueStoreBackend>> {
        self.inner.create_key_value_store_backend(id).await
    }

    async fn create_request_queue_backend(
        &self,
        id: &StorageIdentifier,
    ) -> StorageResult<Arc<dyn RequestQueueBackend>> {
        self.inner.create_request_queue_backend(id).await
    }

    async fn storage_exists(&self, id: &str, kind: StorageKind) -> StorageResult<bool> {
        self.inner.storage_exists(id, kind).await
    }

    async fn purge(&self) -> StorageResult<()> {
        self.inner.purge().await
    }

    async fn teardown(&self) -> StorageResult<()> {
        self.inner.teardown().await
    }

    fn rate_limit_errors(&self) -> u64 {
        self.inner.rate_limit_errors()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager(max: f64, prices: &[(&str, f64)], charged: &[(&str, u64)]) -> ChargingManager {
        let events: serde_json::Map<String, Value> = prices
            .iter()
            .map(|(name, price)| {
                ((*name).to_owned(), serde_json::json!({ "eventTitle": name, "eventPriceUsd": price }))
            })
            .collect();
        let pricing =
            serde_json::json!({ "pricingModel": "PAY_PER_EVENT", "pricingPerEvent": { "actorChargeEvents": events } });
        let charged: serde_json::Map<String, Value> =
            charged.iter().map(|(name, count)| ((*name).to_owned(), serde_json::json!(count))).collect();
        let configuration = Configuration {
            is_at_home: true,
            actor_run_id: Some("run".to_owned()),
            max_total_charge_usd: max,
            actor_pricing_info: Some(pricing.to_string()),
            charged_event_counts: Some(Value::Object(charged).to_string()),
            ..Configuration::default()
        };
        let client = crate::new_client(&configuration, None, Default::default());
        ChargingManager::new(Arc::new(configuration), client)
    }

    #[tokio::test]
    async fn a_charge_of_nothing_changes_nothing_and_keeps_no_key() {
        let manager = manager(10.0, &[(DEFAULT_DATASET_ITEM_EVENT, 1.0)], &[]);
        // A synthetic event, which is counted without an API call.
        manager.init(&crawlee::core::MemoryStorageBackend::new()).await.unwrap();
        assert_eq!(manager.charge(ChargeOptions::new(DEFAULT_DATASET_ITEM_EVENT, 2)).await.unwrap().charged_count, 2);
        let keyed = |count: u64| ChargeOptions {
            idempotency_key: Some("k".to_owned()),
            ..ChargeOptions::new(DEFAULT_DATASET_ITEM_EVENT, count)
        };
        let nothing = manager.charge(keyed(0)).await.unwrap();
        assert_eq!(nothing.charged_count, 0);
        assert!(!nothing.event_charge_limit_reached);
        assert_eq!(manager.charged_event_count(DEFAULT_DATASET_ITEM_EVENT), 2);
        // The key of a charge of nothing is not remembered: a charge under it is made.
        assert_eq!(manager.charge(keyed(1)).await.unwrap().charged_count, 1);
        assert_eq!(manager.charged_event_count(DEFAULT_DATASET_ITEM_EVENT), 3);
    }

    #[test]
    fn the_effective_pricing_is_the_latest_one_started() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-06-01T00:00:00Z").unwrap().with_timezone(&chrono::Utc);
        let info = |n: u64, started_at: &str| serde_json::json!({ "n": n, "startedAt": started_at });
        let history = [
            info(1, "2021-01-01T00:00:00.000Z"),
            info(2, "2999-01-01T00:00:00.000Z"),
            info(3, "2020-01-01T00:00:00.000Z"),
            info(4, "2026-06-01T00:00:00.000Z"),
            info(5, "not a date"),
        ];
        assert_eq!(effective_pricing_info(&history, now).unwrap()["n"], 4, "started exactly now");
        assert_eq!(effective_pricing_info(&history[..3], now).unwrap()["n"], 1, "not the last, nor a future one");
        // Of entries started at the same time, the last one, as JS's stable sort keeps it.
        let ties = [info(1, "2020-01-01T00:00:00Z"), info(2, "2020-01-01T00:00:00.000Z")];
        assert_eq!(effective_pricing_info(&ties, now).unwrap()["n"], 2);
        assert!(effective_pricing_info(&history[1..2], now).is_none());
        assert!(effective_pricing_info(&[], now).is_none());
    }

    #[tokio::test]
    async fn a_charge_repeated_under_its_idempotency_key_counts_once() {
        let manager = manager(10.0, &[("page", 1.0), ("apify-default-dataset-item", 0.5)], &[]);
        manager.init(&crawlee::core::MemoryStorageBackend::new()).await.unwrap();
        let keyed = |event: &str, count: u64, key: &str| ChargeOptions {
            idempotency_key: Some(key.to_owned()),
            ..ChargeOptions::new(event, count)
        };
        let item = "apify-default-dataset-item";
        assert_eq!(manager.charge(keyed(item, 2, "k")).await.unwrap().charged_count, 2);
        let repeated = manager.charge(keyed(item, 2, "k")).await.unwrap();
        assert_eq!(repeated.charged_count, 2, "the count of the original charge");
        assert_eq!(manager.charged_event_count(item), 2);
        // The key alone decides: another event under it is not charged.
        assert_eq!(manager.charge(keyed("page", 1, "k")).await.unwrap().charged_count, 0);
        assert_eq!(manager.charged_event_count("page"), 0);
        assert_eq!(manager.charge(keyed(item, 1, "other")).await.unwrap().charged_count, 1);
        assert!(matches!(manager.charge(keyed(item, 1, " ")).await, Err(ChargingError::Invalid(_))));
    }

    #[tokio::test(start_paused = true)]
    async fn an_idempotency_key_is_forgotten_after_three_minutes() {
        let manager = manager(10.0, &[(DEFAULT_DATASET_ITEM_EVENT, 0.5)], &[]);
        manager.init(&crawlee::core::MemoryStorageBackend::new()).await.unwrap();
        let keyed = || ChargeOptions {
            idempotency_key: Some("k".to_owned()),
            ..ChargeOptions::new(DEFAULT_DATASET_ITEM_EVENT, 1)
        };

        manager.charge(keyed()).await.unwrap();
        tokio::time::advance(IDEMPOTENCY_KEY_TTL - Duration::from_secs(1)).await;
        manager.charge(keyed()).await.unwrap();
        assert_eq!(manager.charged_event_count(DEFAULT_DATASET_ITEM_EVENT), 1, "a repeat within the TTL");
        tokio::time::advance(Duration::from_secs(1)).await;
        manager.charge(keyed()).await.unwrap();
        assert_eq!(manager.charged_event_count(DEFAULT_DATASET_ITEM_EVENT), 2, "charged again once expired");
    }

    #[tokio::test]
    async fn budget_limits_and_restored_counts() {
        let manager = manager(10.0, &[("page", 1.0), ("apify-default-dataset-item", 0.5)], &[("page", 3)]);
        manager.init(&crawlee::core::MemoryStorageBackend::new()).await.unwrap();
        assert!(manager.is_pay_per_event());
        assert_eq!(manager.charged_event_count("page"), 3);
        assert_eq!(manager.max_event_charge_count_within_limit("page"), Some(7));
        assert_eq!(manager.max_event_charge_count_within_limit("unknown"), None, "unpriced events are unlimited");
        // An item costs 1 + 0.5; 7 USD are left.
        assert_eq!(manager.push_data_limit(10, Some("page"), true), 4);
        assert_eq!(manager.push_data_limit(3, Some("page"), true), 3);

        // Charges above the budget are cut, plus one to make the platform stop the run.
        let result = manager.charge(ChargeOptions::new("apify-default-dataset-item", 20)).await.unwrap();
        assert_eq!(result.charged_count, 15);
        assert!(result.event_charge_limit_reached);
        assert_eq!(manager.max_event_charge_count_within_limit("page"), Some(0));
        let result = manager.charge(ChargeOptions::new("apify-default-dataset-item", 1)).await.unwrap();
        assert_eq!(result.charged_count, 0, "strictly over the budget: nothing more");
        assert!(result.event_charge_limit_reached);
        assert_eq!(manager.push_data_limit(5, None, true), 0);
    }

    /// An initialized manager of a run whose items cost 0.5 USD, out of a budget of 5.
    async fn item_manager() -> Arc<ChargingManager> {
        let manager = Arc::new(manager(5.0, &[(DEFAULT_DATASET_ITEM_EVENT, 0.5)], &[]));
        manager.init(&crawlee::core::MemoryStorageBackend::new()).await.unwrap();
        manager
    }

    /// In-memory storage whose default dataset, `default-id`, charges for its items.
    fn charged_storage(manager: &Arc<ChargingManager>) -> ChargingStorageBackend {
        ChargingStorageBackend {
            inner: Arc::new(crawlee::core::MemoryStorageBackend::new()),
            charging: manager.clone(),
            default_dataset_id: "default-id".to_owned(),
        }
    }

    fn items(count: usize) -> Vec<Value> {
        (0..count).map(|n| serde_json::json!({ "n": n })).collect()
    }

    #[tokio::test]
    async fn the_default_dataset_stores_as_many_items_as_the_budget_allows() {
        let manager = item_manager().await;
        let storage = charged_storage(&manager);
        let dataset = Dataset::open(&storage, &StorageIdentifier::Default).await.unwrap();
        let stored = async || dataset.get_all::<Value>().await.unwrap().len();

        dataset.push_data(&items(4)).await.unwrap();
        assert_eq!(manager.charged_event_count(DEFAULT_DATASET_ITEM_EVENT), 4);
        assert_eq!(manager.max_event_charge_count_within_limit(DEFAULT_DATASET_ITEM_EVENT), Some(6));
        // Cut to the 6 the budget has left.
        dataset.push_data(&items(10)).await.unwrap();
        assert_eq!(manager.charged_event_count(DEFAULT_DATASET_ITEM_EVENT), 10);
        assert_eq!(stored().await, 10);
        // The budget spent: one more, so that the platform stops the run, then none.
        dataset.push_data(&items(3)).await.unwrap();
        assert_eq!(stored().await, 11);
        dataset.push_data(&items(3)).await.unwrap();
        assert_eq!(stored().await, 11, "dropped");
        assert_eq!(manager.charged_event_count(DEFAULT_DATASET_ITEM_EVENT), 11);
    }

    /// A dataset that fails to store anything.
    struct FailingDataset;

    #[async_trait]
    impl DatasetBackend for FailingDataset {
        async fn get_metadata(&self) -> StorageResult<DatasetInfo> {
            unimplemented!()
        }

        async fn drop_storage(&self) -> StorageResult<()> {
            unimplemented!()
        }

        async fn purge(&self) -> StorageResult<()> {
            unimplemented!()
        }

        async fn push_data(&self, _items: Vec<DatasetItem>) -> StorageResult<()> {
            Err(StorageError::Backend("refused".into()))
        }

        async fn get_data(&self, _options: DatasetListOptions) -> StorageResult<PaginatedList<DatasetItem>> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn items_the_default_dataset_fails_to_store_are_not_charged() {
        let manager = item_manager().await;
        let dataset = ChargingDataset { inner: Arc::new(FailingDataset), charging: manager.clone() };
        let items = items(3).iter().map(|item| serde_json::value::to_raw_value(item).unwrap()).collect();

        assert!(dataset.push_data(items).await.is_err());
        assert_eq!(manager.charged_event_count(DEFAULT_DATASET_ITEM_EVENT), 0);
        assert_eq!(manager.max_event_charge_count_within_limit(DEFAULT_DATASET_ITEM_EVENT), Some(10));
    }

    #[tokio::test]
    async fn the_default_dataset_is_charged_also_when_opened_by_its_id() {
        let manager = item_manager().await;
        let storage = charged_storage(&manager);
        let charged = async |id: StorageIdentifier| {
            let before = manager.charged_event_count(DEFAULT_DATASET_ITEM_EVENT);
            Dataset::open(&storage, &id).await.unwrap().push_data(&items(1)).await.unwrap();
            manager.charged_event_count(DEFAULT_DATASET_ITEM_EVENT) > before
        };

        assert!(charged(StorageIdentifier::Default).await);
        assert!(charged(StorageIdentifier::id("default-id")).await);
        assert!(charged(StorageIdentifier::alias(DEFAULT_STORAGE_ALIAS)).await);
        assert!(!charged(StorageIdentifier::id("other-id")).await);
        assert!(!charged(StorageIdentifier::name("default-id")).await, "a name is not an id");
    }
}
