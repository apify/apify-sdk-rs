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
//! With buffered dataset writes (an extra of crawlee-rs, `CRAWLEE_DATASET_BUFFER`), the charges
//! of pushed items are still counted against the budget when the items are pushed, but sent once
//! per flush, with the counts of all its items summed by event. A flush that fails gives its
//! charges back.

use std::sync::Arc;
use std::time::{Duration, Instant};

use apify_client::{ApifyClient, ApifyClientError, RunChargeOptions};
use async_trait::async_trait;
use crawlee::core::StorageResult;
use crawlee::core::errors::StorageError;
use crawlee::core::storage::backend::{
    DatasetBackend, DatasetInfo, DatasetItem, DatasetListOptions, KeyValueStoreBackend, PaginatedList,
    RequestQueueBackend, StorageBackend, StorageKind,
};
use crawlee::core::storage::dataset_buffer::FlushCounters;
use crawlee::{Dataset, StorageIdentifier};
use indexmap::IndexMap;
use parking_lot::Mutex;
use serde_json::Value;

use crate::configuration::Configuration;
use crate::storage::DEFAULT_STORAGE_ALIAS;

/// The synthetic event of an item stored in the default dataset.
pub const DEFAULT_DATASET_ITEM_EVENT: &str = "apify-default-dataset-item";
const LOCAL_CHARGING_LOG_DATASET_NAME: &str = "charging_log";
const PAY_PER_EVENT: &str = "PAY_PER_EVENT";

/// `Number(x.toFixed(digits))` of JavaScript: rounds the exact value of `x`, halves away from zero.
pub(crate) fn js_to_fixed(x: f64, digits: usize) -> f64 {
    if !x.is_finite() || x.abs() >= 1e21 {
        return x;
    }
    // The exact decimal expansion of an f64 has at most 1074 fractional digits.
    let exact = format!("{:.1074}", x.abs());
    let (int_part, frac) = exact.split_once('.').expect("a fractional part");
    let mut digits_str: Vec<u8> = int_part.bytes().chain(frac.bytes().take(digits)).collect();
    if frac.as_bytes()[digits] >= b'5' {
        // Increment the decimal number in `digits_str`.
        let mut i = digits_str.len();
        loop {
            if i == 0 {
                digits_str.insert(0, b'1');
                break;
            }
            i -= 1;
            if digits_str[i] == b'9' {
                digits_str[i] = b'0';
            } else {
                digits_str[i] += 1;
                break;
            }
        }
    }
    let split = digits_str.len() - digits;
    let text = format!(
        "{}{}.{}",
        if x < 0.0 { "-" } else { "" },
        std::str::from_utf8(&digits_str[..split]).expect("digits"),
        std::str::from_utf8(&digits_str[split..]).expect("digits")
    );
    text.parse().expect("a number")
}

/// A charge to make.
#[derive(Clone, Debug)]
pub struct ChargeOptions {
    pub event_name: String,
    pub count: u64,
    /// Keeps a retried operation from being charged twice: a charge repeated under the same key
    /// within three minutes is not made again (see [`ChargingManager::charge`]).
    pub idempotency_key: Option<String>,
}

impl ChargeOptions {
    pub fn new(event_name: impl Into<String>, count: u64) -> Self {
        ChargeOptions { event_name: event_name.into(), count, idempotency_key: None }
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
    pricing_model: Option<String>,
    max_total_charge_usd: f64,
    prices: IndexMap<String, EventPrice>,
    /// Charged count and amount by event.
    charged: IndexMap<String, (u64, f64)>,
    not_ppe_warning_printed: bool,
    /// Charges made with an idempotency key, by key, in the order they expire.
    idempotent: IndexMap<String, IdempotentCharge>,
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

/// `f64` counts (possibly infinite) as optional integers.
fn limit(count: f64) -> Option<u64> {
    count.is_finite().then(|| count.max(0.0) as u64)
}

tokio::task_local! {
    /// Set while the Actor stores items whose charges it already reserved, so that the default
    /// dataset does not reserve them again. Holds the reserved charges until a buffered dataset
    /// takes them over, to send them when it stores the items.
    static ITEMS_RESERVED: Arc<Mutex<Option<FlushCounters>>>;
}

/// Tracks the charges of a run and makes them.
pub struct ChargingManager {
    configuration: Arc<Configuration>,
    client: ApifyClient,
    state: Mutex<State>,
    log_dataset: tokio::sync::OnceCell<Dataset>,
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
        }
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

        let (pricing_info, charged_counts, max_total_charge_usd) = self.fetch_pricing_info().await?;
        let is_ppe = {
            let mut state = self.state.lock();
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

    /// Pricing info (JSON), charged counts, and the budget.
    async fn fetch_pricing_info(&self) -> Result<(Value, IndexMap<String, u64>, f64), ChargingError> {
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
            return Ok((pricing, counts(Some(&charged)), configuration.max_total_charge_usd));
        }
        if configuration.is_at_home {
            let run_id = configuration.actor_run_id.clone().ok_or_else(|| {
                ChargingError::Invalid("Actor run ID not found even though the Actor is running on Apify".to_owned())
            })?;
            let run = self
                .client
                .run(run_id)
                .get()
                .await?
                .ok_or_else(|| ChargingError::Invalid("Actor run not found".to_owned()))?;
            let max = run
                .extra
                .get("options")
                .and_then(|options| options.get("maxTotalChargeUsd"))
                .and_then(Value::as_f64)
                .filter(|&max| max != 0.0)
                .unwrap_or(f64::INFINITY);
            let pricing = run.extra.get("pricingInfo").cloned().unwrap_or(Value::Null);
            return Ok((pricing, counts(run.extra.get("chargedEventCounts")), max));
        }
        Ok((Value::Null, IndexMap::new(), configuration.max_total_charge_usd))
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
    /// reports the count of the original charge, or 0 when that was of another event.
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
        if !self.is_initialized() {
            return Err(ChargingError::NotInitialized);
        }
        let ChargeOptions { event_name, count, idempotency_key } = options;
        let charged_count = {
            let mut state = self.state.lock();
            if let Some(key) = &idempotency_key
                && let Some(previous) = state.idempotent_charge(key)
            {
                // A repeat the platform would discard: counting it would spend the budget early.
                let repeated_count = if previous.event_name == event_name {
                    tracing::debug!("Skipped a repeated charge of event '{event_name}' under key '{key}'.");
                    previous.charged_count
                } else {
                    tracing::warn!(
                        "Idempotency key '{key}' was already used to charge for event '{}', so the charge of event \
                         '{event_name}' is skipped.",
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
            // Remembered right away, so that a concurrent repeat is skipped too.
            if let Some(key) = &idempotency_key {
                state.remember_idempotent_charge(key.clone(), &event_name, charged_count);
            }
            charged_count
        };
        if charged_count > 0
            && let Err(err) = self.send_charge(&event_name, charged_count, idempotency_key.clone()).await
        {
            // Counted only once made, so that a failed charge retried under its key is counted once.
            let mut state = self.state.lock();
            self.add_charged(&mut state, &event_name, -(charged_count as i64));
            if let Some(key) = &idempotency_key {
                state.idempotent.shift_remove(key);
            }
            return Err(err);
        }
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
    /// one of every event of `events`, plus the default dataset item event if
    /// `is_default_dataset`. Returns how many items to store. All the charges are counted at
    /// once, so that concurrent pushes cannot together spend more than the budget.
    pub(crate) fn reserve_items(&self, items_count: usize, events: &[&str], is_default_dataset: bool) -> usize {
        let mut state = self.state.lock();
        let limit = self.push_data_limit_of(&state, items_count, events, is_default_dataset);
        if state.pricing_model.as_deref() == Some(PAY_PER_EVENT) {
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
        if state.pricing_model.as_deref() == Some(PAY_PER_EVENT) {
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
        if !self.is_pay_per_event() || items_count == 0 {
            return Ok(());
        }
        for event in Self::item_events(events, is_default_dataset) {
            self.send_charge(event, items_count as u64, None).await?;
        }
        Ok(())
    }

    fn item_events<'a>(events: &'a [&'a str], is_default_dataset: bool) -> impl Iterator<Item = &'a str> {
        events.iter().copied().chain(is_default_dataset.then_some(DEFAULT_DATASET_ITEM_EVENT))
    }

    /// The charges [`reserve_items`](Self::reserve_items) reserved for `items_count` items.
    fn item_counters(events: &[&str], is_default_dataset: bool, items_count: usize) -> FlushCounters {
        let mut counters = FlushCounters::new();
        for event in Self::item_events(events, is_default_dataset) {
            counters.add(event, items_count as u64);
        }
        counters
    }

    /// Takes back reserved charges whose items were not stored.
    fn unreserve(&self, counters: &FlushCounters) {
        let mut state = self.state.lock();
        if state.pricing_model.as_deref() == Some(PAY_PER_EVENT) {
            for (event, count) in counters.iter() {
                self.add_charged(&mut state, event, -(count as i64));
            }
        }
    }

    /// Sends reserved charges, one call per event.
    async fn send_reserved(&self, counters: &FlushCounters) -> Result<(), ChargingError> {
        if !self.is_pay_per_event() {
            return Ok(());
        }
        for (event, count) in counters.iter() {
            self.send_charge(event, count, None).await?;
        }
        Ok(())
    }

    /// Stores items with `store`, whose charges (`reserved`) were reserved already: the default
    /// dataset does not charge them again. Returns the charges unless a buffered dataset took
    /// them over, which then sends them when it stores the items, or gives them back.
    pub(crate) async fn with_items_reserved<F: Future>(
        reserved: FlushCounters,
        store: F,
    ) -> (F::Output, Option<FlushCounters>) {
        let reserved = Arc::new(Mutex::new(Some(reserved)));
        let output = ITEMS_RESERVED.scope(reserved.clone(), store).await;
        let left = reserved.lock().take();
        (output, left)
    }

    /// Stores as many of `items` in `dataset` as the budget allows for `events` (plus the
    /// default dataset item event if `is_default_dataset`), reserving their charges first and
    /// sending them after (or, for a buffered dataset, with the flush that stores them), so that
    /// concurrent pushes neither wait for each other nor spend more than the budget together.
    pub(crate) async fn push_reserving_charges(
        &self,
        dataset: &Dataset,
        mut items: Vec<DatasetItem>,
        events: &[&str],
        is_default_dataset: bool,
    ) -> Result<ChargeResult, ChargingError> {
        let limit = self.reserve_items(items.len(), events, is_default_dataset);
        if limit == 0 {
            return Ok(ChargeResult {
                event_charge_limit_reached: !items.is_empty(),
                charged_count: 0,
                chargeable_within_limit: self.chargeable_within_limit(),
            });
        }
        items.truncate(limit);
        let reserved = Self::item_counters(events, is_default_dataset, limit);
        let (stored, left) = Self::with_items_reserved(reserved, dataset.push_items(items)).await;
        match (stored, left) {
            (Err(err), Some(reserved)) => {
                self.unreserve(&reserved);
                return Err(err.into());
            }
            // A buffered dataset gave the charges back already.
            (Err(err), None) => return Err(err.into()),
            (Ok(()), Some(reserved)) => self.send_reserved(&reserved).await?,
            // A buffered dataset sends them when it stores the items.
            (Ok(()), None) => {}
        }
        let reached = if events.is_empty() {
            self.is_event_charge_limit_reached(DEFAULT_DATASET_ITEM_EVENT)
        } else {
            events.iter().any(|event| self.is_event_charge_limit_reached(event))
        };
        Ok(ChargeResult {
            event_charge_limit_reached: reached,
            // Without the default dataset item event, `push_data` charges nothing.
            charged_count: if events.is_empty() && !is_default_dataset { 0 } else { limit as u64 },
            chargeable_within_limit: self.chargeable_within_limit(),
        })
    }

    /// Sends a charge counted already: to the platform (unless synthetic, charged by the platform
    /// itself), and to the local charging log.
    async fn send_charge(
        &self,
        event_name: &str,
        count: u64,
        idempotency_key: Option<String>,
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
        if items.is_empty()
            || !charging.is_initialized()
            || !charging.is_pay_per_event()
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

    /// Items a buffered dataset takes: their charges are reserved now, so that the budget is
    /// enforced when they are pushed, and sent with the flush that stores them. The Actor's own
    /// pushes reserved theirs already, and hand them over.
    fn admit_buffered(&self, items: &mut Vec<DatasetItem>) -> FlushCounters {
        if let Ok(reserved) = ITEMS_RESERVED.try_with(|reserved| reserved.lock().take()) {
            return reserved.unwrap_or_default();
        }
        let charging = &self.charging;
        if items.is_empty() || !charging.is_initialized() || !charging.is_pay_per_event() {
            return FlushCounters::new();
        }
        let limit = charging.reserve_items(items.len(), &[], true);
        items.truncate(limit);
        ChargingManager::item_counters(&[], true, limit)
    }

    /// Stores the items of a flush and sends their charges, one call per event; gives the charges
    /// back when the items could not be stored. A charge that cannot be sent is logged: the items
    /// are stored, and failing the flush would store them again.
    async fn push_buffered(&self, items: Vec<DatasetItem>, counters: FlushCounters) -> StorageResult<()> {
        if let Err(err) = self.inner.push_data(items).await {
            self.charging.unreserve(&counters);
            return Err(err);
        }
        if let Err(err) = self.charging.send_reserved(&counters).await {
            tracing::error!("Charging for stored dataset items failed: {err}");
        }
        Ok(())
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

    #[test]
    fn to_fixed_rounds_like_javascript() {
        // Values checked with node: Number(x.toFixed(n)).
        assert_eq!(js_to_fixed(0.25, 1), 0.3, "a tie rounds up, not to even");
        assert_eq!(js_to_fixed(1.005, 2), 1.0, "1.005 is below 1.005 in binary");
        assert_eq!(js_to_fixed(4.99999999999, 4), 5.0);
        assert_eq!(js_to_fixed(-2.5, 0), -3.0);
        assert_eq!(js_to_fixed(0.1 + 0.2, 6), 0.3);
        assert_eq!(js_to_fixed(9.9999995, 6), 9.999999);
        assert_eq!(js_to_fixed(f64::INFINITY, 4), f64::INFINITY);
    }

    #[test]
    fn to_fixed_golden() {
        let cases: Vec<Value> = serde_json::from_str(include_str!("../conformance/golden/to_fixed.json")).unwrap();
        for case in cases {
            let (x, digits) = (case["x"].as_f64().unwrap(), case["digits"].as_u64().unwrap() as usize);
            assert_eq!(js_to_fixed(x, digits), case["expected"].as_f64().unwrap(), "{x}.toFixed({digits})");
        }
    }

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

    /// A local pay-per-event manager whose charges go to a charging log in `storage`; every event
    /// costs 1 USD.
    async fn local_manager(max: f64, storage: &crawlee::core::MemoryStorageBackend) -> Arc<ChargingManager> {
        let configuration = Configuration {
            test_pay_per_event: true,
            use_charging_log_dataset: true,
            max_total_charge_usd: max,
            ..Configuration::default()
        };
        let client = crate::new_client(&configuration, None, Default::default());
        let manager = Arc::new(ChargingManager::new(Arc::new(configuration), client));
        manager.init(storage).await.unwrap();
        manager
    }

    /// The charges sent, from the charging log: (event, count).
    async fn charges_sent(storage: &crawlee::core::MemoryStorageBackend) -> Vec<(String, u64)> {
        let log = Dataset::open(storage, &StorageIdentifier::name(LOCAL_CHARGING_LOG_DATASET_NAME)).await.unwrap();
        let entries: Vec<Value> = log.get_all().await.unwrap();
        entries
            .iter()
            .map(|entry| (entry["eventName"].as_str().unwrap().to_owned(), entry["chargedCount"].as_u64().unwrap()))
            .collect()
    }

    fn lingering() -> crawlee::core::DatasetBufferOptions {
        crawlee::core::DatasetBufferOptions::new()
            .with_stages([crawlee::core::BufferStage::new(0, Duration::from_secs(3600))])
    }

    fn items(count: usize) -> Vec<DatasetItem> {
        (0..count).map(|n| serde_json::value::to_raw_value(&serde_json::json!({ "n": n })).unwrap()).collect()
    }

    /// The default dataset of an Actor, on `storage`, charging through `charging`.
    async fn default_dataset(
        storage: &Arc<crawlee::core::MemoryStorageBackend>,
        charging: &Arc<ChargingManager>,
    ) -> Dataset {
        let charged = ChargingStorageBackend {
            inner: storage.clone(),
            charging: charging.clone(),
            default_dataset_id: "default".to_owned(),
        };
        Dataset::open(&charged, &StorageIdentifier::Default).await.unwrap()
    }

    #[tokio::test]
    async fn buffered_items_are_charged_once_per_flush() {
        let storage = Arc::new(crawlee::core::MemoryStorageBackend::new());
        let charging = local_manager(100.0, &storage).await;
        let dataset = default_dataset(&storage, &charging).await.with_buffer(lingering(), None);

        for _ in 0..3 {
            let result = charging.push_reserving_charges(&dataset, items(2), &["page"], true).await.unwrap();
            assert_eq!(result.charged_count, 2);
        }
        // Pushed without reserving, as a crawler's `ctx.push_data` does: reserved when buffered.
        dataset.push_items(items(1)).await.unwrap();
        // Counted against the budget at once, sent with the flush.
        assert_eq!(charging.charged_event_count("page"), 6);
        assert_eq!(charging.charged_event_count(DEFAULT_DATASET_ITEM_EVENT), 7);
        assert!(charges_sent(&storage).await.is_empty());

        dataset.flush().await.unwrap();
        let sent = charges_sent(&storage).await;
        assert_eq!(sent, [("page".to_owned(), 6), (DEFAULT_DATASET_ITEM_EVENT.to_owned(), 7)]);
        assert_eq!(dataset.get_info().await.unwrap().item_count, 7);
    }

    #[tokio::test]
    async fn the_budget_is_enforced_when_items_are_pushed() {
        // Each item costs 2 USD locally: `page` and the default dataset item.
        let storage = Arc::new(crawlee::core::MemoryStorageBackend::new());
        let charging = local_manager(9.0, &storage).await;
        let buffered = default_dataset(&storage, &charging).await.with_buffer(lingering(), None);
        let result = charging.push_reserving_charges(&buffered, items(3), &["page"], true).await.unwrap();
        assert_eq!((result.charged_count, result.event_charge_limit_reached), (3, false));
        let result = charging.push_reserving_charges(&buffered, items(3), &["page"], true).await.unwrap();
        assert_eq!((result.charged_count, result.event_charge_limit_reached), (1, false), "3 USD buy 1 more");
        let result = charging.push_reserving_charges(&buffered, items(3), &["page"], true).await.unwrap();
        assert_eq!((result.charged_count, result.event_charge_limit_reached), (1, true), "one over, to stop the run");
        let result = charging.push_reserving_charges(&buffered, items(3), &["page"], true).await.unwrap();
        assert_eq!(result.charged_count, 0);
        // A crawler's push over the budget is cut too.
        buffered.push_items(items(2)).await.unwrap();
        buffered.flush().await.unwrap();
        assert_eq!(buffered.get_info().await.unwrap().item_count, 5);
        assert_eq!(charges_sent(&storage).await, [("page".to_owned(), 5), (DEFAULT_DATASET_ITEM_EVENT.to_owned(), 5)]);
    }

    /// Fails every push.
    struct DownDataset;

    #[async_trait]
    impl DatasetBackend for DownDataset {
        async fn get_metadata(&self) -> StorageResult<DatasetInfo> {
            unimplemented!()
        }
        async fn drop_storage(&self) -> StorageResult<()> {
            Ok(())
        }
        async fn purge(&self) -> StorageResult<()> {
            Ok(())
        }
        async fn push_data(&self, _items: Vec<DatasetItem>) -> StorageResult<()> {
            Err(StorageError::Backend("the API is down".into()))
        }
        async fn get_data(&self, _options: DatasetListOptions) -> StorageResult<PaginatedList<DatasetItem>> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn a_failed_flush_gives_its_charges_back() {
        let storage = crawlee::core::MemoryStorageBackend::new();
        let charging = local_manager(100.0, &storage).await;
        let backend = Arc::new(ChargingDataset { inner: Arc::new(DownDataset), charging: charging.clone() });
        let dataset = Dataset::from_backend(backend).with_buffer(lingering(), None);
        charging.push_reserving_charges(&dataset, items(3), &["page"], true).await.unwrap();
        dataset.push_items(items(1)).await.unwrap();
        assert_eq!(charging.charged_event_count("page"), 3);
        assert_eq!(charging.charged_event_count(DEFAULT_DATASET_ITEM_EVENT), 4);

        assert!(dataset.flush().await.is_err());
        assert_eq!(charging.charged_event_count("page"), 0);
        assert_eq!(charging.charged_event_count(DEFAULT_DATASET_ITEM_EVENT), 0);
        assert!(charges_sent(&storage).await.is_empty());

        // Unbuffered, a failed push gives them back as before.
        let unbuffered = Dataset::from_backend(Arc::new(ChargingDataset {
            inner: Arc::new(DownDataset),
            charging: charging.clone(),
        }));
        assert!(charging.push_reserving_charges(&unbuffered, items(2), &["page"], true).await.is_err());
        assert_eq!(charging.charged_event_count("page"), 0);
    }

    #[tokio::test]
    async fn unbuffered_items_are_charged_once_per_push() {
        let storage = Arc::new(crawlee::core::MemoryStorageBackend::new());
        let charging = local_manager(100.0, &storage).await;
        let dataset = default_dataset(&storage, &charging).await;
        charging.push_reserving_charges(&dataset, items(2), &["page"], true).await.unwrap();
        charging.push_reserving_charges(&dataset, items(1), &["page"], true).await.unwrap();
        assert_eq!(
            charges_sent(&storage).await,
            [
                ("page".to_owned(), 2),
                (DEFAULT_DATASET_ITEM_EVENT.to_owned(), 2),
                ("page".to_owned(), 1),
                (DEFAULT_DATASET_ITEM_EVENT.to_owned(), 1),
            ]
        );
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
}
