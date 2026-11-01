//! Buffered pushes to the default dataset ([`Actor::push_data_buffered`](crate::Actor::push_data_buffered)):
//! the items pushed within [`InitOptions::push_buffer_window`](crate::InitOptions::push_buffer_window)
//! of each other are stored with one request, and charged with one charge call per event.
//!
//! Each push gets a [`BufferedPush`], which can be awaited (or not) for its items to be stored.
//! When the dataset's schema rejects items, the platform stores none of the request's items: the
//! rejected ones are handled as their push asks ([`OnInvalidItems`]: given back to it as
//! [`PushError::Invalid`] by default, repaired or dropped), and the others are pushed again.

use std::collections::BTreeSet;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use apify_client::ApifyClientError;
use crawlee::core::StorageError;
use crawlee::core::storage::backend::DatasetItem;
use crawlee::{Services, StorageIdentifier};
use indexmap::IndexMap;
use parking_lot::Mutex;
use serde_json::Value;
use tokio::sync::oneshot;

use crate::charging::{ChargeResult, ChargingError, ChargingManager, DEFAULT_DATASET_ITEM_EVENT};
use crate::schema_repair::{RepairedItem, field_issue, format_fields};

/// The most bytes of items in one request, below the platform's payload limit.
const MAX_BATCH_BYTES: usize = 9_000_000;

/// How many times a batch is pushed again after the dataset's schema rejected items. Repairs take
/// up to three rounds (a missing field: a placeholder, then its type); the last push is only for
/// the items that passed, without the ones still failing.
const MAX_VALIDATION_ROUNDS: usize = 5;

/// The error type of the platform when a dataset's schema rejects items.
const SCHEMA_VALIDATION_ERROR: &str = "schema-validation-error";

/// A push of [`Actor::push_data_buffered`](crate::Actor::push_data_buffered): its charges, reserved at
/// once, and (when awaited) whether its items were stored.
#[must_use = "dropping it leaves the items to be stored in the background, unawaited"]
pub struct BufferedPush {
    charge: ChargeResult,
    stored: Option<oneshot::Receiver<Result<(), PushError>>>,
}

impl BufferedPush {
    pub(crate) fn new(charge: ChargeResult, stored: Option<oneshot::Receiver<Result<(), PushError>>>) -> Self {
        BufferedPush { charge, stored }
    }

    /// The charges of the push, as [`Actor::push_data`](crate::Actor::push_data) returns them: the
    /// items over the budget were left out. Items the dataset's schema rejects are not charged.
    pub fn charge_result(&self) -> &ChargeResult {
        &self.charge
    }
}

impl IntoFuture for BufferedPush {
    type Output = Result<(), PushError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    /// Waits until the items are stored (or not).
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let Some(stored) = self.stored else { return Ok(()) };
            stored.await.unwrap_or_else(|_| {
                Err(PushError::Failed(Arc::new(StorageError::Backend(
                    "The items were dropped before they were stored".into(),
                ))))
            })
        })
    }
}

/// Why the items of a [`BufferedPush`] were not stored.
#[derive(Debug, thiserror::Error)]
pub enum PushError {
    /// The dataset's schema rejected these items of the push; its other items were stored.
    #[error("{} pushed item(s) failed the validation of the dataset's schema", .0.len())]
    Invalid(Vec<InvalidItem>),
    /// None of the items of the push were stored.
    #[error("Storing the pushed items failed: {0}")]
    Failed(Arc<StorageError>),
    /// The items were stored, but charging for them failed.
    #[error("Charging for the pushed items failed: {0}")]
    Charge(Arc<ChargingError>),
}

/// What happens to the items of a push that the dataset's schema rejects. The other items are
/// stored either way.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OnInvalidItems {
    /// They come back to the push as [`PushError::Invalid`].
    #[default]
    Return,
    /// They are repaired and pushed again, as `pushDataWithSchemaRepair` of `@apify/actor-utils`
    /// repairs them: a field that fails the schema is removed, a missing required field gets an
    /// empty placeholder (`""`, `[]`, `{}` or `null`), and a property the schema does not allow
    /// is removed. No other value is made up. The repairs are logged; the items that cannot be
    /// repaired come back as [`PushError::Invalid`], with the errors that kept them from it.
    Repair,
    /// They are dropped, which is logged; the push succeeds.
    Drop,
}

/// Options of [`Actor::push_data_buffered_with`](crate::Actor::push_data_buffered_with).
#[derive(Clone, Debug, Default)]
pub struct BufferedPushOptions {
    /// The events to charge for each item, as
    /// [`Actor::push_data_and_charge_events`](crate::Actor::push_data_and_charge_events) charges
    /// them.
    pub charge_events: Vec<String>,
    pub on_invalid_items: OnInvalidItems,
}

/// An item the dataset's schema rejected.
#[derive(Debug)]
pub struct InvalidItem {
    /// Its index in the items of the push.
    pub index: usize,
    /// The item, as it was pushed.
    pub item: DatasetItem,
    /// Why, as the platform reports it: `instancePath`, `keyword`, `params`, `message`, ...
    /// For an item that could not be repaired, the errors that kept it from it.
    pub validation_errors: Vec<Value>,
}

/// An item of a batch being stored.
struct Entry {
    /// The push it comes from, and its index there.
    push: usize,
    index: usize,
    item: DatasetItem,
    /// The item as pushed, once it is repaired.
    original: Option<DatasetItem>,
    repair: Option<RepairedItem>,
}

/// Per event, how many items of a batch were stored, and the pushes that charge it.
type ChargeTotals<'a> = IndexMap<&'a str, (usize, Vec<usize>)>;

/// One push in a batch.
struct Push {
    len: usize,
    events: Vec<String>,
    is_default_dataset: bool,
    on_invalid_items: OnInvalidItems,
    done: oneshot::Sender<Result<(), PushError>>,
}

/// The items buffered, and the pushes they come from, in order.
#[derive(Default)]
struct Batch {
    items: Vec<DatasetItem>,
    bytes: usize,
    pushes: Vec<Push>,
}

/// The buffer of the default dataset's pushes.
pub(crate) struct PushBuffer {
    window: Duration,
    services: Services,
    charging: Arc<ChargingManager>,
    pending: Mutex<Batch>,
    /// Held while a batch is stored: batches are stored one at a time, in order.
    storing: tokio::sync::Mutex<()>,
}

impl PushBuffer {
    pub(crate) fn new(window: Duration, services: Services, charging: Arc<ChargingManager>) -> Arc<Self> {
        Arc::new(PushBuffer {
            window,
            services,
            charging,
            pending: Mutex::default(),
            storing: tokio::sync::Mutex::new(()),
        })
    }

    /// Buffers the items of a push whose charges are reserved: stored `window` after the first
    /// item of the batch, or sooner when the batch is full.
    pub(crate) fn add(
        self: &Arc<Self>,
        items: Vec<DatasetItem>,
        events: Vec<String>,
        is_default_dataset: bool,
        on_invalid_items: OnInvalidItems,
    ) -> oneshot::Receiver<Result<(), PushError>> {
        let (done, stored) = oneshot::channel();
        let bytes: usize = items.iter().map(|item| item.get().len() + 1).sum();
        let mut pending = self.pending.lock();
        if !pending.items.is_empty() && pending.bytes + bytes > MAX_BATCH_BYTES {
            let full = std::mem::take(&mut *pending);
            let this = self.clone();
            tokio::spawn(async move { this.store(full).await });
        }
        let first = pending.pushes.is_empty();
        pending.bytes += bytes;
        pending.pushes.push(Push { len: items.len(), events, is_default_dataset, on_invalid_items, done });
        pending.items.extend(items);
        drop(pending);
        if first {
            let this = self.clone();
            tokio::spawn(async move {
                tokio::time::sleep(this.window).await;
                this.flush().await;
            });
        }
        stored
    }

    /// Stores the items buffered now, after the batches being stored.
    pub(crate) async fn flush(&self) {
        let batch = std::mem::take(&mut *self.pending.lock());
        self.store(batch).await;
    }

    async fn store(&self, batch: Batch) {
        let _storing = self.storing.lock().await;
        if batch.pushes.is_empty() {
            return;
        }
        let Batch { items, pushes, .. } = batch;
        // The items still to store, with the push each comes from and its index there.
        let owners = pushes.iter().enumerate().flat_map(|(push, p)| (0..p.len).map(move |index| (push, index)));
        let mut entries: Vec<Entry> = owners
            .zip(items)
            .map(|((push, index), item)| Entry { push, index, item, original: None, repair: None })
            .collect();
        let mut invalid: Vec<Vec<InvalidItem>> = pushes.iter().map(|_| Vec::new()).collect();
        let policies: Vec<OnInvalidItems> = pushes.iter().map(|push| push.on_invalid_items).collect();
        let failed = self.store_entries(&mut entries, &mut invalid, &policies).await.err().map(Arc::new);

        // Reservations of the items not stored are taken back; the stored ones are charged with
        // one call per event, whatever the events of each push.
        let mut totals = ChargeTotals::new();
        for (index, push) in pushes.iter().enumerate() {
            let stored = if failed.is_some() { 0 } else { push.len - invalid[index].len() };
            let events: Vec<&str> = push.events.iter().map(String::as_str).collect();
            self.charging.unreserve_items(&events, push.is_default_dataset, push.len - stored);
            let default_item = push.is_default_dataset.then_some(DEFAULT_DATASET_ITEM_EVENT);
            for event in events.into_iter().chain(default_item) {
                let total = totals.entry(event).or_default();
                total.0 += stored;
                total.1.push(index);
            }
        }
        let mut charge_failed: Vec<Option<Arc<ChargingError>>> = pushes.iter().map(|_| None).collect();
        for (event, (stored, members)) in totals {
            // The default dataset item event goes as it is reserved, with the items' other events.
            let charged = if event == DEFAULT_DATASET_ITEM_EVENT {
                self.charging.send_item_charges(&[], true, stored).await
            } else {
                self.charging.send_item_charges(&[event], false, stored).await
            };
            if let Err(err) = charged {
                let err = Arc::new(err);
                for index in members {
                    charge_failed[index] = Some(err.clone());
                }
            }
        }

        for (index, push) in pushes.into_iter().enumerate() {
            let mut rejected = std::mem::take(&mut invalid[index]);
            rejected.sort_by_key(|item| item.index);
            let result = match (&failed, &charge_failed[index]) {
                (Some(err), _) => Err(PushError::Failed(err.clone())),
                (None, Some(err)) => Err(PushError::Charge(err.clone())),
                // Dropped items were logged.
                (None, None) if !rejected.is_empty() && push.on_invalid_items != OnInvalidItems::Drop => {
                    Err(PushError::Invalid(rejected))
                }
                (None, None) => Ok(()),
            };
            if let Err(err) = &result
                && push.done.is_closed()
            {
                // Nobody waits for the push.
                tracing::error!("A buffered push to the dataset failed: {err}");
            }
            let _ = push.done.send(result);
        }
    }

    /// Pushes the entries. The ones the dataset's schema rejects are repaired, when their push
    /// asks for it, or go to `invalid`; the others are pushed again.
    async fn store_entries(
        &self,
        entries: &mut Vec<Entry>,
        invalid: &mut [Vec<InvalidItem>],
        policies: &[OnInvalidItems],
    ) -> Result<(), StorageError> {
        let dataset = self.services.open_dataset(&StorageIdentifier::Default).await?;
        for round in 0..=MAX_VALIDATION_ROUNDS {
            if entries.is_empty() {
                return Ok(());
            }
            let items = entries.iter().map(|entry| entry.item.clone()).collect();
            let err = match ChargingManager::with_items_reserved(dataset.push_items(items)).await {
                Ok(()) => return Ok(()),
                Err(err) => err,
            };
            let Some(rejected) = schema_validation_errors(&err, entries.len()) else { return Err(err) };
            // The last push is for the items that passed: the ones still failing are not repaired.
            let may_repair = round + 1 < MAX_VALIDATION_ROUNDS;
            let (mut repaired, mut repaired_fields, mut dropped, mut dropped_fields) =
                (0, BTreeSet::new(), 0, BTreeSet::new());
            // Back to front, so that the positions of the others stay valid.
            for (position, mut validation_errors) in rejected.into_iter().rev() {
                let policy = policies[entries[position].push];
                if policy == OnInvalidItems::Repair && may_repair {
                    match repair(&mut entries[position], &validation_errors) {
                        Ok(()) => {
                            repaired += 1;
                            repaired_fields.extend(validation_errors.iter().map(field_issue));
                            continue;
                        }
                        Err(blocking) => validation_errors = blocking,
                    }
                }
                if policy != OnInvalidItems::Return {
                    dropped += 1;
                    dropped_fields.extend(validation_errors.iter().map(field_issue));
                }
                let entry = entries.remove(position);
                let item = entry.original.unwrap_or(entry.item);
                invalid[entry.push].push(InvalidItem { index: entry.index, item, validation_errors });
            }
            if repaired > 0 {
                tracing::warn!(
                    "The dataset's schema rejected {repaired} item(s), pushing them again repaired; fields: {}",
                    format_fields(&repaired_fields)
                );
            }
            if dropped > 0 {
                tracing::warn!(
                    "The dataset's schema rejected {dropped} item(s) that were not repaired, dropping them; fields: {}",
                    format_fields(&dropped_fields)
                );
            }
        }
        Err(StorageError::Backend(
            format!("The dataset's schema still rejected items after {MAX_VALIDATION_ROUNDS} rounds").into(),
        ))
    }
}

/// Repairs an entry for the errors of a rejection; `Err` holds the errors that kept it from it.
fn repair(entry: &mut Entry, validation_errors: &[Value]) -> Result<(), Vec<Value>> {
    let item = &entry.item;
    let repair =
        entry.repair.get_or_insert_with(|| RepairedItem::new(serde_json::from_str(item.get()).unwrap_or(Value::Null)));
    repair.repair(validation_errors)?;
    let repaired = serde_json::value::to_raw_value(&repair.value).map_err(|_| validation_errors.to_vec())?;
    let original = std::mem::replace(&mut entry.item, repaired);
    entry.original.get_or_insert(original);
    Ok(())
}

/// The positions (in a request of `len` items) and errors of the items a dataset's schema
/// rejected, sorted, when `err` is that rejection.
fn schema_validation_errors(err: &StorageError, len: usize) -> Option<Vec<(usize, Vec<Value>)>> {
    let StorageError::Backend(source) = err else { return None };
    let api = source.downcast_ref::<ApifyClientError>()?.as_api_error()?;
    if api.error_type.as_deref() != Some(SCHEMA_VALIDATION_ERROR) {
        return None;
    }
    let mut rejected = api
        .data
        .as_ref()?
        .get("invalidItems")?
        .as_array()?
        .iter()
        .map(|item| {
            let position = usize::try_from(item.get("itemPosition")?.as_u64()?).ok()?;
            let errors = item.get("validationErrors").and_then(Value::as_array).cloned().unwrap_or_default();
            Some((position, errors))
        })
        .collect::<Option<Vec<_>>>()?;
    rejected.sort_by_key(|(position, _)| *position);
    rejected.dedup_by_key(|(position, _)| *position);
    (!rejected.is_empty() && rejected.iter().all(|(position, _)| *position < len)).then_some(rejected)
}

#[cfg(test)]
mod tests {
    use apify_client::ApiError;

    use super::*;

    fn rejection(data: Value) -> StorageError {
        StorageError::Backend(Box::new(ApifyClientError::from(ApiError {
            status_code: 400,
            error_type: Some(SCHEMA_VALIDATION_ERROR.to_owned()),
            message: "Schema validation failed".to_owned(),
            attempt: 1,
            http_method: Some("POST".to_owned()),
            path: None,
            data: Some(data),
        })))
    }

    #[test]
    fn reads_the_rejected_items_of_a_schema_validation_error() {
        // As the platform answers (captured on a dataset with a schema).
        let err = rejection(serde_json::json!({ "invalidItems": [
            { "itemPosition": 2, "validationErrors": [{ "keyword": "type", "message": "must be integer" }] },
            { "itemPosition": 1, "validationErrors": [{ "keyword": "required", "message": "must have required property 'url'" }] },
        ]}));
        let rejected = schema_validation_errors(&err, 4).unwrap();
        assert_eq!(rejected.iter().map(|(position, _)| *position).collect::<Vec<_>>(), [1, 2]);
        assert_eq!(rejected[0].1[0]["keyword"], "required");

        // Positions outside the request, other errors: not a rejection to act on.
        assert!(schema_validation_errors(&err, 2).is_none());
        assert!(schema_validation_errors(&rejection(serde_json::json!({})), 4).is_none());
        assert!(schema_validation_errors(&StorageError::Backend("boom".into()), 4).is_none());
    }
}
