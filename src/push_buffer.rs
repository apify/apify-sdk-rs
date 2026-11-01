//! Buffered pushes to the default dataset ([`Actor::push_data_buffered`](crate::Actor::push_data_buffered)):
//! the items pushed within [`InitOptions::push_buffer_window`](crate::InitOptions::push_buffer_window)
//! of each other are stored with one request, and charged with one charge call per event.
//!
//! Each push gets a [`BufferedPush`], which can be awaited (or not) for its items to be stored.
//! The items the dataset's schema rejects are handled as the push asks, as for unbuffered pushes
//! (see [`schema_validation`](crate::schema_validation)).

use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crawlee::core::StorageError;
use crawlee::core::storage::backend::DatasetItem;
use crawlee::{Services, StorageIdentifier};
use indexmap::IndexMap;
use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::charging::{ChargeResult, ChargingError, ChargingManager, DEFAULT_DATASET_ITEM_EVENT};
use crate::schema_validation::{
    Entry, OnInvalidItems, PushError, PushResult, Rejected, charge_of_stored, store_validated,
};

/// The most bytes of items in one request, below the platform's payload limit.
const MAX_BATCH_BYTES: usize = 9_000_000;

/// What a stored push resolves to.
type Stored = Result<PushResult, PushError>;

/// A push of [`Actor::push_data_buffered`](crate::Actor::push_data_buffered): its charges, reserved at
/// once, and (when awaited) what was stored.
#[must_use = "dropping it leaves the items to be stored in the background, unawaited"]
pub struct BufferedPush {
    charge: ChargeResult,
    stored: Option<oneshot::Receiver<Stored>>,
}

impl BufferedPush {
    pub(crate) fn new(charge: ChargeResult, stored: Option<oneshot::Receiver<Stored>>) -> Self {
        BufferedPush { charge, stored }
    }

    /// The charges reserved for the push, as [`Actor::push_data`](crate::Actor::push_data) returns
    /// them: the items over the budget were left out. Items the dataset's schema rejects are not
    /// charged in the end ([`PushResult::charge`]).
    pub fn charge_result(&self) -> &ChargeResult {
        &self.charge
    }
}

impl IntoFuture for BufferedPush {
    type Output = Stored;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    /// Waits until the items are stored (or not).
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let Some(stored) = self.stored else {
                return Ok(PushResult { charge: self.charge, invalid_items: Vec::new() });
            };
            stored.await.unwrap_or_else(|_| {
                Err(PushError::Failed(Arc::new(StorageError::Backend(
                    "The items were dropped before they were stored".into(),
                ))))
            })
        })
    }
}

/// Per event, how many items of a batch were stored, and the pushes that charge it.
type ChargeTotals<'a> = IndexMap<&'a str, (usize, Vec<usize>)>;

/// One push in a batch.
struct Push {
    len: usize,
    events: Vec<String>,
    is_default_dataset: bool,
    on_invalid_items: OnInvalidItems,
    /// The charges reserved for it.
    charge: ChargeResult,
    done: oneshot::Sender<Stored>,
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
        charge: ChargeResult,
    ) -> oneshot::Receiver<Stored> {
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
        pending.pushes.push(Push { len: items.len(), events, is_default_dataset, on_invalid_items, charge, done });
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
        let entries = owners.zip(items).map(|((push, index), item)| Entry::new(push, index, item)).collect();
        let policies: Vec<OnInvalidItems> = pushes.iter().map(|push| push.on_invalid_items).collect();
        let (stored, mut rejected) = match self.services.open_dataset(&StorageIdentifier::Default).await {
            Ok(dataset) => store_validated(&dataset, entries, &policies).await,
            Err(err) => (Err(err), policies.iter().map(|_| Rejected::default()).collect()),
        };
        let failed = stored.err().map(Arc::new);

        // Reservations of the items not stored are taken back; the stored ones are charged with
        // one call per event, whatever the events of each push.
        let mut totals = ChargeTotals::new();
        for (index, push) in pushes.iter().enumerate() {
            let stored = if failed.is_some() { 0 } else { push.len - rejected[index].count() };
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
            let rejected = std::mem::take(&mut rejected[index]);
            let result = match (&failed, &charge_failed[index]) {
                (Some(err), _) => Err(PushError::Failed(err.clone())),
                (None, Some(err)) => Err(PushError::Charge(err.clone())),
                (None, None) => Ok(PushResult {
                    charge: charge_of_stored(&push.charge, rejected.count(), &self.charging),
                    invalid_items: rejected.invalid,
                }),
            };
            if push.done.is_closed() {
                // Nobody waits for the push.
                match &result {
                    Err(err) => tracing::error!("A buffered push to the dataset failed: {err}"),
                    Ok(result) if !result.invalid_items.is_empty() => tracing::error!(
                        "The dataset's schema rejected {} item(s) of a buffered push",
                        result.invalid_items.len()
                    ),
                    Ok(_) => {}
                }
            }
            let _ = push.done.send(result);
        }
    }
}
