//! The options and results of the pushes to the default dataset (see
//! [`Actor::push_data_with`](crate::Actor::push_data_with)).

use std::sync::Arc;

use crawlee::core::StorageError;
use crawlee::core::storage::backend::DatasetItem;
use serde_json::Value;

use crate::charging::{ChargeResult, ChargingError};

/// What happens to the items of a push that the dataset's schema rejects. The other items are
/// stored either way.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OnInvalidItems {
    /// They come back in [`PushResult::invalid_items`].
    #[default]
    Return,
    /// They are repaired and pushed again, as `pushDataWithSchemaRepair` of `@apify/actor-utils`
    /// repairs them: a field that fails the schema is removed, a missing required field gets an
    /// empty placeholder (`""`, `[]`, `{}` or `null`), and a property the schema does not allow
    /// is removed. No other value is made up. The repairs are logged; the items that cannot be
    /// repaired come back in [`PushResult::invalid_items`], with the errors that kept them from it.
    Repair,
    /// They are dropped, which is logged.
    Drop,
}

/// Options of [`Actor::push_data_with`](crate::Actor::push_data_with) and
/// [`Actor::push_data_buffered_with`](crate::Actor::push_data_buffered_with).
#[derive(Clone, Debug, Default)]
pub struct PushOptions {
    /// The events to charge for each item, as
    /// [`Actor::push_data_and_charge_events`](crate::Actor::push_data_and_charge_events) charges
    /// them. An event listed `n` times is charged `n` times for each item, as an add-on priced by
    /// the item's parts is.
    pub charge_events: Vec<String>,
    pub on_invalid_items: OnInvalidItems,
}

/// What a push stored and charged.
#[derive(Debug)]
pub struct PushResult {
    /// The charges of the stored items: the items over the budget were left out, and the items
    /// the dataset's schema rejected are not charged.
    pub charge: ChargeResult,
    /// The items the dataset's schema rejected that were not stored, as the push asked
    /// ([`OnInvalidItems`]). The other items were stored.
    pub invalid_items: Vec<InvalidItem>,
}

/// Why the items of a push were not stored, or not charged.
#[derive(Debug, thiserror::Error)]
pub enum PushError {
    /// None of the items of the push were stored.
    #[error("Storing the pushed items failed: {0}")]
    Failed(Arc<StorageError>),
    /// The items were not charged: the event cannot be charged, or charging for the stored items
    /// failed.
    #[error("Charging for the pushed items failed: {0}")]
    Charge(Arc<ChargingError>),
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
