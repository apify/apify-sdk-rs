//! Pushes to a dataset with the rejections of its schema handled, for
//! [`Actor::push_data_with`](crate::Actor::push_data_with) and the buffered pushes alike.
//!
//! When a dataset's schema rejects items, the platform stores none of the request's items. The
//! rejected ones are handled as their push asks ([`OnInvalidItems`]: given back to it, repaired
//! or dropped), and the others are pushed again.

use std::collections::BTreeSet;

use apify_client::ApifyClientError;
use crawlee::Dataset;
use crawlee::core::StorageError;
use crawlee::core::storage::backend::DatasetItem;
use serde_json::Value;

use crate::charging::{ChargeResult, ChargingManager};
use crate::push::{InvalidItem, OnInvalidItems};
use crate::schema_repair::{RepairedItem, field_issue, format_fields};

/// How many times the items are pushed again after the dataset's schema rejected some. Repairs
/// take up to three rounds (a missing field: a placeholder, then its type); the last push is only
/// for the items that passed, without the ones still failing.
const MAX_VALIDATION_ROUNDS: usize = 5;

/// The error type of the platform when a dataset's schema rejects items.
const SCHEMA_VALIDATION_ERROR: &str = "schema-validation-error";

/// An item to store.
pub(crate) struct Entry {
    /// The push it comes from, and its index there.
    push: usize,
    index: usize,
    item: DatasetItem,
    /// The item as pushed, once it is repaired.
    original: Option<DatasetItem>,
    repair: Option<RepairedItem>,
}

impl Entry {
    pub(crate) fn new(push: usize, index: usize, item: DatasetItem) -> Self {
        Entry { push, index, item, original: None, repair: None }
    }
}

/// The items of a push that were not stored.
#[derive(Default)]
pub(crate) struct Rejected {
    /// The items given back to the push.
    pub(crate) invalid: Vec<InvalidItem>,
    /// How many were dropped.
    pub(crate) dropped: usize,
}

impl Rejected {
    pub(crate) fn count(&self) -> usize {
        self.invalid.len() + self.dropped
    }
}

/// The charges of a push's stored items: those reserved, without the `rejected` items.
pub(crate) fn charge_of_stored(reserved: &ChargeResult, rejected: usize, charging: &ChargingManager) -> ChargeResult {
    ChargeResult {
        event_charge_limit_reached: reserved.event_charge_limit_reached,
        charged_count: reserved.charged_count.saturating_sub(if reserved.charged_count > 0 {
            rejected as u64
        } else {
            0
        }),
        chargeable_within_limit: charging.chargeable_within_limit(),
    }
}

/// Pushes the entries of one or more pushes, whose charges are reserved, in order. The ones the
/// dataset's schema rejects are repaired, when their push asks for it (`policies`, by push), or
/// are given back or dropped; the others are pushed again. What was rejected is returned by push,
/// the items given back sorted, also when storing failed.
pub(crate) async fn store_validated(
    dataset: &Dataset,
    mut entries: Vec<Entry>,
    policies: &[OnInvalidItems],
) -> (Result<(), StorageError>, Vec<Rejected>) {
    let mut rejected: Vec<Rejected> = policies.iter().map(|_| Rejected::default()).collect();
    let stored = store_rounds(dataset, &mut entries, &mut rejected, policies).await;
    for push in &mut rejected {
        push.invalid.sort_by_key(|item| item.index);
    }
    (stored, rejected)
}

async fn store_rounds(
    dataset: &Dataset,
    entries: &mut Vec<Entry>,
    rejected_by_push: &mut [Rejected],
    policies: &[OnInvalidItems],
) -> Result<(), StorageError> {
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
            let entry = entries.remove(position);
            if policy == OnInvalidItems::Drop {
                rejected_by_push[entry.push].dropped += 1;
                dropped += 1;
                dropped_fields.extend(validation_errors.iter().map(field_issue));
                continue;
            }
            let item = entry.original.unwrap_or(entry.item);
            rejected_by_push[entry.push].invalid.push(InvalidItem { index: entry.index, item, validation_errors });
        }
        if repaired > 0 {
            tracing::warn!(
                "The dataset's schema rejected {repaired} item(s), pushing them again repaired; fields: {}",
                format_fields(&repaired_fields)
            );
        }
        if dropped > 0 {
            tracing::warn!(
                "The dataset's schema rejected {dropped} item(s), dropping them; fields: {}",
                format_fields(&dropped_fields)
            );
        }
    }
    Err(StorageError::Backend(
        format!("The dataset's schema still rejected items after {MAX_VALIDATION_ROUNDS} rounds").into(),
    ))
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
