//! A dataset on the Apify platform.

use apify_client::DatasetListItemsOptions;
use apify_client::clients::dataset::DatasetClient;
use async_trait::async_trait;
use crawlee::core::StorageResult;
use crawlee::core::errors::StorageError;
use crawlee::core::storage::backend::{DatasetBackend, DatasetInfo, DatasetItem, DatasetListOptions, PaginatedList};
use serde_json::value::RawValue;

use super::{backend_error, date, extra_date, unsupported};

/// The API rejects payloads over 9 MB (`MAX_PAYLOAD_SIZE_BYTES` in `@apify/consts`).
const MAX_PAYLOAD_SIZE_BYTES: usize = 9_437_184;
/// The payload limit minus a 0.01% safety buffer, as in the JS SDK.
const EFFECTIVE_LIMIT_BYTES: usize = MAX_PAYLOAD_SIZE_BYTES - MAX_PAYLOAD_SIZE_BYTES.div_ceil(10_000);
/// 2 bytes under the chunk limit, so that even a lone item fits its `[]` wrapper.
const MAX_ITEM_BYTES: usize = EFFECTIVE_LIMIT_BYTES - 2;

pub(crate) struct ApifyDataset {
    pub(crate) client: DatasetClient,
}

/// Groups serialized items into JSON arrays of at most `limit` bytes, keeping their order.
/// Assumes that no single item exceeds the limit.
fn chunk_by_size(items: &[DatasetItem], limit: usize) -> Vec<String> {
    let mut chunks: Vec<String> = Vec::new();
    for item in items {
        let item = item.get();
        match chunks.last_mut() {
            // +1 for the ',' separator, +1 for the closing ']'.
            Some(chunk) if chunk.len() + item.len() + 2 <= limit => {
                chunk.push(',');
                chunk.push_str(item);
            }
            _ => {
                let mut chunk = String::with_capacity(item.len() + 2);
                chunk.push('[');
                chunk.push_str(item);
                chunks.push(chunk);
            }
        }
    }
    for chunk in &mut chunks {
        chunk.push(']');
    }
    chunks
}

#[async_trait(?Send)]
impl DatasetBackend for ApifyDataset {
    async fn get_metadata(&self) -> StorageResult<DatasetInfo> {
        let dataset = self
            .client
            .get()
            .await
            .map_err(backend_error)?
            .ok_or_else(|| StorageError::NotFound("Dataset not found or has been deleted.".to_owned()))?;
        Ok(DatasetInfo {
            created_at: date(dataset.created_at),
            modified_at: date(dataset.modified_at),
            accessed_at: extra_date(&dataset.extra, "accessedAt"),
            item_count: dataset.item_count.unwrap_or(0).max(0) as u64,
            id: dataset.id,
            name: dataset.name,
        })
    }

    async fn drop_storage(&self) -> StorageResult<()> {
        self.client.delete().await.map_err(backend_error)
    }

    async fn purge(&self) -> StorageResult<()> {
        Err(unsupported(
            "Purging a dataset is not supported on the Apify platform. \
             Use `drop()` to delete the dataset entirely, or open a new dataset instead.",
        ))
    }

    async fn push_data(&self, items: Vec<DatasetItem>) -> StorageResult<()> {
        for (index, item) in items.iter().enumerate() {
            let bytes = item.get().len();
            if bytes > MAX_ITEM_BYTES {
                return Err(StorageError::InvalidArgument(format!(
                    "Data item at index {index} is too large (size: {bytes} bytes, limit: {MAX_ITEM_BYTES} bytes)"
                )));
            }
        }
        // Pushed one after another, to keep the order of the items.
        for chunk in chunk_by_size(&items, EFFECTIVE_LIMIT_BYTES) {
            let payload = RawValue::from_string(chunk)?;
            self.client.push_items(&payload).await.map_err(backend_error)?;
        }
        Ok(())
    }

    async fn get_data(&self, options: DatasetListOptions) -> StorageResult<PaginatedList<DatasetItem>> {
        let list = self
            .client
            .list_items::<Box<RawValue>>(DatasetListItemsOptions {
                offset: Some(options.offset as i64),
                limit: options.limit.map(|limit| limit as i64),
                desc: options.desc.then_some(true),
                ..DatasetListItemsOptions::default()
            })
            .await
            .map_err(backend_error)?;
        Ok(PaginatedList {
            total: list.total.max(0) as usize,
            count: list.count.max(0) as usize,
            offset: list.offset.max(0) as usize,
            limit: options.limit,
            desc: options.desc,
            items: list.items,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn items(texts: &[&str]) -> Vec<DatasetItem> {
        texts.iter().map(|text| RawValue::from_string((*text).to_owned()).unwrap()).collect()
    }

    #[test]
    fn items_are_chunked_by_size_in_order() {
        let items = items(&[r#"{"a":1}"#, r#"{"b":2}"#, r#"{"c":3}"#]);
        assert_eq!(chunk_by_size(&items, 1000), [r#"[{"a":1},{"b":2},{"c":3}]"#]);
        // `[{"a":1},{"b":2}]` is 17 bytes.
        assert_eq!(chunk_by_size(&items, 17), [r#"[{"a":1},{"b":2}]"#, r#"[{"c":3}]"#]);
        assert_eq!(chunk_by_size(&items, 16), [r#"[{"a":1}]"#, r#"[{"b":2}]"#, r#"[{"c":3}]"#]);
        assert!(chunk_by_size(&[], 10).is_empty());
    }

    #[test]
    fn limits_match_the_js_sdk() {
        assert_eq!(EFFECTIVE_LIMIT_BYTES, 9_436_240);
        assert_eq!(MAX_ITEM_BYTES, 9_436_238);
    }
}
