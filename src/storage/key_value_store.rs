//! A key-value store on the Apify platform.

use apify_client::ListKeysOptions;
use apify_client::clients::key_value_store::KeyValueStoreClient;
use async_trait::async_trait;
use bytes::Bytes;
use crawlee::core::StorageResult;
use crawlee::core::errors::StorageError;
use crawlee::core::storage::backend::{
    KeyValueStoreBackend, KeyValueStoreInfo, KeyValueStoreItemData, KeyValueStoreListKeysOptions,
    KeyValueStoreListKeysResult, KeyValueStoreRecord,
};

use super::{DropHook, backend_error, date, extra_date, unsupported};

/// Content type of records stored without one.
const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

pub(crate) struct ApifyKeyValueStore {
    pub(crate) client: KeyValueStoreClient,
    pub(crate) drop_hook: DropHook,
}

#[async_trait]
impl KeyValueStoreBackend for ApifyKeyValueStore {
    async fn get_metadata(&self) -> StorageResult<KeyValueStoreInfo> {
        let store = self
            .client
            .get()
            .await
            .map_err(backend_error)?
            .ok_or_else(|| StorageError::NotFound("Key-value store not found or has been deleted.".to_owned()))?;
        Ok(KeyValueStoreInfo {
            created_at: date(store.created_at),
            modified_at: date(store.modified_at),
            accessed_at: extra_date(&store.extra, "accessedAt"),
            id: store.id,
            name: store.name,
        })
    }

    async fn drop_storage(&self) -> StorageResult<()> {
        self.client.delete().await.map_err(backend_error)?;
        self.drop_hook.dropped();
        Ok(())
    }

    async fn purge(&self) -> StorageResult<()> {
        Err(unsupported(
            "Purging a key-value store is not supported on the Apify platform. \
             Use `drop()` to delete the store entirely, or open a new store instead.",
        ))
    }

    async fn get_value(&self, key: &str) -> StorageResult<Option<KeyValueStoreRecord>> {
        let record = self.client.get_record(key).await.map_err(backend_error)?;
        Ok(record.map(|record| KeyValueStoreRecord {
            key: record.key,
            value: Bytes::from(record.value),
            content_type: record.content_type,
        }))
    }

    async fn set_value(&self, record: KeyValueStoreRecord) -> StorageResult<()> {
        let content_type = record.content_type.as_deref().unwrap_or(DEFAULT_CONTENT_TYPE);
        self.client.set_record_raw(&record.key, record.value.to_vec(), content_type).await.map_err(backend_error)
    }

    async fn delete_value(&self, key: &str) -> StorageResult<()> {
        self.client.delete_record(key).await.map_err(backend_error)
    }

    async fn list_keys(&self, options: KeyValueStoreListKeysOptions) -> StorageResult<KeyValueStoreListKeysResult> {
        let page = self
            .client
            .list_keys(ListKeysOptions {
                limit: options.limit.map(|limit| limit as i64),
                exclusive_start_key: options.exclusive_start_key,
                prefix: options.prefix,
                ..ListKeysOptions::default()
            })
            .await
            .map_err(backend_error)?;
        // The API does not report content types of listed keys (the JS SDK leaves them undefined).
        let items: Vec<KeyValueStoreItemData> = page
            .items
            .into_iter()
            .map(|item| KeyValueStoreItemData {
                content_type: item.extra.get("contentType").and_then(|v| v.as_str()).unwrap_or_default().to_owned(),
                size: item.size.unwrap_or(0).max(0) as usize,
                key: item.key,
            })
            .collect();
        Ok(KeyValueStoreListKeysResult {
            count: items.len(),
            limit: page.limit.max(0) as usize,
            exclusive_start_key: page.exclusive_start_key,
            is_truncated: page.is_truncated,
            next_exclusive_start_key: page.next_exclusive_start_key,
            items,
        })
    }

    async fn get_public_url(&self, key: &str) -> StorageResult<Option<String>> {
        Ok(Some(self.client.get_record_public_url(key).await.map_err(backend_error)?))
    }

    async fn record_exists(&self, key: &str) -> StorageResult<bool> {
        self.client.record_exists(key).await.map_err(backend_error)
    }
}
