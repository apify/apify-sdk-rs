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

use std::sync::Arc;

use super::prefetch::Prefetched;
use super::{DropHook, backend_error, date, extra_date, unsupported};

/// Content type of records stored without one.
const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

pub(crate) struct ApifyKeyValueStore {
    pub(crate) id: String,
    pub(crate) client: KeyValueStoreClient,
    pub(crate) drop_hook: DropHook,
    pub(crate) prefetched: Arc<PrefetchedStores>,
}

/// What is read ahead of key-value stores: their metadata by store id, and records by store id
/// and key.
#[derive(Default)]
pub(crate) struct PrefetchedStores {
    pub(crate) metadata: Prefetched<String, KeyValueStoreInfo>,
    pub(crate) records: Prefetched<(String, String), Option<KeyValueStoreRecord>>,
}

impl PrefetchedStores {
    /// Starts reading the metadata of the store of `client` (whose id is `store_id`) and its
    /// records `keys`.
    pub(crate) fn prefetch<'a>(
        &self,
        client: &KeyValueStoreClient,
        store_id: &str,
        keys: impl IntoIterator<Item = &'a str>,
    ) {
        let store = client.clone();
        self.metadata.start(store_id.to_owned(), async move { get_metadata(&store).await });
        for key in keys {
            let (store, owned_key) = (client.clone(), key.to_owned());
            self.records
                .start((store_id.to_owned(), key.to_owned()), async move { get_record(&store, &owned_key).await });
        }
    }
}

async fn get_metadata(client: &KeyValueStoreClient) -> StorageResult<KeyValueStoreInfo> {
    let store = client
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

async fn get_record(client: &KeyValueStoreClient, key: &str) -> StorageResult<Option<KeyValueStoreRecord>> {
    let record = client.get_record(key).await.map_err(backend_error)?;
    Ok(record.map(|record| KeyValueStoreRecord {
        key: record.key,
        value: Bytes::from(record.value),
        content_type: record.content_type,
    }))
}

impl ApifyKeyValueStore {
    /// Discards what was read ahead of the record `key` and of the store, which a write changes.
    fn forget(&self, key: &str) {
        self.prefetched.records.forget(&(self.id.clone(), key.to_owned()));
        self.prefetched.metadata.forget(&self.id);
    }
}

#[async_trait]
impl KeyValueStoreBackend for ApifyKeyValueStore {
    async fn get_metadata(&self) -> StorageResult<KeyValueStoreInfo> {
        self.prefetched.metadata.take_or(&self.id, || get_metadata(&self.client)).await
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
        let key = (self.id.clone(), key.to_owned());
        self.prefetched.records.take_or(&key, || get_record(&self.client, &key.1)).await
    }

    async fn set_value(&self, record: KeyValueStoreRecord) -> StorageResult<()> {
        self.forget(&record.key);
        let content_type = record.content_type.as_deref().unwrap_or(DEFAULT_CONTENT_TYPE);
        self.client.set_record_raw(&record.key, record.value.to_vec(), content_type).await.map_err(backend_error)
    }

    async fn delete_value(&self, key: &str) -> StorageResult<()> {
        self.forget(key);
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
