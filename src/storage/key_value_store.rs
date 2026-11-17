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

use std::collections::HashSet;
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

/// The keys of a store, listed once for all the records read ahead of it; `None` when they could
/// not be listed, or the store holds too many to list.
type Listing = Arc<tokio::sync::OnceCell<Option<HashSet<String>>>>;

/// The size of a page of the listing: the most the API returns at once.
const LISTING_PAGE_SIZE: i64 = 1000;
/// How many pages the listing reads at most. The records of a store holding more keys are read
/// one by one.
const MAX_LISTING_PAGES: usize = 10;

impl PrefetchedStores {
    /// Starts reading the metadata of the store of `client` (whose id is `store_id`) and its
    /// records `keys`: `read_at_once` (the input, which a run nearly always has) at once, the others
    /// once a listing of the store's keys finds them there. Those it does not find are read as
    /// missing without asking the API: in a fresh run, most records read ahead are (state an
    /// earlier container of the run would have saved). When the store cannot be listed, or holds
    /// more keys than [`MAX_LISTING_PAGES`] pages, each record is read.
    pub(crate) fn prefetch<'a>(
        &self,
        client: &KeyValueStoreClient,
        store_id: &str,
        keys: impl IntoIterator<Item = &'a str>,
        read_at_once: Option<&str>,
    ) {
        let store = client.clone();
        self.metadata.start(store_id.to_owned(), async move { get_metadata(&store).await });
        let listing = Listing::default();
        for key in keys {
            let (store, owned_key, listing) = (client.clone(), key.to_owned(), listing.clone());
            let listed = read_at_once != Some(key);
            self.records.start((store_id.to_owned(), key.to_owned()), async move {
                // The first of these reads lists the keys, and the others wait for that listing.
                if listed
                    && let Some(keys) = listing.get_or_init(|| list_all_keys(&store)).await
                    && !keys.contains(&owned_key)
                {
                    return Ok(None);
                }
                get_record(&store, &owned_key).await
            });
        }
    }
}

/// All the keys of the store of `client`, or `None` when it holds more than [`MAX_LISTING_PAGES`]
/// pages of them or the listing fails.
///
/// The listing is complete: on the Apify platform, a record is stored before its write is
/// answered, and listed from then on (unlike a request queue's head, which lags behind), so the
/// records an earlier container of the run wrote are listed.
async fn list_all_keys(client: &KeyValueStoreClient) -> Option<HashSet<String>> {
    let mut keys = HashSet::new();
    let mut exclusive_start_key = None;
    for _ in 0..MAX_LISTING_PAGES {
        let options = ListKeysOptions {
            limit: Some(LISTING_PAGE_SIZE),
            exclusive_start_key: exclusive_start_key.take(),
            ..ListKeysOptions::default()
        };
        let page = match client.list_keys(options).await {
            Ok(page) => page,
            Err(err) => {
                tracing::debug!("Listing the keys of the key-value store failed, reading each record instead: {err}");
                return None;
            }
        };
        keys.extend(page.items.into_iter().map(|item| item.key));
        if !page.is_truncated {
            return Some(keys);
        }
        // A truncated page without where to go on is read as a store too big to list.
        exclusive_start_key = Some(page.next_exclusive_start_key?);
    }
    tracing::debug!("The key-value store holds more than {} keys, reading each record instead", keys.len());
    None
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
