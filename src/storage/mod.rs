//! Storages on the Apify platform, as a crawlee-rs [`StorageBackend`], and the backend an Actor
//! uses: platform storages on the platform, local files elsewhere.

mod dataset;
mod key_value_store;
mod request_queue;
mod request_queue_shared;

use std::collections::HashMap;
use std::sync::Arc;

use apify_client::models::Extra;
use apify_client::{ApifyClient, ApifyClientError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crawlee::StorageIdentifier;
use crawlee::core::StorageResult;
use crawlee::core::errors::StorageError;
use crawlee::core::storage::backend::{
    DatasetBackend, KeyValueStoreBackend, RequestQueueBackend, StorageBackend, StorageKind,
};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::client::{RateLimitCounter, new_client};
use crate::configuration::Configuration;

pub use self::request_queue::RequestQueueAccess;

/// The alias Crawlee for JS opens the default storage under.
pub const DEFAULT_STORAGE_ALIAS: &str = "__default__";
/// The record of the default key-value store holding a run's alias → storage id mapping.
const ALIAS_MAPPING_RECORD_KEY: &str = "__STORAGE_ALIASES_MAPPING";
/// The longest client key the request queue API accepts.
const MAX_CLIENT_KEY_LENGTH: usize = 32;

pub(crate) fn backend_error(err: ApifyClientError) -> StorageError {
    StorageError::Backend(Box::new(err))
}

pub(crate) fn unsupported(message: &str) -> StorageError {
    StorageError::Backend(message.into())
}

/// A timestamp the API left out; storages always have them, so this is only a fallback.
pub(crate) fn date(value: Option<DateTime<Utc>>) -> DateTime<Utc> {
    value.unwrap_or_else(Utc::now)
}

pub(crate) fn extra_date(extra: &Extra, key: &str) -> DateTime<Utc> {
    extra.get(key).and_then(|value| serde_json::from_value(value.clone()).ok()).unwrap_or_else(Utc::now)
}

pub(crate) fn extra_u64(extra: &Extra, key: &str) -> u64 {
    extra.get(key).and_then(Value::as_u64).unwrap_or(0)
}

fn kind_name(kind: StorageKind) -> &'static str {
    match kind {
        StorageKind::Dataset => "Dataset",
        StorageKind::KeyValueStore => "KeyValueStore",
        StorageKind::RequestQueue => "RequestQueue",
    }
}

/// A random id of `length` alphanumeric characters (`cryptoRandomObjectId` in JS).
fn random_id(length: usize) -> String {
    use rand::Rng as _;
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();
    (0..length).map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char).collect()
}

/// Storages on the Apify platform.
///
/// Storages are resolved as in the JS SDK:
/// - [`Id`](StorageIdentifier::Id) is used as it is, and [`Name`](StorageIdentifier::Name)
///   opens (or creates) the named storage;
/// - [`Default`](StorageIdentifier::Default) (and the alias `__default__`) is the run's default
///   storage, from [`Configuration`];
/// - an alias is the storage the Actor's schema declares for it (`ACTOR_STORAGES_JSON`), or else
///   an unnamed storage of its own. On the platform the aliases are remembered in the
///   `__STORAGE_ALIASES_MAPPING` record of the default key-value store, so a migrated run
///   reopens the same storages.
pub struct ApifyStorageBackend {
    client: ApifyClient,
    configuration: Arc<Configuration>,
    request_queue_access: RequestQueueAccess,
    credentials_hash: String,
    rate_limits: RateLimitCounter,
    client_key: String,
    aliases: tokio::sync::Mutex<AliasCache>,
}

#[derive(Default)]
struct AliasCache {
    /// Storages resolved for aliases in this process.
    known: HashMap<String, String>,
    /// The mapping read from the default key-value store, once read.
    persisted: Option<Map<String, Value>>,
}

impl std::fmt::Debug for ApifyStorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApifyStorageBackend").field("request_queue_access", &self.request_queue_access).finish()
    }
}

impl ApifyStorageBackend {
    /// Storages of the API and account `configuration` points to.
    pub fn new(configuration: Arc<Configuration>) -> Self {
        let token = configuration.token.clone();
        Self::with_token(configuration, token)
    }

    /// Storages accessed with `token` instead of [`Configuration::token`].
    pub fn with_token(configuration: Arc<Configuration>, token: Option<String>) -> Self {
        let rate_limits = RateLimitCounter::default();
        let client = new_client(&configuration, token.as_deref(), rate_limits.clone());
        // `publicBaseUrl` of the JS client: without the trailing slash, plus `/v2`.
        let public_base_url = format!("{}/v2", configuration.api_public_base_url.trim_end_matches('/'));
        let digest = Sha256::digest(format!("{public_base_url}{}", token.as_deref().unwrap_or_default()));
        let credentials_hash = digest.iter().take(4).map(|byte| format!("{byte:02x}")).collect();
        // A stable client key per run lets a migrated run take over its earlier locks.
        let client_key = configuration.actor_run_id.clone().unwrap_or_else(|| random_id(MAX_CLIENT_KEY_LENGTH));
        let client_key = client_key.chars().take(MAX_CLIENT_KEY_LENGTH).collect();
        ApifyStorageBackend {
            client,
            configuration,
            request_queue_access: RequestQueueAccess::default(),
            credentials_hash,
            rate_limits,
            client_key,
            aliases: tokio::sync::Mutex::default(),
        }
    }

    /// How request queues are consumed (see [`RequestQueueAccess`]).
    pub fn request_queue_access(mut self, access: RequestQueueAccess) -> Self {
        self.request_queue_access = access;
        self
    }

    pub fn client(&self) -> &ApifyClient {
        &self.client
    }

    /// The rate-limited calls of [`client`](Self::client) (`client.stats.rateLimitErrors` in JS).
    /// Not reported to autoscaling as [`StorageBackend::rate_limit_errors`]: the JS SDK's backend
    /// has no `stats`, so its storage load signal never sees a rate limit.
    pub fn rate_limits(&self) -> &RateLimitCounter {
        &self.rate_limits
    }

    async fn get_exists(&self, id: &str, kind: StorageKind) -> StorageResult<Option<String>> {
        let found = match kind {
            StorageKind::Dataset => self.client.dataset(id).get().await.map_err(backend_error)?.map(|s| s.id),
            StorageKind::KeyValueStore => {
                self.client.key_value_store(id).get().await.map_err(backend_error)?.map(|s| s.id)
            }
            StorageKind::RequestQueue => {
                self.client.request_queue(id).get().await.map_err(backend_error)?.map(|s| s.id)
            }
        };
        Ok(found)
    }

    async fn get_or_create(&self, name: Option<&str>, kind: StorageKind) -> StorageResult<String> {
        let id = match kind {
            StorageKind::Dataset => self.client.datasets().get_or_create(name).await.map_err(backend_error)?.id,
            StorageKind::KeyValueStore => {
                self.client.key_value_stores().get_or_create(name).await.map_err(backend_error)?.id
            }
            StorageKind::RequestQueue => {
                self.client.request_queues().get_or_create(name).await.map_err(backend_error)?.id
            }
        };
        Ok(id)
    }

    async fn resolve_id(&self, identifier: &StorageIdentifier, kind: StorageKind) -> StorageResult<String> {
        let alias = match identifier {
            StorageIdentifier::Id(id) => return Ok(id.clone()),
            StorageIdentifier::Name(name) => return self.get_or_create(Some(name), kind).await,
            StorageIdentifier::Default => DEFAULT_STORAGE_ALIAS,
            StorageIdentifier::Alias(alias) => alias.as_str(),
        };
        if alias == DEFAULT_STORAGE_ALIAS {
            let default_id = match kind {
                StorageKind::Dataset => &self.configuration.default_dataset_id,
                StorageKind::KeyValueStore => &self.configuration.default_key_value_store_id,
                StorageKind::RequestQueue => &self.configuration.default_request_queue_id,
            };
            if !default_id.is_empty() {
                return Ok(default_id.clone());
            }
        } else if let Some(id) = self.alias_from_actor_storages(alias, kind)? {
            return Ok(id);
        }
        self.resolve_alias_id(alias, kind).await
    }

    /// The storage the Actor's schema declares for `alias` (`ACTOR_STORAGES_JSON`).
    fn alias_from_actor_storages(&self, alias: &str, kind: StorageKind) -> StorageResult<Option<String>> {
        let Some(json) = &self.configuration.actor_storages_json else { return Ok(None) };
        let storages: Value = serde_json::from_str(json).map_err(|_| {
            StorageError::InvalidArgument(format!("Failed to parse ACTOR_STORAGES_JSON environment variable: {json}"))
        })?;
        let key = match kind {
            StorageKind::Dataset => "datasets",
            StorageKind::KeyValueStore => "keyValueStores",
            StorageKind::RequestQueue => "requestQueues",
        };
        Ok(storages.get(key).and_then(|by_alias| by_alias.get(alias)).and_then(Value::as_str).map(str::to_owned))
    }

    /// The unnamed storage of `alias`, created on first use. Serialized, so one alias is one
    /// storage and the read-modify-write of the mapping record drops no entries.
    async fn resolve_alias_id(&self, alias: &str, kind: StorageKind) -> StorageResult<String> {
        // The credentials are part of the key: the same alias opened with two tokens is two storages.
        let key = format!("{},{alias},{}", kind_name(kind), self.credentials_hash);
        let mut aliases = self.aliases.lock().await;
        if let Some(id) = aliases.known.get(&key) {
            return Ok(id.clone());
        }

        let store = self.alias_mapping_store();
        if aliases.persisted.is_none() {
            aliases.persisted = Some(match &store {
                Some(store) => read_alias_mapping(store).await?,
                None => Map::new(),
            });
        }
        let persisted_id = aliases.persisted.as_ref().and_then(|mapping| mapping.get(&key)).and_then(Value::as_str);
        // A remembered storage may have been deleted since.
        if let Some(id) = persisted_id.map(str::to_owned)
            && self.get_exists(&id, kind).await?.is_some()
        {
            aliases.known.insert(key, id.clone());
            return Ok(id);
        }

        let id = self.get_or_create(None, kind).await?;
        aliases.known.insert(key.clone(), id.clone());
        if let Some(store) = store {
            // Only costs a new storage after a migration when it fails, so it is not an error.
            match persist_alias_id(&store, &key, &id).await {
                Ok(mapping) => aliases.persisted = Some(mapping),
                Err(err) => tracing::warn!("Failed to persist the storage alias mapping: {err}"),
            }
        }
        Ok(id)
    }

    /// The run's default key-value store, which keeps the alias mapping; only on the platform.
    fn alias_mapping_store(&self) -> Option<apify_client::clients::key_value_store::KeyValueStoreClient> {
        self.configuration
            .is_at_home
            .then(|| self.client.key_value_store(self.configuration.default_key_value_store_id.clone()))
    }
}

async fn read_alias_mapping(
    store: &apify_client::clients::key_value_store::KeyValueStoreClient,
) -> StorageResult<Map<String, Value>> {
    let record = store.get_record(ALIAS_MAPPING_RECORD_KEY).await.map_err(backend_error)?;
    Ok(record.and_then(|record| serde_json::from_slice(&record.value).ok()).unwrap_or_default())
}

/// Reads the record again first, so entries written by another backend of this run are kept.
async fn persist_alias_id(
    store: &apify_client::clients::key_value_store::KeyValueStoreClient,
    key: &str,
    id: &str,
) -> StorageResult<Map<String, Value>> {
    let mut mapping = read_alias_mapping(store).await?;
    mapping.insert(key.to_owned(), Value::String(id.to_owned()));
    store.set_record_json(ALIAS_MAPPING_RECORD_KEY, &mapping).await.map_err(backend_error)?;
    Ok(mapping)
}

#[async_trait]
impl StorageBackend for ApifyStorageBackend {
    async fn create_dataset_backend(&self, id: &StorageIdentifier) -> StorageResult<Arc<dyn DatasetBackend>> {
        let id = self.resolve_id(id, StorageKind::Dataset).await?;
        Ok(Arc::new(dataset::ApifyDataset { client: self.client.dataset(id) }))
    }

    async fn create_key_value_store_backend(
        &self,
        id: &StorageIdentifier,
    ) -> StorageResult<Arc<dyn KeyValueStoreBackend>> {
        let id = self.resolve_id(id, StorageKind::KeyValueStore).await?;
        Ok(Arc::new(key_value_store::ApifyKeyValueStore { client: self.client.key_value_store(id) }))
    }

    async fn create_request_queue_backend(
        &self,
        id: &StorageIdentifier,
    ) -> StorageResult<Arc<dyn RequestQueueBackend>> {
        let id = self.resolve_id(id, StorageKind::RequestQueue).await?;
        let client = self.client.request_queue(id).with_client_key(self.client_key.clone());
        Ok(match self.request_queue_access {
            RequestQueueAccess::Single => Arc::new(request_queue::ApifySingleRequestQueue::new(client)),
            RequestQueueAccess::Shared => Arc::new(request_queue_shared::ApifySharedRequestQueue::new(client)),
        })
    }

    /// Whether `id` is the id of a storage (the API also finds storages by name).
    async fn storage_exists(&self, id: &str, kind: StorageKind) -> StorageResult<bool> {
        Ok(self.get_exists(id, kind).await?.as_deref() == Some(id))
    }

    // `purge` keeps the default no-op: the platform gives every run empty default storages.
    // `rate_limit_errors` keeps the default 0, see `rate_limits`.
}

/// The storage backend of an Actor: [`ApifyStorageBackend`] on the platform, `local` elsewhere,
/// except for storages opened with `force_cloud`.
pub struct SmartStorageBackend {
    cloud: Arc<dyn StorageBackend>,
    local: Arc<dyn StorageBackend>,
    configuration: Arc<Configuration>,
}

impl SmartStorageBackend {
    pub fn new(
        configuration: Arc<Configuration>,
        cloud: Arc<dyn StorageBackend>,
        local: Arc<dyn StorageBackend>,
    ) -> Self {
        SmartStorageBackend { cloud, local, configuration }
    }

    /// The local backend of an Actor: files under the storage directory (see
    /// [`crawlee::Configuration::storage_dir`]) with the input adopted from `INPUT` or
    /// `INPUT.json` and kept on purge, or memory when storage is not persisted.
    pub fn local_backend(configuration: &Configuration) -> Arc<dyn StorageBackend> {
        if configuration.crawlee.persist_storage {
            Arc::new(
                crawlee::core::FileSystemStorageBackend::new(configuration.crawlee.storage_dir.clone())
                    .with_input_keys(["INPUT", configuration.input_key.as_str()]),
            )
        } else {
            Arc::new(crawlee::core::MemoryStorageBackend::new())
        }
    }

    /// The backend of an Actor configured by `configuration`.
    pub fn for_configuration(configuration: Arc<Configuration>) -> Self {
        let cloud = Arc::new(ApifyStorageBackend::new(configuration.clone()));
        let local = Self::local_backend(&configuration);
        SmartStorageBackend::new(configuration, cloud, local)
    }

    /// The backend storages are opened through: the cloud one on the platform or with
    /// `force_cloud` (which needs a token), the local one otherwise.
    pub fn suitable(&self, force_cloud: bool) -> StorageResult<&dyn StorageBackend> {
        if self.configuration.is_at_home {
            return Ok(self.cloud.as_ref());
        }
        if !force_cloud {
            return Ok(self.local.as_ref());
        }
        if self.configuration.token.is_none() {
            return Err(StorageError::InvalidArgument(
                "In order to use the Apify cloud storage from your computer, you need to provide an Apify token \
                 using the APIFY_TOKEN environment variable."
                    .to_owned(),
            ));
        }
        Ok(self.cloud.as_ref())
    }

    fn current(&self) -> &dyn StorageBackend {
        if self.configuration.is_at_home { self.cloud.as_ref() } else { self.local.as_ref() }
    }
}

#[async_trait]
impl StorageBackend for SmartStorageBackend {
    async fn create_dataset_backend(&self, id: &StorageIdentifier) -> StorageResult<Arc<dyn DatasetBackend>> {
        self.current().create_dataset_backend(id).await
    }

    async fn create_key_value_store_backend(
        &self,
        id: &StorageIdentifier,
    ) -> StorageResult<Arc<dyn KeyValueStoreBackend>> {
        self.current().create_key_value_store_backend(id).await
    }

    async fn create_request_queue_backend(
        &self,
        id: &StorageIdentifier,
    ) -> StorageResult<Arc<dyn RequestQueueBackend>> {
        self.current().create_request_queue_backend(id).await
    }

    async fn storage_exists(&self, id: &str, kind: StorageKind) -> StorageResult<bool> {
        self.current().storage_exists(id, kind).await
    }

    async fn purge(&self) -> StorageResult<()> {
        self.current().purge().await
    }

    async fn teardown(&self) -> StorageResult<()> {
        self.current().teardown().await
    }

    fn rate_limit_errors(&self) -> u64 {
        self.current().rate_limit_errors()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_hash_matches_the_js_sdk() {
        // createHash('sha256').update('https://api.apify.com/v2' + 'token').digest('hex').slice(0, 8)
        let configuration = Arc::new(Configuration { token: Some("token".to_owned()), ..Configuration::default() });
        let backend = ApifyStorageBackend::new(configuration);
        assert_eq!(backend.credentials_hash, "8659e64f");
        assert_eq!(backend.client_key.len(), MAX_CLIENT_KEY_LENGTH);
    }
}
