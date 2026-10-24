//! [`Actor`]: the lifecycle of an Actor run, and access to its storages, input and platform.

use std::future::Future;
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use apify_client::ApifyClient;
use crawlee::core::storage::backend::StorageBackend;
use crawlee::core::{Event, EventKind, EventManager, StatusLevel};
use crawlee::{Dataset, KeyValueStore, RequestQueue, Services, StorageIdentifier};
use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::charging::{
    ChargeOptions, ChargeResult, ChargingError, ChargingManager, ChargingStorageBackend, DEFAULT_DATASET_ITEM_EVENT,
};
use crate::client::{RateLimitCounter, new_client};
use crate::configuration::Configuration;
use crate::input::{ActorInputError, ActorInputErrorCode, Input, InputSchema};
use crate::storage::{ApifyStorageBackend, RequestQueueAccess, SmartStorageBackend};

/// Exit codes of an Actor run (`EXIT_CODES` in `@apify/consts`).
pub mod exit_codes {
    pub const SUCCESS: i32 = 0;
    pub const ERROR_USER_FUNCTION_THREW: i32 = 91;
    pub const ERROR_UNKNOWN: i32 = 92;
}

/// How long `set_status_message` waits for the API.
const STATUS_MESSAGE_TIMEOUT: Duration = Duration::from_secs(1);

/// Options of [`Actor::init`].
#[derive(Clone)]
pub struct InitOptions {
    /// The configuration; [`Configuration::from_env`] when not set.
    pub configuration: Option<Configuration>,
    /// A storage backend used instead of the platform storages (on the platform) or the local
    /// files (elsewhere).
    pub storage: Option<Arc<dyn StorageBackend>>,
    /// How request queues on the platform are consumed.
    pub request_queue_access: RequestQueueAccess,
    /// Exit when the run is aborted, and reboot when it is migrated. Default `true`.
    pub graceful_shutdown: bool,
    /// How long to wait before the graceful exit or reboot.
    pub graceful_shutdown_delay: Duration,
}

impl Default for InitOptions {
    fn default() -> Self {
        InitOptions {
            configuration: None,
            storage: None,
            request_queue_access: RequestQueueAccess::default(),
            graceful_shutdown: true,
            graceful_shutdown_delay: Duration::ZERO,
        }
    }
}

/// Options of [`Actor::exit`].
#[derive(Clone, Debug)]
pub struct ExitOptions {
    /// The last status message of the run.
    pub status_message: Option<String>,
    pub exit_code: i32,
    /// End the process. With `false`, `exit` only tears the Actor down and returns.
    pub exit: bool,
    /// How long to wait for event listeners and storages before exiting anyway. Default 30 s.
    pub timeout: Duration,
}

impl Default for ExitOptions {
    fn default() -> Self {
        ExitOptions {
            status_message: None,
            exit_code: exit_codes::SUCCESS,
            exit: true,
            timeout: Duration::from_secs(30),
        }
    }
}

impl ExitOptions {
    pub fn message(message: impl Into<String>) -> Self {
        ExitOptions { status_message: Some(message.into()), ..ExitOptions::default() }
    }
}

/// Options of [`Actor::set_status_message`].
#[derive(Clone, Copy, Debug, Default)]
pub struct StatusMessageOptions {
    /// The last message of the run.
    pub is_terminal: bool,
    /// How the message is logged. `Debug` (the default) logs it at the info level, as in JS.
    pub level: StatusLevel,
}

/// Where storages are opened (see [`Actor::open_dataset`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OpenOptions {
    /// Use the platform storage even when running locally (needs `APIFY_TOKEN`).
    pub force_cloud: bool,
}

#[derive(Default)]
struct Handlers {
    graceful: Vec<crawlee::core::events::ListenerId>,
    status_forwarder: Option<crawlee::core::events::ListenerId>,
}

struct Inner {
    configuration: Arc<Configuration>,
    client: ApifyClient,
    storage: Arc<dyn StorageBackend>,
    smart: Option<Arc<SmartStorageBackend>>,
    charging: Arc<ChargingManager>,
    services: Services,
    exiting: AtomicBool,
    rebooting: AtomicBool,
    handlers: Mutex<Handlers>,
    websocket: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// The Actor run of this process, like the static `Actor` of the JS SDK: [`Actor::init`] once,
/// then its functions from anywhere, and [`Actor::exit`] at the end.
///
/// ```no_run
/// # use apify::{Actor, ExitOptions, InitOptions};
/// # async fn run() -> anyhow::Result<()> {
/// Actor::init(InitOptions::default()).await?;
/// let input: serde_json::Value = Actor::get_input().await?;
/// Actor::push_data(&input).await?;
/// Actor::exit(ExitOptions::default()).await;
/// # Ok(())
/// # }
/// ```
///
/// [`Actor::init`] installs the run's [`Services`] as the process-wide crawlee-rs services, so
/// crawlers built afterwards store their data where the Actor does: in the platform storages on
/// the platform, in `./storage` elsewhere.
///
/// # Panics
///
/// The functions that use the run panic when the Actor was not initialized: that is a bug in the
/// code, not a condition to handle.
pub enum Actor {}

static STATE: OnceLock<Inner> = OnceLock::new();

/// The state of the initialized Actor.
fn state() -> &'static Inner {
    STATE.get().expect("Actor::init() must be called before the other functions of Actor")
}

/// Initialization failed.
#[derive(Debug, thiserror::Error)]
pub enum InitError {
    #[error(
        "Actor::init() was called after the crawlee-rs services were used or set: a storage was opened or a crawler \
         was built before it. Call Actor::init() first, and pass a storage of your own as InitOptions::storage."
    )]
    ServicesAlreadySet,
    #[error(transparent)]
    Storage(#[from] crawlee::core::StorageError),
    #[error(transparent)]
    Charging(#[from] ChargingError),
    /// The platform events websocket could not be connected to.
    #[error("Error connecting to platform events websocket! {0}")]
    EventsConnection(String),
}

fn flush_and_exit(code: i32) -> ! {
    let _ = std::io::stdout().flush();
    let _ = std::io::stderr().flush();
    std::process::exit(code)
}

impl Actor {
    /// Runs `user_function` as an Actor: [`Actor::init`], the function, then [`Actor::exit`].
    /// When the function fails, the error is logged and the run exits with code 91.
    ///
    /// ```no_run
    /// # use apify::Actor;
    /// # async fn run() {
    /// Actor::main(|| async {
    ///     let input: serde_json::Value = Actor::get_input().await?;
    ///     Actor::push_data(&input).await?;
    ///     Ok(())
    /// })
    /// .await;
    /// # }
    /// ```
    pub async fn main<F, Fut>(user_function: F)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = anyhow::Result<()>>,
    {
        Self::main_with(InitOptions::default(), ExitOptions::default(), user_function).await;
    }

    /// [`main`](Self::main) with options.
    pub async fn main_with<F, Fut>(init: InitOptions, exit: ExitOptions, user_function: F)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = anyhow::Result<()>>,
    {
        if let Err(err) = Self::init(init).await {
            tracing::error!("Initializing the Actor failed: {err}");
            flush_and_exit(exit_codes::ERROR_UNKNOWN);
        }
        match user_function().await {
            Ok(()) => Self::exit(exit).await,
            Err(err) => {
                tracing::error!("{err:?}");
                Self::exit(ExitOptions { exit_code: exit_codes::ERROR_USER_FUNCTION_THREW, ..exit }).await;
            }
        }
    }

    /// Initializes the Actor: installs the storages and events of the run as the process-wide
    /// crawlee-rs services, connects to the platform events, and purges the local default
    /// storages. Later calls do nothing.
    pub async fn init(options: InitOptions) -> Result<(), InitError> {
        if STATE.get().is_some() {
            tracing::debug!("The Actor was already initialized");
            return Ok(());
        }
        if Services::is_global_set() {
            return Err(InitError::ServicesAlreadySet);
        }

        let configuration = Arc::new(options.configuration.unwrap_or_else(Configuration::from_env));
        // How much the runtime runs in parallel: the CPUs Rust sees (cgroup quotas included) and
        // the tokio worker threads.
        let runtime = tokio::runtime::Handle::current();
        tracing::info!(
            apify_version = env!("CARGO_PKG_VERSION"),
            apify_client_version = apify_client::CLIENT_VERSION,
            crawlee_version = crawlee::VERSION,
            os = std::env::consts::OS,
            available_parallelism = std::thread::available_parallelism().map_or(0, std::num::NonZero::get),
            tokio_flavor = ?runtime.runtime_flavor(),
            tokio_workers = runtime.metrics().num_workers(),
            "System info"
        );

        let client = new_client(&configuration, None, RateLimitCounter::default());
        let charging = Arc::new(ChargingManager::new(configuration.clone(), client.clone()));
        let (storage, smart): (Arc<dyn StorageBackend>, _) = match options.storage {
            Some(storage) => (storage, None),
            None => {
                let cloud: Arc<dyn StorageBackend> = Arc::new(
                    ApifyStorageBackend::new(configuration.clone()).request_queue_access(options.request_queue_access),
                );
                let local = SmartStorageBackend::local_backend(&configuration);
                // The default datasets charge for their items under pay-per-event pricing.
                let charged = |inner| -> Arc<dyn StorageBackend> {
                    Arc::new(ChargingStorageBackend {
                        inner,
                        charging: charging.clone(),
                        default_dataset_id: configuration.default_dataset_id.clone(),
                    })
                };
                let (cloud, local) = (charged(cloud), charged(local));
                let smart = Arc::new(SmartStorageBackend::new(configuration.clone(), cloud, local));
                (smart.clone(), Some(smart))
            }
        };

        // On the platform, system info comes from the platform events; locally it is measured.
        let events = if configuration.is_at_home {
            EventManager::new(configuration.crawlee.persist_state_interval)
        } else {
            EventManager::local(&configuration.crawlee)
        };
        let services = Services::from_parts(configuration.crawlee.clone(), storage.clone(), events.clone());
        // Only one `init` gets past this: the services can be set once.
        Services::set_global(services.clone()).map_err(|_| InitError::ServicesAlreadySet)?;

        let inner = Inner {
            configuration: configuration.clone(),
            client,
            storage,
            smart,
            charging,
            services,
            exiting: AtomicBool::new(false),
            rebooting: AtomicBool::new(false),
            handlers: Mutex::default(),
            websocket: Mutex::default(),
        };
        if STATE.set(inner).is_err() {
            return Err(InitError::ServicesAlreadySet);
        }
        let state = state();

        events.init().await;
        if options.graceful_shutdown {
            Self::register_graceful_shutdown(options.graceful_shutdown_delay);
        }
        // Crawlers report their status as `StatusMessage` events.
        let forwarder = events.on(EventKind::StatusMessage, |event| async move {
            if let Event::StatusMessage(status) = event {
                Self::update_run_status_message(&status.message, status.is_terminal).await;
            }
        });
        state.handlers.lock().status_forwarder = Some(forwarder);

        // Connected once the listeners are in place, so that no early event is missed.
        if configuration.is_at_home {
            match &configuration.actor_events_ws_url {
                Some(url) => {
                    let memory = configuration.crawlee.memory_mbytes.map(|mb| mb * 1024 * 1024);
                    match crate::events::connect(url, events.clone(), memory).await {
                        Ok(relaying) => *state.websocket.lock() = Some(relaying),
                        Err(err) => {
                            events.close().await;
                            return Err(InitError::EventsConnection(err));
                        }
                    }
                }
                None => tracing::debug!(
                    "Environment variable ACTOR_EVENTS_WEBSOCKET_URL is not set, no events from Apify platform will be emitted."
                ),
            }
        }

        state.services.purge_on_start().await?;
        state.charging.init(state.storage.as_ref()).await?;
        if state.charging.is_pay_per_event() && state.smart.is_none() {
            tracing::warn!(
                "Items pushed to the default dataset will not be charged for, because this run does not use Apify \
                 storage - the platform only counts items it stores itself."
            );
        }
        Ok(())
    }

    /// On `aborting`, exit; on `migrating`, reboot to move sooner. Both run outside the listener,
    /// which `exit` and `reboot` wait for.
    fn register_graceful_shutdown(delay: Duration) {
        let events = Self::events();
        let aborting = events.on(EventKind::Aborting, move |_| {
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                Self::exit(ExitOptions::default()).await;
            });
            async {}
        });
        let migrating = events.on(EventKind::Migrating, move |_| {
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                if let Err(err) = Self::reboot().await {
                    tracing::error!("Failed to reboot on migration: {err}");
                }
            });
            async {}
        });
        state().handlers.lock().graceful = vec![aborting, migrating];
    }

    pub fn configuration() -> &'static Arc<Configuration> {
        &state().configuration
    }

    /// The API client of the run, authenticated with `APIFY_TOKEN`.
    pub fn client() -> &'static ApifyClient {
        &state().client
    }

    /// A client authenticated with another token.
    pub fn new_client(token: &str) -> ApifyClient {
        new_client(&state().configuration, Some(token), RateLimitCounter::default())
    }

    /// The services of the run, which crawlers use by default.
    pub fn services() -> &'static Services {
        &state().services
    }

    pub fn events() -> &'static EventManager {
        &state().services.events
    }

    pub fn is_at_home() -> bool {
        state().configuration.is_at_home
    }

    /// Ends the run: stops the events (after a final `PersistState`), waits for their listeners,
    /// tears the storages down, sets the terminal status message and exits the process with
    /// [`ExitOptions::exit_code`]. If this takes longer than [`ExitOptions::timeout`], the process
    /// exits anyway. Returns only with [`ExitOptions::exit`] set to `false`, or when already exiting.
    pub async fn exit(options: ExitOptions) {
        if state().exiting.swap(true, Ordering::AcqRel) {
            tracing::debug!("Actor::exit() called while already exiting, skipping");
            return;
        }
        let events = Self::events().clone();
        for id in std::mem::take(&mut state().handlers.lock().graceful) {
            events.off(id);
        }

        events.close().await;
        tracing::debug!("Events closed");
        events.emit(Event::Exit);

        if options.exit {
            // A thread of its own: the tokio runtime may be too busy to run a timer.
            let (timeout, code) = (options.timeout, options.exit_code);
            std::thread::spawn(move || {
                std::thread::sleep(timeout);
                tracing::error!("Exiting the Actor timed out after {} seconds", timeout.as_secs());
                flush_and_exit(code);
            });
        }

        let teardown = async {
            events.wait_for_all_listeners_to_complete().await;
            // Final status messages are forwarded by now; later ones must not overwrite the terminal one.
            if let Some(id) = state().handlers.lock().status_forwarder.take() {
                events.off(id);
            }
            if let Err(err) = state().storage.teardown().await {
                tracing::error!("Tearing down the storage failed: {err}");
            }
            if let Some(message) = &options.status_message {
                let level = if options.exit_code > 0 { StatusLevel::Error } else { StatusLevel::Info };
                Self::set_status_message(message, StatusMessageOptions { is_terminal: true, level }).await;
            }
        };
        if tokio::time::timeout(options.timeout, teardown).await.is_err() {
            tracing::error!(
                "Waiting for all event listeners to complete their execution timed out after {} seconds",
                options.timeout.as_secs()
            );
        }
        if let Some(websocket) = state().websocket.lock().take() {
            websocket.abort();
        }

        state().exiting.store(false, Ordering::Release);
        if options.exit {
            flush_and_exit(options.exit_code);
        }
    }

    /// [`exit`](Self::exit) with exit code 1, or the code of `options` when it is not 0.
    pub async fn fail(options: ExitOptions) {
        let exit_code = if options.exit_code == exit_codes::SUCCESS { 1 } else { options.exit_code };
        Self::exit(ExitOptions { exit_code, ..options }).await;
    }

    /// Sets the status message of the run, and logs it.
    pub async fn set_status_message(message: &str, options: StatusMessageOptions) {
        match options.level {
            StatusLevel::Warning => tracing::warn!("[Status message]: {message}"),
            StatusLevel::Error => tracing::error!("[Status message]: {message}"),
            _ => tracing::info!("[Status message]: {message}"),
        }
        Self::update_run_status_message(message, options.is_terminal).await;
    }

    /// Best effort: a failure or a slow API is only logged.
    async fn update_run_status_message(message: &str, is_terminal: bool) {
        let Some(run_id) = &state().configuration.actor_run_id else { return };
        let body = serde_json::json!({ "statusMessage": message, "isStatusMessageTerminal": is_terminal });
        let run = state().client.run(run_id.clone());
        match tokio::time::timeout(STATUS_MESSAGE_TIMEOUT, run.update(&body)).await {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => tracing::warn!("Setting the status message failed: {err}"),
            Err(_) => tracing::warn!("Setting status message timed out after 1s"),
        }
    }

    /// Reboots the run on the platform (a new container, same run), after its state was saved.
    /// Does nothing off the platform.
    pub async fn reboot() -> Result<(), apify_client::ApifyClientError> {
        let configuration = &state().configuration;
        if !configuration.is_at_home {
            tracing::warn!("Actor::reboot() is only supported when running on the Apify platform.");
            return Ok(());
        }
        if state().rebooting.swap(true, Ordering::AcqRel) {
            tracing::debug!("Actor is already rebooting, skipping the additional reboot call.");
            return Ok(());
        }
        // The container is killed: save the state and pause the crawlers first.
        let events = Self::events();
        events.emit(Event::PersistState { is_migrating: false });
        events.emit(Event::Migrating);
        events.wait_for_all_listeners_to_complete().await;

        let run_id = configuration
            .actor_run_id
            .clone()
            .ok_or_else(|| apify_client::ApifyClientError::InvalidArgument("ACTOR_RUN_ID is not set".to_owned()))?;
        state().client.run(run_id).reboot().await?;
        tokio::time::sleep(configuration.metamorph_after_sleep).await;
        Ok(())
    }

    fn storage_for(options: OpenOptions) -> crawlee::core::StorageResult<&'static dyn StorageBackend> {
        match &state().smart {
            Some(smart) => smart.suitable(options.force_cloud),
            None => Ok(state().storage.as_ref()),
        }
    }

    pub async fn open_dataset(id: &StorageIdentifier, options: OpenOptions) -> crawlee::core::StorageResult<Dataset> {
        Dataset::open(Self::storage_for(options)?, id).await
    }

    pub async fn open_key_value_store(
        id: &StorageIdentifier,
        options: OpenOptions,
    ) -> crawlee::core::StorageResult<KeyValueStore> {
        KeyValueStore::open(Self::storage_for(options)?, id).await
    }

    pub async fn open_request_queue(
        id: &StorageIdentifier,
        options: OpenOptions,
    ) -> crawlee::core::StorageResult<RequestQueue> {
        RequestQueue::open(Self::storage_for(options)?, id).await
    }

    /// The pay-per-event charges of the run.
    pub fn charging_manager() -> &'static Arc<ChargingManager> {
        &state().charging
    }

    /// Charges for `count` events of `event_name` (see [`ChargingManager::charge`]).
    pub async fn charge(options: ChargeOptions) -> Result<ChargeResult, ChargingError> {
        state().charging.charge(options).await
    }

    /// Pushes one item (an object) or several (an array) to the default dataset. Under
    /// pay-per-event pricing, only as many items are stored as the budget allows, each charged as
    /// an `apify-default-dataset-item` event.
    pub async fn push_data<T: serde::Serialize + ?Sized>(data: &T) -> Result<ChargeResult, ChargingError> {
        Self::push_items_charging(data, &[]).await
    }

    /// Pushes items to the default dataset and charges `event_name` for each, as many as the
    /// budget allows (`Actor.pushData(items, eventName)` in JS).
    pub async fn push_data_and_charge<T: serde::Serialize + ?Sized>(
        data: &T,
        event_name: &str,
    ) -> Result<ChargeResult, ChargingError> {
        Self::push_data_and_charge_events(data, &[event_name]).await
    }

    /// Pushes items to the default dataset and charges each of `events` for each item, as many
    /// items as the budget allows for all of them. `event_charge_limit_reached` is set when the
    /// budget allows no more of one of the events.
    pub async fn push_data_and_charge_events<T: serde::Serialize + ?Sized>(
        data: &T,
        events: &[&str],
    ) -> Result<ChargeResult, ChargingError> {
        if let Some(event) = events.iter().find(|event| event.starts_with("apify-")) {
            return Err(ChargingError::SyntheticEvent((*event).to_owned()));
        }
        Self::push_items_charging(data, events).await
    }

    /// Stores as many items as the budget allows for `events` (plus the default dataset item
    /// event), reserving their charges first and sending them after, so that concurrent pushes
    /// neither wait for each other nor spend more than the budget together.
    async fn push_items_charging<T: serde::Serialize + ?Sized>(
        data: &T,
        events: &[&str],
    ) -> Result<ChargeResult, ChargingError> {
        let mut items = match serde_json::to_value(data)? {
            Value::Array(items) => items,
            item => vec![item],
        };
        let dataset = state().services.open_dataset(&StorageIdentifier::Default).await?;
        let charging = &state().charging;
        if !charging.is_pay_per_event() {
            dataset.push_data(&items).await?;
            // Warns that the Actor is not pay-per-event.
            let mut result = None;
            for event in events {
                result = Some(charging.charge(ChargeOptions::new(*event, items.len() as u64)).await?);
            }
            return Ok(result.unwrap_or_else(|| ChargeResult {
                event_charge_limit_reached: false,
                charged_count: 0,
                chargeable_within_limit: charging.chargeable_within_limit(),
            }));
        }
        // Storing an item also charges the per-item event when the dataset is the Actor's own.
        let is_default_dataset = state().smart.is_some();
        let limit = charging.reserve_items(items.len(), events, is_default_dataset);
        if limit == 0 {
            return Ok(ChargeResult {
                event_charge_limit_reached: !items.is_empty(),
                charged_count: 0,
                chargeable_within_limit: charging.chargeable_within_limit(),
            });
        }
        items.truncate(limit);
        if let Err(err) = ChargingManager::with_items_reserved(dataset.push_data(&items)).await {
            charging.unreserve_items(events, is_default_dataset, limit);
            return Err(err.into());
        }
        charging.send_item_charges(events, is_default_dataset, limit).await?;
        let reached = if events.is_empty() {
            charging.is_event_charge_limit_reached(DEFAULT_DATASET_ITEM_EVENT)
        } else {
            events.iter().any(|event| charging.is_event_charge_limit_reached(event))
        };
        Ok(ChargeResult {
            event_charge_limit_reached: reached,
            // Without the default dataset item event, `push_data` charges nothing.
            charged_count: if events.is_empty() && !is_default_dataset { 0 } else { limit as u64 },
            chargeable_within_limit: charging.chargeable_within_limit(),
        })
    }

    /// A value of the default key-value store.
    pub async fn get_value<T: DeserializeOwned>(key: &str) -> crawlee::core::StorageResult<Option<T>> {
        state().services.open_key_value_store(&StorageIdentifier::Default).await?.get_value(key).await
    }

    /// Stores a value as JSON in the default key-value store.
    pub async fn set_value<T: serde::Serialize + ?Sized>(key: &str, value: &T) -> crawlee::core::StorageResult<()> {
        state().services.open_key_value_store(&StorageIdentifier::Default).await?.set_value(key, value).await
    }

    /// The input of the run, as it was stored. Locally, the input may instead be an `INPUT` or
    /// `INPUT.json` file in the working directory; a storage backend that does not read the default
    /// key-value store's directory (in-memory) also gets the file `apify run` writes there. Input
    /// found in more than one place is an error.
    pub async fn get_input_raw() -> Result<Input, ActorInputError> {
        let configuration = &state().configuration;
        let key = &configuration.input_key;
        let source = format!("the \"{key}\" record of the default key-value store");
        let store = state()
            .services
            .open_key_value_store(&StorageIdentifier::Default)
            .await
            .map_err(|err| ActorInputError::new(ActorInputErrorCode::NotFound, err.to_string()).caused_by(err))?;
        let record = store
            .get_record(key)
            .await
            .map_err(|err| ActorInputError::new(ActorInputErrorCode::NotFound, err.to_string()).caused_by(err))?;
        let local = !configuration.is_at_home;
        // Not a conflict check: under the file system backend, the record is the store directory's file.
        let store_input = match record {
            Some(record) => Some(crate::input::parse_input(record.value, record.content_type.as_deref(), &source)?),
            // Where `apify run` writes the input, for a backend that does not read it (in-memory).
            None if local => {
                let directory = configuration
                    .crawlee
                    .storage_dir
                    .join("key_value_stores")
                    .join(&configuration.default_key_value_store_id);
                crate::input::read_input_file(&directory, "default key-value store directory", key).await?
            }
            None => None,
        };
        let working_directory_input = match local {
            true => crate::input::read_input_file(&working_directory(), "working directory", key).await?,
            false => None,
        };
        if store_input.is_some() && working_directory_input.is_some() {
            return Err(ActorInputError::new(
                ActorInputErrorCode::MultipleFiles,
                format!(
                    "Found the input both in the default key-value store and in a \"{key}\" or \"{key}.json\" file \
                     in the working directory. Keep only one of them."
                ),
            ));
        }
        let Some(input) = store_input.or(working_directory_input) else {
            let mut locations = vec![source];
            if local {
                locations.push(format!(
                    "a \"{key}\" or \"{key}.json\" file in the working or default key-value store directory"
                ));
            }
            return Err(ActorInputError::new(
                ActorInputErrorCode::NotFound,
                format!("Input does not exist. Expected {}.", locations.join(" or ")),
            ));
        };
        let input = Self::decrypt_secrets(input)?;
        Ok(Self::with_schema_defaults(input))
    }

    /// Decrypts the secret fields, when the platform passed the key.
    fn decrypt_secrets(input: Input) -> Result<Input, ActorInputError> {
        let configuration = &state().configuration;
        let (Some(key_file), Some(passphrase)) =
            (&configuration.input_secrets_private_key_file, &configuration.input_secrets_private_key_passphrase)
        else {
            return Ok(input);
        };
        let Input::Json(Value::Object(object)) = input else { return Ok(input) };
        if object.is_empty() {
            return Ok(Input::Json(Value::Object(object)));
        }
        let failed = |err: crate::input_secrets::SecretsError| {
            ActorInputError::new(
                ActorInputErrorCode::DecryptionFailed,
                "Failed to decrypt the secret fields of the input.",
            )
            .caused_by(err)
        };
        let key = crate::input_secrets::InputSecretsKey::from_base64_pem(key_file, passphrase).map_err(failed)?;
        let decrypted = crate::input_secrets::decrypt_input_secrets(object, &key).map_err(failed)?;
        Ok(Input::Json(Value::Object(decrypted)))
    }

    /// The input of the run as `T`, from JSON (see [`get_input_raw`](Self::get_input_raw)).
    /// Locally, missing top-level fields get their defaults from the Actor's input schema.
    pub async fn get_input<T: DeserializeOwned>() -> Result<T, ActorInputError> {
        let input = Self::get_input_raw().await?;
        let json = input
            .into_json()
            .ok_or_else(|| ActorInputError::new(ActorInputErrorCode::ParseFailed, "The input is binary, not JSON."))?;
        serde_json::from_value(json).map_err(|err| {
            ActorInputError::new(
                ActorInputErrorCode::ParseFailed,
                format!("The input does not match the expected type: {err}"),
            )
            .caused_by(err)
        })
    }

    /// The platform applies the defaults itself.
    fn with_schema_defaults(input: Input) -> Input {
        let Input::Json(Value::Object(object)) = input else { return input };
        if state().configuration.is_at_home || object.is_empty() {
            return Input::Json(Value::Object(object));
        }
        match crate::input::read_input_schema(&working_directory()) {
            InputSchema::Found(schema) => Input::Json(Value::Object(crate::input::apply_defaults(object, &schema))),
            InputSchema::NotDefined => Input::Json(Value::Object(object)),
            InputSchema::Missing => {
                tracing::warn!(
                    "Failed to find the input schema for the local run of this Actor. Your input will be missing \
                     fields that have default values set if they are missing from the input you are using."
                );
                Input::Json(Value::Object(object))
            }
        }
    }
}

fn working_directory() -> PathBuf {
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}
