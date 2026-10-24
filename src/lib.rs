//! Apify SDK for Rust: run [crawlee-rs](https://github.com/apify-projects/crawlee-rs) crawlers as
//! Actors on the [Apify platform](https://apify.com), a port of the JS SDK (`apify` on npm).
//!
//! See `docs/plan.md` for the scope and the milestones.

pub mod actor;
pub mod charging;
pub mod child_runs;
pub mod client;
pub mod configuration;
pub mod events;
pub mod input;
pub mod input_secrets;
pub mod platform;
pub mod proxy;
pub mod storage;
pub mod url_filters;

pub use crate::actor::{
    Actor, ActorListenerId, ExitOptions, InitError, InitOptions, OpenOptions, StatusMessageOptions, exit_codes,
};
pub use crate::child_runs::{ChildRunInfo, TrackedChildRun};
pub use crate::client::{RateLimitCounter, new_client};
pub use crate::configuration::Configuration;
pub use crate::input::{ActorInputError, ActorInputErrorCode, Input};
pub use crate::platform::{
    AbortOptions, ApifyEnv, CallOptions, MetamorphOptions, ResurrectOptions, RunTimeout, WebhookOptions,
};
pub use crate::proxy::{ProxyConfiguration, ProxyConfigurationOptions};
pub use crate::storage::{ApifyStorageBackend, RequestQueueAccess, SmartStorageBackend};

pub use apify_client::ApifyClient;

/// The crawlee-rs version this SDK is built on. Use it through this re-export, so that the
/// crawler and the SDK share one version of crawlee-rs.
pub use crawlee;
