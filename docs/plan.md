# Apify SDK for Rust: plan

**Goal.** A Rust crate that ports the JS SDK, runs real Actors on Apify with crawlee-rs crawlers, and uses `apify-client` (the Rust client) as its API layer. The shape follows JS: the SDK plugs Apify storage, platform events and proxies into crawlee-rs, and wraps the Actor lifecycle around them.

This is based on reading the whole JS SDK (about 5,900 lines of TypeScript), the whole Rust client, and the extension points crawlee-rs already has.

## Architecture

```
apify (crate)
├── Configuration     ACTOR_* / APIFY_* / CRAWLEE_* env vars → builds crawlee-rs Configuration too
├── Actor             init / exit / fail / main, input, storages, call/start/metamorph/reboot, webhooks, status
├── storage/
│   ├── ApifyStorageBackend      implements crawlee-rs StorageBackend on apify-client
│   │   ├── dataset / kvs        thin mapping
│   │   └── request_queue/       single (local head estimate) + shared (server-side locks)
│   ├── SmartStorageBackend      local (crawlee-rs FS) vs cloud, by isAtHome / forceCloud
│   └── ChargingStorageBackend   charges pay-per-event dataset items on push
├── events            platform websocket → crawlee-rs EventManager
├── charging          pay-per-event ChargingManager
├── proxy             Apify Proxy ProxyConfiguration (implements crawlee-rs ProxySource)
└── input             INPUT record/file, secrets decryption, schema defaults, URL filters
```

**How it hooks into crawlee-rs:**
- `Actor::init()` builds `Services::from_parts(config, smart_backend, platform_events)` and installs it with `Services::set_global`. Crawlers built afterwards use it with no extra code.
- An existing crawlee-rs crawler becomes an Actor by wrapping it in `Actor::main`, or between `Actor::init` and `Actor::exit`.

## Straightforward

These are direct mappings: the client already has the endpoint, or crawlee-rs already has the hook.

| Feature | Notes |
|---|---|
| **Configuration** | About 45 options, env var precedence ACTOR_* > APIFY_* > CRAWLEE_*, and platform defaults (`availableMemoryRatio = 1` on the platform). It also builds the crawlee-rs `Configuration`, whose fields are public. |
| **`get_env()`, `is_at_home()`** | Typed struct. Values ending in `_AT` are dates, some are integers, empty means none. |
| **`new_client()`** | `ApifyClient::builder()` with token, API base URLs and the `SDK/x Crawlee/y` user-agent suffix. The client doesn't read env vars itself; the SDK passes them in. |
| **Dataset backend** | `push_items` with pre-serialized items. crawlee-rs items are already `RawValue`, so a `Vec<&RawValue>` goes out without re-serializing. Chunking at about 9 MB, and a per-item size check with the JS error message. `purge` fails on the platform, as in JS. |
| **Key-value store backend** | The client already does raw bytes plus content type, key listing, `record_exists` and signed public URLs. |
| **Storage identifiers** | Default ids come from env vars; names map to `get_or_create(name)`; aliases resolve through `ACTOR_STORAGES_JSON`, then a persisted `__STORAGE_ALIASES_MAPPING` record, then a new unnamed storage. |
| **`get_input()`** | Resolution order: the `INPUT` record (key from `ACTOR_INPUT_KEY`), then `INPUT` / `INPUT.json` in the working directory, then in the storage directory, when local, then an `ActorInputError` (`NotFound`, `MultipleFiles`, `ParseFailed`). Schema defaults from `.actor/actor.json` or `INPUT_SCHEMA.json` are applied locally, top level only, as in JS. |
| **`set_value` / `get_value` / `push_data` / `open_*`** | Thin wrappers over the default storages, plus `force_cloud`. |
| **Platform API wrappers** | `call`, `start`, `call_task`, `abort`, `metamorph`, `reboot`, `add_webhook` and `set_status_message` wrap the client. `timeout: Inherit` uses `ACTOR_TIMEOUT_AT`. `metamorph` and `reboot` then sleep up to 5 minutes, waiting for the container to be killed. |
| **Status messages** | crawlee-rs already emits `StatusMessage` events. The SDK forwards them to the run, with the JS 1 s timeout and best-effort error handling. |
| **`use_state`** | Maps to crawlee-rs `Services::auto_saved_value` under `APIFY_GLOBAL_STATE`. |
| **Exit codes and `main`** | 0 for success, 1 for `fail`, 91 when the user function errors. `exit` waits for listeners, tears storage down, sets a terminal status message, then exits. A watchdog thread kills the process after the timeout (30 s) if exit hangs. |
| **Proxy URL composition** | `groups-A+B,session-X,country-US_CA`. The password comes from `APIFY_PROXY_PASSWORD` or from `users/me` (`extra.proxy.password`). The access check calls the proxy status URL with a 4 s timeout and 2 attempts. |
| **URL filters** | Globs are already in crawlee-rs. Pseudo-URLs need a port of `@apify/pseudo_url`, which is about 50 lines. |
| **Init logging** | The system-info line, and the outdated-SDK warning driven by `APIFY_SDK_LATEST_VERSION`. |

## Challenging

| Feature | Why it's hard | Plan |
|---|---|---|
| **Request queue, single mode** (the default) | It keeps a local estimate of the queue head (the next requests to fetch) plus a cache of up to 1M requests, because the platform's head listing lags behind writes. Crawlee checks `is_finished` in a loop, so a wrong estimate ends the crawl early or never. | Port the JS algorithm line by line. Test it against a fake API that deliberately lags, and in live tests on the platform. |
| **Request queue, shared mode** | Server-side locks (180 s, heads of 25) and the forefront re-check. JS never extends locks; crawlee-rs has `extend_request_processing_time`, so the Rust port can. | Port it, and extend a lock when a handler runs longer than the lock. |
| **Platform events** | Websocket `{name, data}` messages → crawlee-rs typed `Event` enum. On `migrating`: stop the periodic save, emit a save with `is_migrating: true`, then reboot the run. On `aborting`: exit. JS doesn't reconnect. | Use `tokio-tungstenite` with rustls. Convert the platform's `systemInfo` payload to crawlee-rs `SystemInfo`. Needs two small crawlee-rs changes, listed under breaking changes. |
| **Pay-per-event charging** | Budget math against the max total charge (JS rounds with `toFixed`); over-charging by one on purpose so the platform stops the run; restoring counts after a restart; synthetic `apify-default-dataset-item` charges on push; items whose charges the Actor reserved (JS uses `AsyncLocalStorage`). | Port the formulas exactly, with golden cases generated from the JS `ChargingManager`. Count a charge against the budget in one short critical section and send it without holding a lock, so concurrent pushes do not wait for each other's API calls; mark items reserved by the Actor with a task-local, so that the dataset does not charge them again. Charge dataset items in the backend at transaction commit, as JS does. |
| **Autoscaling on the platform** | crawlee-rs samples CPU and memory locally, but on the platform `systemInfo` comes from the websocket. The storage load signal needs a count of 429s, which the client doesn't expose. | Use `EventManager::new` (no local sampler) on the platform. Count 429s in a custom client transport (the `HttpBackend` hook) and report them as `rate_limit_errors`. |
| **Exit semantics in Rust** | `std::process::exit` skips destructors. The runtime may be busy, so a watchdog timer inside tokio might never fire. | Flush explicitly: close events (final save, waits for listeners), tear storage down, set the status message, then `exit`. The watchdog is a plain OS thread. |
| **Input secrets** | `@apify/input_secrets` uses RSA plus AES. The RustCrypto `rsa` crate carries a timing side-channel advisory (RUSTSEC-2023-0071). | Use `aws-lc-rs`, which is constant-time and already in the dependency tree through rustls. Golden-test it with secrets encrypted by the JS library. |
| **Init ordering** | `Services::global()` initializes itself on first read. A crawler built before `Actor::init()` silently gets local storage. | `init` detects this and fails with the JS message ("init() was called after a method that would access a storage…"). Needs a small crawlee-rs helper. |
| **Pinning the Rust client** | It's "experimental, AI-maintained", pre-1.0 and has breaking releases. crates.io has 0.10.0 while git is at 0.10.2. It uses reqwest 0.12 with native-tls by default; crawlee-rs uses reqwest 0.13 with rustls. | Pin an exact version. Plug in our own `HttpBackend` on crawlee-rs's reqwest 0.13, so only one HTTP stack is compiled. That backend also counts 429s and allows per-call timeouts. Parse its untyped fields (pricing info, run options, proxy password, request fields) with our own structs. |

## Potentially breaking, or unsafe

**`unsafe`: none needed.** `#![forbid(unsafe_code)]` holds in the SDK. The only native code is in dependencies (`aws-lc-rs`, and `sysinfo` through crawlee-rs). The one real hazard is `process::exit` skipping `Drop`, which the explicit flush above handles.

**Changes to crawlee-rs.** It's ours and pre-1.0, so these are cheap:
1. **Stop periodic saves.** `EventManager` needs a way to stop the periodic `PersistState` without closing, for `migrating`.
2. **Unknown platform events.** `Event` becomes `#[non_exhaustive]`, with an `Event::Custom { name, data }` variant, so platform events we don't model aren't dropped. It breaks exhaustive `match`es.
3. **Detect early storage use.** A `Services::global_is_set()`, or a `try_global()`, so `init` can detect storage used before it.
4. **Platform storage hooks.** Possibly a `StorageBackend::stats()` extension. crawlee-rs's `purge_on_start` semantics already fit: on the platform the cloud backend's purge is a no-op, as in JS.

**Deliberate API differences from JS, to agree on:**
- **Entry point.** JS has a static singleton (`Actor.pushData`). I'd make `Actor` a cheap cloneable handle returned by `Actor::init()` and passed into the closure in `apify::main(|actor| async move { … })`, plus `Actor::current()` for code that can't receive it. Since then, `Actor` is static as in JS: `Actor::init()` once, then `Actor::get_input()`, `Actor::push_data()`... from anywhere, panicking when not initialized. A `#[apify::main]` macro could come later.
- **JS bugs to fix, not copy:**
  - "At home" is read two ways (the env var in some places, the configuration in others). Make it one.
  - `abort({statusMessage})` sets the message on the current run instead of the aborted one.
  - `main()` drops the user's exit options when the function errors.
  - `use_state` ignores the instance's configuration.

  Each fix would be listed in an allowed-differences file.
- **No crawlee version check**, since Cargo handles that.
- **`push_data` with an event name.** In JS it returns a `ChargeResult` and refuses to run inside a transaction. In Rust, `ctx.push_data` in crawler handlers stays transactional and charges only the synthetic dataset-item event at commit, as in JS. Charging for a custom event goes through `Actor::push_data_and_charge(item, event)`, outside the transaction, which also matches JS.

## Checking that it behaves like the JS SDK

1. **Golden files from JS**, as in crawlee-rs. Cases: configuration from a given env, `get_env`, proxy URLs and usernames, charging math (`calculatePushDataLimit`, max counts, over-charge-by-one), input parsing and schema defaults, pseudo-URLs, request id derivation, and decrypting JS-encrypted secrets.
2. **A fake Apify API**, a local server covering the storage endpoints. It deliberately lags the queue head and can inject 429s and 500s. The request queue backends, alias persistence and charging run against it in CI, with no token.
3. **Live integration tests** gated on `APIFY_TOKEN`: real storages, both queue modes with 2 concurrent consumers, locks and charging, similar to the client's own live tests.
4. **End-to-end Actors on the platform**: build and run small test Actors (Dockerfile plus `.actor/`) with the Apify CLI. Scenarios: input and output, migration, abort, metamorph, PPE charging, proxy. Where JS has an equivalent e2e test, compare outputs.

## Milestones

| Milestone | Scope | Done when |
|---|---|---|
| **M1: runs real Actors** | Configuration, client, dataset and KVS backends, single-mode queue, aliases, `init`/`exit`/`fail`/`main`, `get_input`, platform events (migrate, abort, `systemInfo`), status messages, the 429 counter, a Rust Actor template (Dockerfile, `actor.json`, input schema) | The crawlee-dev scraper runs on Apify as an Actor, survives a migration, and its results show up in the default dataset |
| **M2: platform API** | Proxy configuration, `call`/`start`/`call_task`/`abort`/`metamorph`/`reboot`, webhooks, `use_state`, `force_cloud`, shared-mode queue | Live tests pass |
| **M3: monetization and input** | Pay-per-event charging (including the charging log dataset used locally), input secrets, pseudo-URL filters | Golden and fake-API suites green |
| **M4: hardening** | E2E suite on the platform, docs, README, publishing | |

M1 is the smallest slice that meets "run real Actors with crawlee-rs". Everything after it is incremental.

## Decisions

Agreed before implementation started:

1. **Crate name:** `apify`, a single crate. Not published to crates.io yet.
2. **API style:** static, as in JS: `Actor::init()` once, then `Actor::get_input()`,
   `Actor::push_data()`... from anywhere, and `Actor::exit()` (or `Actor::main` for all three).
3. **JS quirks:** the clear bugs listed above are fixed, not copied, and each one is listed in
   `docs/allowed-differences.md`.
4. **Platform access:** live and e2e tests are gated on `APIFY_TOKEN`, which will be provided later.
   Until then everything runs against the fake API.
5. **apify-client:** pinned to a git revision. Gaps in it are worked around in this crate.
6. **Scope:** start with M1 and continue through M4.

The crawlee-rs changes 1 to 3 above are done (`EventManager::stop_periodic_persist_state`,
`Event::Custom`, `Services::is_global_set`).

## Status

| Milestone | State |
|---|---|
| M1: runs real Actors | Done. Verified locally and against the fake API; not yet run on the platform |
| M2: platform API | Done, including shared request queues |
| M3: monetization and input | Done: pay-per-event charging, input secrets, pseudo-URL filters |
| M4: hardening | Golden tests against the JS SDK, live and e2e harnesses, CI and docs done. The live and e2e suites wait for an Apify token (`APIFY_TOKEN` secret in CI). Not published |

Changes made to crawlee-rs for the SDK: `Event::Custom`, `EventManager::stop_periodic_persist_state`,
`Services::is_global_set`, a public `Configuration::from_sources`, `crawlee::VERSION`, and input
keys of the file-system storage (`FileSystemStorageBackend::with_input_keys`).
