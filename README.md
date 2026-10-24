# Apify SDK for Rust

Run [crawlee-rs](https://github.com/apify-projects/crawlee-rs) crawlers as Actors on the
[Apify platform](https://apify.com). A port of the [JS SDK](https://github.com/apify/apify-sdk-js),
using the Rust [Apify API client](https://github.com/apify/apify-client-rust).

**Status: feature-complete port, not yet published or run on the platform.** See
[docs/plan.md](docs/plan.md) for the scope, and
[docs/allowed-differences.md](docs/allowed-differences.md) for deliberate differences from the JS
SDK.

## Install

The crate is not on crates.io yet. Depend on it by git:

```toml
[dependencies]
apify = { git = "https://github.com/apify-projects/apify-sdk-rs" }
```

Use crawlee-rs through `apify::crawlee`, so that the SDK and your crawler share one version of it.

## Usage

```rust
use apify::crawlee::{EnqueueLinksOptions, HtmlContext, HtmlCrawler};
use apify::{Actor, ExitOptions, InitOptions};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Actor::init(InitOptions::default()).await?;
    match run().await {
        Ok(()) => Actor::exit(ExitOptions::default()).await,
        Err(err) => {
            tracing::error!("{err:?}");
            Actor::fail(ExitOptions::default()).await;
        }
    }
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    // `Actor` is the run of this process, like the static `Actor` of the JS SDK.
    let input: serde_json::Value = Actor::get_input().await?;
    let start = input["startUrl"].as_str().unwrap_or("https://crawlee.dev").to_owned();

    // Crawlers store their data where the Actor does: in the platform storages on Apify,
    // in ./storage elsewhere.
    let crawler = HtmlCrawler::builder()
        .services(Actor::services().clone())
        .request_handler(|ctx: HtmlContext| async move {
            let title = ctx.with_html(|doc| doc.title()).await?;
            ctx.push_data(&serde_json::json!({ "url": ctx.url().as_str(), "title": title }))?;
            ctx.enqueue_links(EnqueueLinksOptions::new()).await?;
            Ok(())
        })
        .build()?;
    crawler.run([start]).await?;
    Ok(())
}
```

`Actor::main(|| async { ... })` does the same in one call, with exit code 91 when the function
fails.

[`templates/actor`](templates/actor) is a complete Actor: Dockerfile, `.actor/actor.json` and an
input schema. Copy it and `apify push`.

## Features

| | |
|---|---|
| **Lifecycle** | `Actor::init` / `exit` / `fail` / `reboot` / `main`, the Actor's functions callable from anywhere after `init` (as the static `Actor` of JS), exit codes (91 when the user function fails), a watchdog for hung exits |
| **Configuration** | Every `ACTOR_*` / `APIFY_*` variable of the JS SDK, with its precedence and defaults (all memory on the platform) |
| **Storages** | Platform datasets, key-value stores and request queues behind the crawlee-rs storage traits; names, aliases (`ACTOR_STORAGES_JSON`, remembered across migrations), `force_cloud`; local files with the input kept on purge |
| **Request queues** | Single-consumer mode (local head estimate, no locks) and shared mode (platform locks, prolonged for long requests) |
| **Events** | The platform websocket (reopened when it drops) relayed as crawlee-rs events: system info for autoscaling, `migrating` (state saved, run rebooted), `aborting` (graceful exit) |
| **Input** | `get_input` from the store or `INPUT(.json)`, schema defaults applied locally, secret fields decrypted |
| **Platform API** | `call` (the run's log redirected into this one's), `start`, `call_task`, `start_task`, `resurrect`, `abort`, `metamorph`, `add_webhook`, `set_status_message` (crawler status forwarded), `get_env`, `use_state`; named child runs (`run_name`, `child_runs`) that survive a migration or resurrection |
| **Proxy** | Apify Proxy (groups, country, password from the account, access check) and custom proxies, for crawlers |
| **Charging** | Pay-per-event: budget limits, the `apify-default-dataset-item` event charged as items are stored, `push_data_and_charge`, the local charging log |
| **URL filters** | Globs and pseudo-URLs with request options, as Actor inputs send them |
| **Autoscaling** | Platform system info drives the CPU and memory signals. Rate-limited API calls are counted by attempt as in the JS client (`ApifyStorageBackend::rate_limits`) but, as in the JS SDK, not reported to the storage load signal |

## Testing

| Layer | What runs | Needs |
|---|---|---|
| Unit and integration | Everything against a fake Apify API (lagging queue head, rate limits, locks); Actor scenarios in child processes | nothing |
| Golden | Outputs recorded from the JS SDK, replayed ([conformance/](conformance)) | nothing (regenerating needs Node 22) |
| Live | Storages and both queue modes against the real API ([tests/live.rs](tests/live.rs)) | `APIFY_TOKEN` |
| End to end | The template built and run as an Actor, rebooted mid-crawl ([tests/e2e.rs](tests/e2e.rs)) | `APIFY_TOKEN`, `APIFY_E2E=1` |

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
APIFY_TOKEN=... cargo test --test live -- --nocapture
APIFY_TOKEN=... APIFY_E2E=1 cargo test --test e2e -- --nocapture
```

Minimum supported Rust version: 1.88.
