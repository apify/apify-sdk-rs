//! End to end on the Apify platform: builds `templates/actor` as an Actor (with the SDK at a
//! pushed commit), runs it, and checks its results. Skipped unless `APIFY_TOKEN` is set and
//! `APIFY_E2E=1`. Takes several minutes: the platform compiles the Actor.
//!
//! ```sh
//! APIFY_TOKEN=... APIFY_E2E=1 cargo test --test e2e -- --nocapture
//! ```
//!
//! `APIFY_E2E_REV` sets the SDK commit (default: `HEAD`, which must be pushed).

use std::path::Path;
use std::time::Duration;

use apify::{ApifyClient, Configuration};
use apify_client::clients::actor::ActorBuildOptions;
use serde_json::{Value, json};

fn client() -> Option<ApifyClient> {
    if std::env::var("APIFY_E2E").as_deref() != Ok("1") {
        eprintln!("APIFY_E2E is not 1; skipping the end-to-end test");
        return None;
    }
    let configuration = Configuration::from_env();
    configuration.token.as_ref()?;
    Some(apify::new_client(&configuration, None, Default::default()))
}

fn sdk_revision() -> String {
    std::env::var("APIFY_E2E_REV").unwrap_or_else(|_| {
        let output = std::process::Command::new("git").args(["rev-parse", "HEAD"]).output().unwrap();
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    })
}

/// The template's files, with the SDK pinned to `revision`.
fn source_files(revision: &str) -> Vec<Value> {
    let template = Path::new(env!("CARGO_MANIFEST_DIR")).join("templates/actor");
    ["Cargo.toml", "Dockerfile", "src/main.rs", ".actor/actor.json", ".actor/input_schema.json"]
        .iter()
        .map(|name| {
            let mut content = std::fs::read_to_string(template.join(name)).unwrap();
            if *name == "Cargo.toml" {
                content = content.replace(
                    r#"apify = { git = "https://github.com/apify-projects/apify-sdk-rs" }"#,
                    &format!(
                        r#"apify = {{ git = "https://github.com/apify-projects/apify-sdk-rs", rev = "{revision}" }}"#
                    ),
                );
            }
            json!({ "name": name, "format": "TEXT", "content": content })
        })
        .collect()
}

async fn wait_for_status(client: &ApifyClient, run_id: &str, wanted: &str) {
    for _ in 0..120 {
        let run = client.run(run_id).get().await.unwrap().unwrap();
        if run.status.as_deref() == Some(wanted) {
            return;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    panic!("run {run_id} did not reach {wanted}");
}

#[tokio::test(flavor = "local")]
async fn the_template_runs_on_the_platform_and_survives_a_reboot() {
    let Some(client) = client() else { return };
    let millis = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
    let actor = client
        .actors()
        .create(&json!({
            "name": format!("apify-sdk-rs-e2e-{millis}"),
            "versions": [{
                "versionNumber": "0.1",
                "sourceType": "SOURCE_FILES",
                "buildTag": "latest",
                "sourceFiles": source_files(&sdk_revision()),
            }],
        }))
        .await
        .unwrap();
    let actor_client = client.actor(actor.id.clone());

    let result = {
        let (client, actor_client) = (client.clone(), actor_client.clone());
        async move {
            let build = actor_client.build("0.1", ActorBuildOptions::default()).await.unwrap();
            let build = client.build(build.id).wait_for_finish(None).await.unwrap();
            assert_eq!(build.status.as_deref(), Some("SUCCEEDED"), "build {build:?}");

            // A short crawl.
            let input = json!({ "startUrls": [{ "url": "https://crawlee.dev" }], "maxRequestsPerCrawl": 3 });
            let run = actor_client.call(Some(&input), Default::default(), None).await.unwrap();
            assert_eq!(run.status.as_deref(), Some("SUCCEEDED"), "run {run:?}");
            let dataset = run.default_dataset_id.clone().unwrap();
            let items = client.dataset(dataset).list_items::<Value>(Default::default()).await.unwrap().items;
            assert_eq!(items.len(), 3);
            assert!(items.iter().all(|item| item["title"].is_string()), "{items:?}");

            // A longer one, rebooted half way: it resumes from its queue and saved state.
            let input = json!({ "startUrls": [{ "url": "https://crawlee.dev" }], "maxRequestsPerCrawl": 40 });
            let run = actor_client.start(Some(&input), Default::default()).await.unwrap();
            wait_for_status(&client, &run.id, "RUNNING").await;
            tokio::time::sleep(Duration::from_secs(5)).await;
            client.run(run.id.clone()).reboot().await.unwrap();
            let run = client.run(run.id.clone()).wait_for_finish(None).await.unwrap();
            assert_eq!(run.status.as_deref(), Some("SUCCEEDED"), "run {run:?}");
            let dataset = run.default_dataset_id.clone().unwrap();
            let items = client.dataset(dataset).list_items::<Value>(Default::default()).await.unwrap().items;
            let mut urls: Vec<&str> = items.iter().filter_map(|item| item["url"].as_str()).collect();
            let total = urls.len();
            urls.sort();
            urls.dedup();
            assert!(total >= 40, "the crawl finished after the reboot: {total} items");
            // Pages in flight during the reboot are crawled again; most must not be.
            assert!(total - urls.len() <= 10, "{} duplicates", total - urls.len());
        }
    };
    let outcome = tokio::task::spawn_local(result).await;
    actor_client.delete().await.unwrap();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic.into_panic());
    }
}
