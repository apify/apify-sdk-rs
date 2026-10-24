//! The Actor lifecycle, end to end. An Actor owns the process (global services, `exit`), so every
//! scenario runs in a child process: this test binary again, running only [`child`].

mod common;

use std::path::Path;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

use apify::Actor;
use serde_json::{Value, json};

use common::{DEFAULT_DATASET, DEFAULT_QUEUE, DEFAULT_STORE, FakeApi, TOKEN};

const SCENARIO: &str = "APIFY_SDK_TEST_SCENARIO";

/// Runs `scenario` of [`child`] in a new process, with `vars` as its whole environment. The wait
/// is off the runtime, which serves the fake API meanwhile.
async fn run_child(scenario: &str, vars: &[(&str, String)], cwd: &Path) -> Output {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.args(["child", "--exact", "--nocapture", "--test-threads=1"]).env_clear().current_dir(cwd);
    command.env(SCENARIO, scenario).env("RUST_BACKTRACE", "1");
    for (name, value) in vars {
        command.env(name, value);
    }
    tokio::task::spawn_blocking(move || command.output().unwrap()).await.unwrap()
}

fn platform_env(api: &FakeApi, events_url: Option<String>) -> Vec<(&'static str, String)> {
    let mut vars = vec![
        ("APIFY_IS_AT_HOME", "1".to_owned()),
        ("APIFY_TOKEN", TOKEN.to_owned()),
        ("APIFY_API_BASE_URL", api.url.clone()),
        ("APIFY_API_PUBLIC_BASE_URL", api.url.clone()),
        ("ACTOR_DEFAULT_DATASET_ID", DEFAULT_DATASET.to_owned()),
        ("ACTOR_DEFAULT_KEY_VALUE_STORE_ID", DEFAULT_STORE.to_owned()),
        ("ACTOR_DEFAULT_REQUEST_QUEUE_ID", DEFAULT_QUEUE.to_owned()),
        ("ACTOR_RUN_ID", "run-1".to_owned()),
        ("ACTOR_MEMORY_MBYTES", "1024".to_owned()),
        ("APIFY_METAMORPH_AFTER_SLEEP_MILLIS", "100".to_owned()),
    ];
    if let Some(url) = events_url {
        vars.push(("ACTOR_EVENTS_WEBSOCKET_URL", url));
    }
    vars
}

fn assert_exit(output: &Output, code: i32) {
    assert_eq!(
        output.status.code(),
        Some(code),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn terminal_messages(api: &FakeApi) -> Vec<String> {
    api.run_updates()
        .iter()
        .filter(|update| update["isStatusMessageTerminal"] == true)
        .map(|update| update["statusMessage"].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test]
async fn input_and_output_on_the_platform() {
    let api = FakeApi::start().await;
    api.put_record(DEFAULT_STORE, "INPUT", br#"{"greeting":"hi"}"#, "application/json; charset=utf-8");
    let dir = tempfile::tempdir().unwrap();

    let output = run_child("echo", &platform_env(&api, None), dir.path()).await;
    assert_exit(&output, 0);
    assert_eq!(api.dataset_items(DEFAULT_DATASET), [json!({ "echo": { "greeting": "hi" } })]);
    assert_eq!(terminal_messages(&api), ["Echoed"]);
    assert!(!dir.path().join("storage").exists(), "nothing is stored locally");
}

#[tokio::test]
async fn a_failing_user_function_exits_with_91() {
    let api = FakeApi::start().await;
    let dir = tempfile::tempdir().unwrap();
    let output = run_child("fail", &platform_env(&api, None), dir.path()).await;
    assert_exit(&output, 91);
    assert!(String::from_utf8_lossy(&output.stdout).contains("boom"));
    // The exit options of `main_with` are kept.
    assert_eq!(terminal_messages(&api), ["Done"]);
}

#[tokio::test]
async fn fail_exits_with_1_or_the_code_given() {
    let api = FakeApi::start().await;
    let dir = tempfile::tempdir().unwrap();
    assert_exit(&run_child("fail-default", &platform_env(&api, None), dir.path()).await, 1);
    assert_exit(&run_child("fail-code", &platform_env(&api, None), dir.path()).await, 3);
}

/// `Actor::init` and `exit` without `Actor::main`, and the functions of the Actor in between.
#[tokio::test]
async fn an_actor_initialized_and_exited_explicitly() {
    let api = FakeApi::start().await;
    api.put_record(DEFAULT_STORE, "INPUT", br#"{"greeting":"hi"}"#, "application/json; charset=utf-8");
    let dir = tempfile::tempdir().unwrap();

    let output = run_child("init-exit", &platform_env(&api, None), dir.path()).await;
    assert_exit(&output, 0);
    assert_eq!(api.dataset_items(DEFAULT_DATASET), [json!({ "echo": { "greeting": "hi" } })]);
    assert_eq!(terminal_messages(&api), ["Echoed"]);
}

#[tokio::test]
async fn listeners_registered_before_init_are_attached_by_init() {
    let dir = tempfile::tempdir().unwrap();
    let output = run_child("listen-before-init", &[], dir.path()).await;
    assert_exit(&output, 0);
    let stdout = String::from_utf8_lossy(&output.stdout);
    // The listeners of the exit see its options.
    assert!(stdout.contains(r#"EXIT LISTENER kept Some("Bye")"#), "{stdout}");
    assert!(!stdout.contains("EXIT LISTENER removed"), "{stdout}");
}

#[tokio::test]
async fn status_messages_are_logged_at_their_level_and_return_the_updated_run() {
    let dir = tempfile::tempdir().unwrap();
    let output = run_child("status-local", &[], dir.path()).await;
    assert_exit(&output, 0);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("[Status message]: at info by default"), "{stdout}");
    assert!(!stdout.contains("[Status message]: at debug"), "{stdout}");
}

#[tokio::test]
async fn named_child_runs_are_reused_resurrected_or_replaced_after_a_restart() {
    let api = FakeApi::start().await;
    let dir = tempfile::tempdir().unwrap();
    let vars = platform_env(&api, None);
    let output = run_child("child-runs", &vars, dir.path()).await;
    assert_exit(&output, 0);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("RUNS child-1 child-2 child-3"), "{stdout}");
    assert!(stdout.contains(r#"The run name "a" was already used for a different Actor, task or input."#), "{stdout}");
    assert!(stdout.contains(r#"CHILD RUNS ["a", "b", "c"]"#), "{stdout}");
    let starts = |api: &FakeApi| -> Vec<String> { api.platform_calls().into_iter().map(|(path, _, _)| path).collect() };
    assert_eq!(starts(&api), ["/v2/actors/user~a/runs", "/v2/actor-tasks/user~b/runs", "/v2/actors/user~c/runs"]);

    // After a restart: the aborted run is resurrected, the failed one replaced and the succeeded one kept.
    api.set_run_status("child-1", "ABORTED");
    api.set_run_status("child-2", "FAILED");
    let output = run_child("child-runs", &vars, dir.path()).await;
    assert_exit(&output, 0);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("RUNS child-1 child-4 child-3"), "{stdout}");
    assert_eq!(starts(&api)[3..], ["/v2/actor-runs/child-1/resurrect", "/v2/actor-tasks/user~b/runs"]);
    let (stored, _) = api.record(DEFAULT_STORE, "__ACTOR_CHILD_RUNS").unwrap();
    let stored: Value = serde_json::from_slice(&stored).unwrap();
    assert_eq!(stored["a"]["status"], "RUNNING");
    assert_eq!(stored["b"]["runId"], "child-4");
    assert_eq!(
        stored["b"]["history"],
        json!([{ "runId": "child-2", "status": "FAILED", "startedAt": "2026-01-01T00:00:00.000Z" }])
    );
}

#[tokio::test]
async fn the_actor_panics_before_init() {
    let dir = tempfile::tempdir().unwrap();
    let output = run_child("before-init", &[], dir.path()).await;
    assert_exit(&output, 101);
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("Actor::init() must be called before the other functions of Actor")
    );
}

#[tokio::test]
async fn an_aborted_run_exits_gracefully_after_saving_its_state() {
    let api = FakeApi::start().await;
    let events =
        common::serve_events(Duration::from_millis(500), vec![json!({ "name": "aborting", "data": null })]).await;
    let dir = tempfile::tempdir().unwrap();

    let started = Instant::now();
    let output = run_child("wait", &platform_env(&api, Some(events)), dir.path()).await;
    assert_exit(&output, 0);
    assert!(started.elapsed() < Duration::from_secs(20), "did not wait for the user function");
    let (state, _) = api.record(DEFAULT_STORE, "STATE").expect("state saved on exit");
    assert_eq!(serde_json::from_slice::<Value>(&state).unwrap(), json!({ "count": 1 }));
}

#[tokio::test]
async fn a_migrating_run_saves_its_state_and_reboots() {
    let api = FakeApi::start().await;
    // Once the user function has registered its state.
    let events = common::serve_events(Duration::from_millis(500), vec![
        json!({ "name": "systemInfo", "data": { "cpuCurrentUsage": 10.0, "isCpuOverloaded": false, "memCurrentBytes": 1 } }),
        json!({ "name": "migrating", "data": null }),
    ])
    .await;
    let dir = tempfile::tempdir().unwrap();

    let output = run_child("migrate", &platform_env(&api, Some(events)), dir.path()).await;
    assert_exit(&output, 0);
    let calls = api.calls();
    let reboot = calls.iter().position(|call| call == "POST /v2/actor-runs/run-1/reboot").expect("rebooted");
    let saved = calls.iter().position(|call| call == "PUT /v2/key-value-stores/default-store/records/STATE").unwrap();
    assert!(saved < reboot, "the state is saved before the reboot: {calls:?}");
}

#[tokio::test]
async fn a_local_run_reads_its_input_from_the_working_directory() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("INPUT.json"), r#"{"greeting":"hello"}"#).unwrap();
    std::fs::create_dir(dir.path().join(".actor")).unwrap();
    let schema = json!({ "input": { "properties": { "maxPages": { "type": "integer", "default": 5 } } } });
    std::fs::write(dir.path().join(".actor/actor.json"), schema.to_string()).unwrap();

    let output = run_child("echo", &[], dir.path()).await;
    assert_exit(&output, 0);
    let item: Value = serde_json::from_str(
        &std::fs::read_to_string(dir.path().join("storage/datasets/default/000000001.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(item, json!({ "echo": { "maxPages": 5, "greeting": "hello" } }));

    // A second run finds the input in the store, which purging on start keeps.
    std::fs::remove_file(dir.path().join("INPUT.json")).unwrap();
    std::fs::write(dir.path().join("storage/key_value_stores/default/INPUT.json"), r#"{"greeting":"again"}"#).unwrap();
    assert_exit(&run_child("echo", &[], dir.path()).await, 0);
    let item: Value = serde_json::from_str(
        &std::fs::read_to_string(dir.path().join("storage/datasets/default/000000001.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(item["echo"]["greeting"], "again");
    assert!(!dir.path().join("storage/datasets/default/000000002.json").exists(), "the dataset was purged");
}

#[tokio::test]
async fn a_local_run_reads_the_default_store_directory_but_not_next_to_a_working_directory_file() {
    let dir = tempfile::tempdir().unwrap();
    // Where `apify run` writes the input; the in-memory backend never reads it.
    let in_memory = [("CRAWLEE_PERSIST_STORAGE", "false".to_owned())];
    std::fs::create_dir_all(dir.path().join("storage/key_value_stores/default")).unwrap();
    std::fs::write(dir.path().join("storage/key_value_stores/default/INPUT.json"), r#"{"from":"storage"}"#).unwrap();
    // The storage root is not looked at.
    std::fs::write(dir.path().join("storage/INPUT.json"), r#"{"from":"root"}"#).unwrap();
    let output = run_child("echo-stdout", &in_memory, dir.path()).await;
    assert_exit(&output, 0);
    assert!(String::from_utf8_lossy(&output.stdout).contains(r#"ECHO {"from":"storage"}"#));

    std::fs::write(dir.path().join("INPUT.json"), r#"{"from":"cwd"}"#).unwrap();
    for vars in [&in_memory[..], &[]] {
        let output = run_child("echo-stdout", vars, dir.path()).await;
        assert_exit(&output, 91);
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains(
                r#"Found the input both in the default key-value store and in a "INPUT" or "INPUT.json" file in the working directory. Keep only one of them."#
            ),
            "{stdout}"
        );
    }
}

#[tokio::test]
async fn missing_input_and_late_init_are_errors() {
    let dir = tempfile::tempdir().unwrap();
    let output = run_child("echo", &[], dir.path()).await;
    assert_exit(&output, 91);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(
            r#"Input does not exist. Expected the "INPUT" record of the default key-value store or a "INPUT" or "INPUT.json" file in the working or default key-value store directory."#
        ),
        "{stdout}"
    );

    assert_exit(&run_child("late-init", &[], dir.path()).await, 0);
}

#[tokio::test]
async fn the_platform_api() {
    let api = FakeApi::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut vars = platform_env(&api, None);
    vars.push(("ACTOR_TIMEOUT_AT", (chrono::Utc::now() + chrono::Duration::seconds(100)).to_rfc3339()));
    assert_exit(&run_child("platform", &vars, dir.path()).await, 0);

    let calls = api.platform_calls();
    let paths: Vec<&str> = calls.iter().map(|(path, _, _)| path.as_str()).collect();
    assert_eq!(
        paths,
        [
            "/v2/actors/apify~hello-world/runs",
            "/v2/actor-tasks/user~task/runs",
            "/v2/actor-tasks/user~task/runs",
            "/v2/actor-runs/old-run/resurrect",
            "/v2/actor-runs/other-run/abort",
            "/v2/webhooks",
            "/v2/actor-runs/run-1/metamorph",
        ]
    );
    let (_, query, input) = &calls[0];
    assert_eq!(input, &json!({ "a": 1 }));
    let timeout: i64 = query["timeout"].parse().unwrap();
    assert!((95..=100).contains(&timeout), "the remaining time of this run: {timeout}");
    assert_eq!(calls[1].1["memory"], "512");
    assert_eq!((&calls[2].1["timeout"], &calls[2].2), (&"60".to_owned(), &json!({ "b": 2 })));
    assert_eq!((&calls[3].1["timeout"], &calls[3].1["memory"]), (&"60".to_owned(), &"256".to_owned()));
    assert_eq!(calls[4].1["gracefully"], "1");
    assert_eq!(
        calls[5].2,
        json!({ "eventTypes": ["ACTOR.RUN.SUCCEEDED"], "requestUrl": "https://example.com/hook", "isAdHoc": true,
                "condition": { "actorRunId": "run-1" } })
    );
    assert_eq!(calls[6].1["targetActorId"], "apify~other");
    // The status message of an aborted run goes to that run.
    let updates = api.run_updates();
    assert_eq!(updates, [json!({ "statusMessage": "Stopped by the parent", "isStatusMessageTerminal": true })]);
    let (state, _) = api.record(DEFAULT_STORE, "APIFY_GLOBAL_STATE").expect("state saved");
    assert_eq!(serde_json::from_slice::<Value>(&state).unwrap(), json!({ "calls": 5 }));
}

#[tokio::test]
async fn pay_per_event_charges_stay_within_the_budget() {
    let api = FakeApi::start().await;
    let dir = tempfile::tempdir().unwrap();
    let pricing = json!({ "pricingModel": "PAY_PER_EVENT", "pricingPerEvent": { "actorChargeEvents": {
        "page": { "eventTitle": "Page", "eventPriceUsd": 1.0 },
        "apify-default-dataset-item": { "eventTitle": "Item", "eventPriceUsd": 0.5 },
    }}});
    let mut vars = platform_env(&api, None);
    vars.push(("APIFY_ACTOR_PRICING_INFO", pricing.to_string()));
    vars.push(("APIFY_CHARGED_ACTOR_EVENT_COUNTS", "{}".to_owned()));
    vars.push(("ACTOR_MAX_TOTAL_CHARGE_USD", "5".to_owned()));
    assert_exit(&run_child("ppe", &vars, dir.path()).await, 0);

    assert_eq!(api.dataset_items(DEFAULT_DATASET).len(), 10, "5 USD buy 10 items at 0.5");
    let charges: Vec<Value> = api
        .platform_calls()
        .into_iter()
        .filter(|(path, _, _)| path.ends_with("/charge"))
        .map(|(_, _, body)| body)
        .collect();
    // Dataset items are charged by the platform itself; `page` once, over the budget.
    assert_eq!(charges, [json!({ "eventName": "page", "count": 1 })]);
}

#[tokio::test]
async fn concurrent_pay_per_event_pushes_do_not_wait_for_each_other() {
    let api = FakeApi::start().await;
    let dir = tempfile::tempdir().unwrap();
    let pricing = json!({ "pricingModel": "PAY_PER_EVENT", "pricingPerEvent": { "actorChargeEvents": {
        "page": { "eventTitle": "Page", "eventPriceUsd": 1.0 },
        "apify-default-dataset-item": { "eventTitle": "Item", "eventPriceUsd": 0.5 },
    }}});
    let mut vars = platform_env(&api, None);
    vars.push(("APIFY_ACTOR_PRICING_INFO", pricing.to_string()));
    vars.push(("APIFY_CHARGED_ACTOR_EVENT_COUNTS", "{}".to_owned()));
    vars.push(("ACTOR_MAX_TOTAL_CHARGE_USD", "30".to_owned()));
    assert_exit(&run_child("ppe-concurrent", &vars, dir.path()).await, 0);

    // 30 USD buy 20 items at 1.5, and one more is charged, so that the platform stops the run.
    assert_eq!(api.dataset_items(DEFAULT_DATASET).len(), 21);
    let pages: u64 = api
        .platform_calls()
        .into_iter()
        .filter(|(path, _, _)| path.ends_with("/charge"))
        .map(|(_, _, body)| body["count"].as_u64().unwrap())
        .sum();
    assert_eq!(pages, 21, "each stored item is charged once");
}

#[tokio::test]
async fn local_pay_per_event_runs_log_their_charges() {
    let dir = tempfile::tempdir().unwrap();
    let vars = [
        ("ACTOR_TEST_PAY_PER_EVENT", "1".to_owned()),
        ("ACTOR_USE_CHARGING_LOG_DATASET", "1".to_owned()),
        ("ACTOR_MAX_TOTAL_CHARGE_USD", "3".to_owned()),
    ];
    assert_exit(&run_child("ppe-local", &vars, dir.path()).await, 0);
    let items = std::fs::read_dir(dir.path().join("storage/datasets/default")).unwrap().count() - 1;
    assert_eq!(items, 3, "every event costs 1 locally");
    let log: Value = serde_json::from_str(
        &std::fs::read_to_string(dir.path().join("storage/datasets/charging_log/000000001.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(log["eventName"], "apify-default-dataset-item");
    assert_eq!(log["chargedCount"], 3);
}

#[tokio::test]
async fn secret_input_fields_are_decrypted() {
    let api = FakeApi::start().await;
    let fixture: Value = serde_json::from_str(include_str!("fixtures/input_secrets.json")).unwrap();
    api.put_record(DEFAULT_STORE, "INPUT", fixture["encrypted"].to_string().as_bytes(), "application/json");
    let dir = tempfile::tempdir().unwrap();
    let mut vars = platform_env(&api, None);
    vars.push(("APIFY_INPUT_SECRETS_PRIVATE_KEY_FILE", fixture["privateKeyFile"].as_str().unwrap().to_owned()));
    vars.push(("APIFY_INPUT_SECRETS_PRIVATE_KEY_PASSPHRASE", "pwd1234".to_owned()));
    assert_exit(&run_child("echo", &vars, dir.path()).await, 0);
    assert_eq!(api.dataset_items(DEFAULT_DATASET), [json!({ "echo": fixture["decrypted"] })]);

    // A wrong passphrase fails the input.
    let vars: Vec<_> = vars
        .into_iter()
        .map(|(name, value)| if name.ends_with("PASSPHRASE") { (name, "wrong".to_owned()) } else { (name, value) })
        .collect();
    let output = run_child("echo", &vars, dir.path()).await;
    assert_exit(&output, 91);
    assert!(String::from_utf8_lossy(&output.stdout).contains("Failed to decrypt the secret fields of the input."));
}

/// The scenarios, run by [`run_child`]. Does nothing in a normal test run.
#[test]
fn child() {
    let Ok(scenario) = std::env::var(SCENARIO) else { return };
    tracing_subscriber::fmt().with_writer(std::io::stdout).init();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async move {
        match scenario.as_str() {
            "echo" => {
                let exit = apify::ExitOptions::message("Echoed");
                Actor::main_with(apify::InitOptions::default(), exit, || async move {
                    let input: Value = Actor::get_input().await?;
                    Actor::push_data(&json!({ "echo": input })).await?;
                    Ok(())
                })
                .await
            }
            "echo-stdout" => {
                Actor::main(|| async move {
                    let input: Value = Actor::get_input().await?;
                    println!("ECHO {input}");
                    Ok(())
                })
                .await
            }
            "fail" => {
                let exit = apify::ExitOptions::message("Done");
                Actor::main_with(apify::InitOptions::default(), exit, || async { anyhow::bail!("boom") }).await
            }
            "wait" | "migrate" => {
                Actor::main(|| async move {
                    let state = Actor::services().auto_saved_value("STATE", || json!({ "count": 0 })).await?;
                    state.lock()["count"] = json!(1);
                    let wait = if scenario == "wait" { 60 } else { 2 };
                    tokio::time::sleep(Duration::from_secs(wait)).await;
                    Ok(())
                })
                .await
            }
            "platform" => {
                Actor::main(|| async move {
                    let state = Actor::use_state(None, || json!({ "calls": 0 })).await?;
                    let inherit =
                        apify::CallOptions { timeout: Some(apify::RunTimeout::Inherit), ..Default::default() };
                    let run = Actor::call("apify/hello-world", Some(&json!({ "a": 1 })), inherit).await?;
                    assert_eq!(run.status.as_deref(), Some("SUCCEEDED"));
                    let mut options = apify::CallOptions::default();
                    options.start.memory_mbytes = Some(512);
                    Actor::call_task("user/task", None::<&Value>, options).await?;
                    let minute = apify::RunTimeout::Duration(Duration::from_secs(60));
                    let options = apify::CallOptions { timeout: Some(minute), ..Default::default() };
                    let run = Actor::start_task("user/task", Some(&json!({ "b": 2 })), options).await?;
                    assert_eq!(run.status.as_deref(), Some("READY"), "not waited for");
                    let mut resurrect = apify::ResurrectOptions { timeout: Some(minute), ..Default::default() };
                    resurrect.resurrect.memory_mbytes = Some(256);
                    Actor::resurrect("old-run", resurrect).await?;
                    let abort = apify::AbortOptions {
                        status_message: Some("Stopped by the parent".to_owned()),
                        gracefully: Some(true),
                        ..Default::default()
                    };
                    Actor::abort("other-run", abort).await?;
                    let webhook = apify::WebhookOptions {
                        event_types: vec!["ACTOR.RUN.SUCCEEDED".to_owned()],
                        request_url: "https://example.com/hook".to_owned(),
                        ..Default::default()
                    };
                    assert!(Actor::add_webhook(webhook).await?.is_some());
                    Actor::metamorph("apify/other", None::<&Value>, Default::default()).await?;
                    state.lock()["calls"] = json!(5);
                    Ok(())
                })
                .await
            }
            "ppe" => {
                Actor::main(|| async move {
                    assert!(Actor::charging_manager().is_pay_per_event());
                    let items: Vec<Value> = (0..20).map(|n| json!({ "n": n })).collect();
                    let result = Actor::push_data(&items).await?;
                    assert_eq!(result.charged_count, 10);
                    assert!(result.event_charge_limit_reached);
                    let result = Actor::charge(apify::charging::ChargeOptions::new("page", 1)).await?;
                    assert_eq!(result.charged_count, 1, "one over the budget, so that the platform stops the run");
                    let result = Actor::push_data_and_charge(&items[..3], "page").await?;
                    assert_eq!(result.charged_count, 0);
                    assert!(Actor::push_data_and_charge(&items[0], "apify-default-dataset-item").await.is_err());
                    Ok(())
                })
                .await
            }
            "ppe-concurrent" => {
                Actor::main(|| async move {
                    let started = std::time::Instant::now();
                    let pushes = (0..30).map(|n| {
                        tokio::spawn(async move { Actor::push_data_and_charge(&json!({ "n": n }), "page").await })
                    });
                    let mut stored = 0;
                    for push in pushes.collect::<Vec<_>>() {
                        stored += push.await.unwrap()?.charged_count;
                    }
                    assert_eq!(stored, 21);
                    // Each charge call takes 300 ms: one after another, 21 would take over 6 s.
                    let elapsed = started.elapsed();
                    assert!(elapsed < Duration::from_secs(2), "the pushes waited for each other: {elapsed:?}");
                    Ok(())
                })
                .await
            }
            "ppe-local" => {
                Actor::main(|| async move {
                    let items: Vec<Value> = (0..5).map(|n| json!({ "n": n })).collect();
                    assert_eq!(Actor::push_data(&items).await?.charged_count, 3);
                    Ok(())
                })
                .await
            }
            "init-exit" => {
                async fn echo() -> anyhow::Result<()> {
                    let input: Value = Actor::get_input().await?;
                    Actor::push_data(&json!({ "echo": input })).await?;
                    Ok(())
                }
                Actor::init(apify::InitOptions::default()).await.unwrap();
                echo().await.unwrap();
                assert_eq!(Actor::get_value_or("MISSING", json!({ "default": true })).await.unwrap()["default"], true);
                assert_eq!(Actor::get_value_or("INPUT", json!(null)).await.unwrap()["greeting"], "hi");
                let run = Actor::set_status_message("Working", apify::StatusMessageOptions::default()).await;
                assert!(run.is_some(), "the updated run");
                Actor::exit(apify::ExitOptions::message("Echoed")).await;
            }
            "fail-default" | "fail-code" => {
                Actor::init(apify::InitOptions::default()).await.unwrap();
                let exit_code = if scenario == "fail-code" { 3 } else { apify::exit_codes::SUCCESS };
                Actor::fail(apify::ExitOptions { exit_code, ..apify::ExitOptions::default() }).await;
            }
            "status-local" => {
                Actor::init(apify::InitOptions::default()).await.unwrap();
                let debug = apify::StatusMessageOptions {
                    level: Some(crawlee::core::StatusLevel::Debug),
                    ..apify::StatusMessageOptions::default()
                };
                assert!(Actor::set_status_message("at debug", debug).await.is_none(), "no run to update");
                Actor::set_status_message("at info by default", apify::StatusMessageOptions::default()).await;
                Actor::exit(apify::ExitOptions::default()).await;
            }
            "child-runs" => {
                Actor::init(apify::InitOptions::default()).await.unwrap();
                let named = |name: &str| apify::CallOptions {
                    run_name: Some(name.to_owned()),
                    ..apify::CallOptions::default()
                };
                let a = Actor::start("user/a", Some(&json!({ "n": 1 })), named("a")).await.unwrap();
                let b = Actor::start_task("user/b", None::<&Value>, named("b")).await.unwrap();
                let c = Actor::start("user/c", None::<&Value>, named("c")).await.unwrap();
                println!("RUNS {} {} {}", a.id, b.id, c.id);
                if let Err(apify::ApifyClientError::InvalidArgument(message)) =
                    Actor::start("user/other", None::<&Value>, named("a")).await
                {
                    println!("REUSED {message}");
                }
                let mut names: Vec<String> = Actor::child_runs().await.into_keys().collect();
                names.sort();
                println!("CHILD RUNS {names:?}");
                Actor::exit(apify::ExitOptions::default()).await;
            }
            "listen-before-init" => {
                let listener = |name: &'static str| {
                    move |_| async move {
                        let message = Actor::exit_options().and_then(|options| options.status_message);
                        println!("EXIT LISTENER {name} {message:?}");
                    }
                };
                Actor::on(crawlee::core::EventKind::Exit, listener("kept"));
                let removed = Actor::on(crawlee::core::EventKind::Exit, listener("removed"));
                Actor::off(removed);
                Actor::init(apify::InitOptions::default()).await.unwrap();
                Actor::exit(apify::ExitOptions::message("Bye")).await;
            }
            "before-init" => {
                Actor::is_at_home();
            }
            "late-init" => {
                crawlee::Services::global();
                let result = Actor::init(apify::InitOptions::default()).await;
                assert!(matches!(result, Err(apify::InitError::ServicesAlreadySet)));
                std::process::exit(0);
            }
            other => panic!("unknown scenario {other}"),
        }
    });
    unreachable!("the Actor exits the process");
}
