//! The Apify Proxy access check and password lookup, against a local stand-in for the proxy.

mod common;

use std::sync::{Arc, Mutex};

use apify::{Configuration, ProxyConfiguration, ProxyConfigurationOptions};
use axum::Router;
use axum::http::{HeaderMap, Uri};
use axum::routing::get;
use base64::Engine as _;
use crawlee::basic::proxy::ProxySource as _;

use common::{FakeApi, TOKEN};

/// A proxy that answers the status page itself, recording the proxy credentials and target.
async fn serve_proxy(connected: bool) -> (u16, Arc<Mutex<Vec<(String, String)>>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = seen.clone();
    let app = Router::new().fallback(get(move |uri: Uri, headers: HeaderMap| {
        let recorded = recorded.clone();
        async move {
            let credentials = headers
                .get("proxy-authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Basic "))
                .and_then(|v| base64::engine::general_purpose::STANDARD.decode(v).ok())
                .map(|v| String::from_utf8(v).unwrap())
                .unwrap_or_default();
            recorded.lock().unwrap().push((uri.to_string(), credentials));
            let status = if connected {
                serde_json::json!({ "connected": true, "isManInTheMiddle": true })
            } else {
                serde_json::json!({ "connected": false, "connectionError": "No access to group X" })
            };
            axum::Json(status)
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::task::spawn_local(async move { axum::serve(listener, app).await.unwrap() });
    (port, seen)
}

fn configuration(api: &FakeApi, port: u16, is_at_home: bool) -> Configuration {
    Configuration {
        is_at_home,
        token: Some(TOKEN.to_owned()),
        api_base_url: api.url.clone(),
        api_public_base_url: api.url.clone(),
        proxy_hostname: "127.0.0.1".to_owned(),
        proxy_port: port,
        ..Configuration::default()
    }
}

#[tokio::test(flavor = "local")]
async fn the_password_comes_from_the_account_and_access_is_checked() {
    let api = FakeApi::start().await;
    let (port, seen) = serve_proxy(true).await;
    let configuration = configuration(&api, port, true);
    let client = apify::new_client(&configuration, None, Default::default());
    let options = ProxyConfigurationOptions { groups: vec!["RESIDENTIAL".to_owned()], ..Default::default() };

    let proxy = ProxyConfiguration::new(&configuration, &client, options).await.unwrap().unwrap();
    assert!(proxy.is_man_in_the_middle);
    let (target, credentials) = seen.lock().unwrap()[0].clone();
    assert_eq!(target, "http://proxy.apify.com/?format=json");
    let (username, password) = credentials.split_once(':').unwrap();
    assert!(username.starts_with("groups-RESIDENTIAL,session-"), "{username}");
    assert_eq!(password, "proxy-secret", "fetched from users/me");

    let first = proxy.new_proxy_info("s1").unwrap();
    let second = proxy.new_proxy_info("s2").unwrap();
    assert_ne!(first.username, second.username, "a new Apify Proxy session per crawler session");
    assert_eq!(first.password, "proxy-secret");
}

#[tokio::test(flavor = "local")]
async fn no_access_is_an_error_on_the_platform_and_no_proxy_locally() {
    let api = FakeApi::start().await;
    let (port, _) = serve_proxy(false).await;
    let options = || ProxyConfigurationOptions { password: Some("pw".to_owned()), ..Default::default() };

    let at_home = configuration(&api, port, true);
    let client = apify::new_client(&at_home, None, Default::default());
    let error = ProxyConfiguration::new(&at_home, &client, options()).await.unwrap_err();
    assert_eq!(error.to_string(), "No access to group X");

    let local = configuration(&api, port, false);
    assert!(ProxyConfiguration::new(&local, &client, options()).await.unwrap().is_none());
    let unchecked = ProxyConfigurationOptions { check_access: Some(false), ..options() };
    assert!(ProxyConfiguration::new(&local, &client, unchecked).await.unwrap().is_some());
}
