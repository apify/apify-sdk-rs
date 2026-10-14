//! A crawlee-rs crawler with its queue and dataset on the (fake) platform.

mod common;

use std::rc::Rc;

use apify::{ApifyStorageBackend, Configuration};
use axum::Router;
use axum::extract::Path;
use axum::response::Html;
use axum::routing::get;
use crawlee::{EnqueueLinksOptions, HtmlContext, HtmlCrawler, Services};

use common::{DEFAULT_DATASET, DEFAULT_QUEUE, DEFAULT_STORE, FakeApi, TOKEN};

async fn serve_site() -> String {
    let app = Router::new().route(
        "/page/{n}",
        get(|Path(n): Path<u32>| async move {
            let links: String = (0..5).map(|m| format!(r#"<a href="/page/{m}">{m}</a>"#)).collect();
            Html(format!("<title>Page {n}</title>{links}"))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::task::spawn_local(async move { axum::serve(listener, app).await.unwrap() });
    url
}

#[tokio::test(flavor = "local")]
async fn a_crawl_stores_its_queue_and_results_on_the_platform() {
    let api = FakeApi::start().await;
    let site = serve_site().await;
    let configuration = Rc::new(Configuration {
        is_at_home: true,
        token: Some(TOKEN.to_owned()),
        api_base_url: api.url.clone(),
        api_public_base_url: api.url.clone(),
        default_dataset_id: DEFAULT_DATASET.to_owned(),
        default_key_value_store_id: DEFAULT_STORE.to_owned(),
        default_request_queue_id: DEFAULT_QUEUE.to_owned(),
        ..Configuration::default()
    });
    let services = Services::new(Rc::new(ApifyStorageBackend::new(configuration)));

    let crawler = HtmlCrawler::builder()
        .services(services)
        .request_handler(|ctx: HtmlContext| async move {
            let title = ctx.with_html(|doc| doc.title()).await?;
            ctx.push_data(&serde_json::json!({ "url": ctx.request().url, "title": title }))?;
            ctx.enqueue_links(EnqueueLinksOptions::new()).await?;
            Ok(())
        })
        .build()
        .unwrap();
    let stats = crawler.run([format!("{site}/page/0")]).await.unwrap();
    assert_eq!(stats.requests_succeeded, 5);

    let mut titles: Vec<String> =
        api.dataset_items(DEFAULT_DATASET).iter().map(|item| item["title"].as_str().unwrap().to_owned()).collect();
    titles.sort();
    assert_eq!(titles, ["Page 0", "Page 1", "Page 2", "Page 3", "Page 4"]);
    // The crawler's state is persisted to the default store on the platform.
    assert!(api.calls().iter().any(|call| call.starts_with("PUT /v2/key-value-stores/default-store/records/")));
}
