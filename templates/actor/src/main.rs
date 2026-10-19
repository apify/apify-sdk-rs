//! An Actor that crawls the start URLs of its input and saves the title of every page.

use apify::crawlee::{EnqueueLinksOptions, HtmlContext, HtmlCrawler};
use apify::{Actor, ExitOptions, InitOptions};
use serde::Deserialize;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Input {
    start_urls: Vec<StartUrl>,
    max_requests_per_crawl: u64,
}

#[derive(Debug, Deserialize)]
struct StartUrl {
    url: String,
}

#[tokio::main(flavor = "local")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

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
    // Missing fields get their defaults from `.actor/input_schema.json`.
    let input: Input = Actor::get_input().await?;

    let crawler = HtmlCrawler::builder()
        .services(Actor::services().clone())
        .max_requests_per_crawl(input.max_requests_per_crawl)
        .request_handler(|ctx: HtmlContext| async move {
            let title = ctx.with_html(|doc| doc.title()).await?;
            tracing::info!("{}: {title:?}", ctx.url());
            ctx.push_data(&serde_json::json!({ "url": ctx.url().as_str(), "title": title }))?;
            ctx.enqueue_links(EnqueueLinksOptions::new()).await?;
            Ok(())
        })
        .build()?;
    crawler.run(input.start_urls.into_iter().map(|start| start.url)).await?;
    Ok(())
}
