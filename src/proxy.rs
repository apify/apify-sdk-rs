//! Apify Proxy for crawlee-rs crawlers: a [`ProxySource`] that composes Apify Proxy URLs (or uses
//! custom proxy URLs), like `ProxyConfiguration` of the JS SDK.

use std::sync::Arc;
use std::time::Duration;

use apify_client::ApifyClient;
use crawlee::basic::proxy::{ProxyInfo, ProxySource};
use serde::Deserialize;
use url::Url;

use crate::configuration::Configuration;

const CHECK_ACCESS_REQUEST_TIMEOUT: Duration = Duration::from_secs(4);
const CHECK_ACCESS_MAX_ATTEMPTS: usize = 2;
const SESSION_ID_LENGTH: usize = 12;

/// Options of [`ProxyConfiguration`]. It is also the shape of the proxy field of Actor inputs
/// (the `proxy` editor), so it can be deserialized from the input.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ProxyConfigurationOptions {
    /// `false` (as the input editor sends for "no proxy") disables Apify Proxy.
    pub use_apify_proxy: Option<bool>,
    /// The proxy password; `APIFY_PROXY_PASSWORD`, or the password of the account of
    /// `APIFY_TOKEN`, when not set.
    pub password: Option<String>,
    #[serde(alias = "apifyProxyGroups")]
    pub groups: Vec<String>,
    #[serde(alias = "apifyProxyCountry")]
    pub country_code: Option<String>,
    #[serde(alias = "apifyProxySubdivision")]
    pub subdivision_code: Option<String>,
    /// Custom proxies instead of Apify Proxy.
    pub proxy_urls: Vec<String>,
    /// Check that Apify Proxy can be used when the configuration is created. Default `true`.
    pub check_access: Option<bool>,
}

#[derive(Debug, thiserror::Error)]
pub enum ProxyConfigurationError {
    #[error("ProxyConfiguration: invalid {field} {value:?}")]
    InvalidValue { field: &'static str, value: String },
    #[error("ProxyConfiguration: \"subdivisionCode\" requires \"countryCode\" to be set.")]
    SubdivisionWithoutCountry,
    #[error(
        "Cannot combine custom proxies with Apify Proxy! It is not allowed to set \"options.proxyUrls\" combined with \
         \"options.groups\", \"options.countryCode\" or \"options.subdivisionCode\"."
    )]
    CustomAndApify,
    #[error(transparent)]
    Custom(#[from] crawlee::basic::proxy::ProxyConfigurationError),
    #[error(
        "Apify Proxy password must be provided using options.password or the \"APIFY_PROXY_PASSWORD\" environment \
         variable. You can also provide your Apify token via the \"APIFY_TOKEN\" environment variable, so that the SDK \
         can fetch the proxy password from Apify API, when APIFY_PROXY_PASSWORD is not defined"
    )]
    MissingPassword,
    #[error("fetching the proxy password failed: {0}")]
    User(#[from] apify_client::ApifyClientError),
    /// The proxy status check says the proxy cannot be used.
    #[error("{0}")]
    NoAccess(String),
}

fn is_proxy_value(value: &str) -> bool {
    !value.is_empty() && value.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '~'))
}

fn random_session_id() -> String {
    use rand::Rng as _;
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();
    (0..SESSION_ID_LENGTH).map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char).collect()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProxyStatus {
    connected: bool,
    #[serde(default)]
    connection_error: Option<String>,
    #[serde(default)]
    is_man_in_the_middle: bool,
}

#[derive(Debug)]
enum Kind {
    Apify { groups: Vec<String>, country_code: Option<String>, subdivision_code: Option<String>, password: String },
    Custom(crawlee::ProxyConfiguration),
}

/// Proxies for a crawler: Apify Proxy, or custom proxy URLs. Pass it to a crawler's
/// `proxy_configuration`. Every new session of the crawler gets a new Apify Proxy session.
#[derive(Debug)]
pub struct ProxyConfiguration {
    kind: Kind,
    hostname: String,
    port: u16,
    /// Whether the proxy intercepts HTTPS (reported by the status check).
    pub is_man_in_the_middle: bool,
}

impl ProxyConfiguration {
    /// Creates the configuration, like `Actor.createProxyConfiguration()` in JS. Returns `None`
    /// when no proxy should be used: Apify Proxy is disabled and there are no custom proxies, or
    /// (locally) there is no password, or the access check failed off the platform.
    pub async fn new(
        configuration: &Configuration,
        client: &ApifyClient,
        options: ProxyConfigurationOptions,
    ) -> Result<Option<Arc<Self>>, ProxyConfigurationError> {
        if options.use_apify_proxy == Some(false) && options.proxy_urls.is_empty() {
            return Ok(None);
        }
        let uses_apify = options.proxy_urls.is_empty();
        let mut proxy = ProxyConfiguration::from_options(configuration, &options)?;
        if !uses_apify {
            return Ok(Some(Arc::new(proxy)));
        }

        let mut password = options.password.clone().or_else(|| configuration.proxy_password.clone());
        if password.is_none() && configuration.token.is_some() {
            match client.me().get().await {
                Ok(user) => {
                    password =
                        user.and_then(|user| user.extra.get("proxy")?.get("password")?.as_str().map(str::to_owned));
                }
                Err(err) if configuration.is_at_home => return Err(err.into()),
                Err(err) => tracing::warn!("Failed to fetch user data using token: {err}"),
            }
        }
        let Some(password) = password else {
            if configuration.is_at_home {
                return Err(ProxyConfigurationError::MissingPassword);
            }
            tracing::warn!(
                "No proxy password or token detected, running without proxy. To use Apify Proxy locally, provide \
                 options.password or \"APIFY_PROXY_PASSWORD\" environment variable. You can also provide your Apify \
                 token via the \"APIFY_TOKEN\" environment variable, so that the SDK can fetch the proxy password \
                 from Apify API, when APIFY_PROXY_PASSWORD is not defined"
            );
            return Ok(None);
        };
        if let Kind::Apify { password: slot, .. } = &mut proxy.kind {
            *slot = password;
        }

        if options.check_access != Some(false) && !proxy.check_access(configuration).await? {
            return Ok(None);
        }
        Ok(Some(Arc::new(proxy)))
    }

    /// The configuration without the password lookup and the access check.
    fn from_options(
        configuration: &Configuration,
        options: &ProxyConfigurationOptions,
    ) -> Result<Self, ProxyConfigurationError> {
        for group in &options.groups {
            if !is_proxy_value(group) {
                return Err(ProxyConfigurationError::InvalidValue { field: "group", value: group.clone() });
            }
        }
        if let Some(country) = &options.country_code
            && !(country.len() == 2 && country.chars().all(|c| c.is_ascii_uppercase()))
        {
            return Err(ProxyConfigurationError::InvalidValue { field: "countryCode", value: country.clone() });
        }
        if let Some(subdivision) = &options.subdivision_code
            && !((1..=3).contains(&subdivision.len())
                && subdivision.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()))
        {
            return Err(ProxyConfigurationError::InvalidValue { field: "subdivisionCode", value: subdivision.clone() });
        }
        if options.subdivision_code.is_some() && options.country_code.is_none() {
            return Err(ProxyConfigurationError::SubdivisionWithoutCountry);
        }
        let kind = if options.proxy_urls.is_empty() {
            Kind::Apify {
                groups: options.groups.clone(),
                country_code: options.country_code.clone(),
                subdivision_code: options.subdivision_code.clone(),
                password: options.password.clone().unwrap_or_default(),
            }
        } else {
            if !options.groups.is_empty() || options.country_code.is_some() || options.subdivision_code.is_some() {
                return Err(ProxyConfigurationError::CustomAndApify);
            }
            if options.proxy_urls.iter().any(|url| url.contains("apify.com")) {
                tracing::warn!(
                    "Some Apify proxy features may work incorrectly. Please consider setting up Apify properties \
                     instead of `proxyUrls`."
                );
            }
            Kind::Custom(crawlee::ProxyConfiguration::new(&options.proxy_urls)?)
        };
        Ok(ProxyConfiguration {
            kind,
            hostname: configuration.proxy_hostname.clone(),
            port: configuration.proxy_port,
            is_man_in_the_middle: false,
        })
    }

    /// The Apify Proxy username: `groups-A+B,session-X,country-US_CA`.
    fn username(&self, session_id: &str) -> String {
        let Kind::Apify { groups, country_code, subdivision_code, .. } = &self.kind else { return String::new() };
        let mut parts = Vec::new();
        if !groups.is_empty() {
            parts.push(format!("groups-{}", groups.join("+")));
        }
        parts.push(format!("session-{session_id}"));
        match (country_code, subdivision_code) {
            (Some(country), Some(subdivision)) => parts.push(format!("country-{country}_{subdivision}")),
            (Some(country), None) => parts.push(format!("country-{country}")),
            _ => {}
        }
        parts.join(",")
    }

    /// A proxy URL with a new Apify Proxy session (or the next custom proxy).
    pub fn new_url(&self) -> Option<Url> {
        self.new_proxy_info("").map(|info| info.url)
    }

    /// The Apify Proxy URL of the proxy session `session_id`; `None` for custom proxies.
    pub fn url_for_session(&self, session_id: &str) -> Option<Url> {
        matches!(self.kind, Kind::Apify { .. }).then(|| self.apify_url(session_id))
    }

    fn apify_url(&self, session_id: &str) -> Url {
        self.apify_url_with_username(&self.username(session_id))
    }

    fn apify_url_with_username(&self, username: &str) -> Url {
        let Kind::Apify { password, .. } = &self.kind else { unreachable!("an Apify Proxy configuration") };
        let mut url = Url::parse(&format!("http://{}:{}", self.hostname, self.port)).expect("a valid proxy URL");
        let _ = url.set_username(username);
        let _ = url.set_password(Some(password));
        url
    }

    /// Whether Apify Proxy can be used, from its status page. A timeout counts as yes (with a
    /// warning), as in JS. On the platform, no access is an error.
    async fn check_access(&mut self, configuration: &Configuration) -> Result<bool, ProxyConfigurationError> {
        let Some(status) = self.fetch_status(configuration).await else {
            tracing::warn!(
                "Apify Proxy access check timed out. Watch out for errors with status code 407. If you see some, it \
                 most likely means you don't have access to either all or some of the proxies you're trying to use."
            );
            return Ok(true);
        };
        self.is_man_in_the_middle = status.is_man_in_the_middle;
        if status.connected {
            return Ok(true);
        }
        let error = status.connection_error.unwrap_or_default();
        if configuration.is_at_home {
            return Err(ProxyConfigurationError::NoAccess(error));
        }
        tracing::warn!("{error}");
        Ok(false)
    }

    async fn fetch_status(&self, configuration: &Configuration) -> Option<ProxyStatus> {
        let status_url = format!("{}/?format=json", configuration.proxy_status_url);
        let proxy = reqwest::Proxy::all(self.apify_url(&random_session_id()).as_str()).ok()?;
        let client = reqwest::Client::builder().proxy(proxy).timeout(CHECK_ACCESS_REQUEST_TIMEOUT).build().ok()?;
        for _ in 0..CHECK_ACCESS_MAX_ATTEMPTS {
            let Ok(response) = client.get(&status_url).send().await else { continue };
            if !response.status().is_success() {
                continue;
            }
            let Ok(body) = response.bytes().await else { continue };
            if let Ok(status) = serde_json::from_slice::<ProxyStatus>(&body) {
                return Some(status);
            }
        }
        None
    }
}

impl crate::Actor {
    /// Proxies for crawlers, like `Actor.createProxyConfiguration()` in JS (see
    /// [`ProxyConfiguration::new`]). The options are often the `proxy` field of the input.
    pub async fn create_proxy_configuration(
        options: ProxyConfigurationOptions,
    ) -> Result<Option<Arc<ProxyConfiguration>>, ProxyConfigurationError> {
        ProxyConfiguration::new(Self::configuration(), Self::client(), options).await
    }
}

impl ProxySource for ProxyConfiguration {
    fn new_proxy_info(&self, session_id: &str) -> Option<ProxyInfo> {
        match &self.kind {
            // A session of its own per crawler session: the crawler keeps the proxy of a session.
            Kind::Apify { .. } => Some(ProxyInfo::from_url(self.apify_url(&random_session_id()))),
            Kind::Custom(custom) => custom.new_proxy_info(session_id),
        }
    }

    /// A restored Apify Proxy session (its username carries `session-…`) moves to the current
    /// endpoint and password, keeping the username and with it the proxy session. Other proxies are
    /// returned unchanged.
    fn restored_proxy_info(&self, session_id: &str, restored: ProxyInfo) -> Option<ProxyInfo> {
        match &self.kind {
            Kind::Apify { .. } if has_proxy_session(&restored.username) => {
                Some(ProxyInfo::from_url(self.apify_url_with_username(&restored.username)))
            }
            Kind::Apify { .. } => Some(restored),
            Kind::Custom(custom) => custom.restored_proxy_info(session_id, restored),
        }
    }
}

/// Whether a proxy username has a `session-…` part (`SESSION_USERNAME_REGEX` in JS).
fn has_proxy_session(username: &str) -> bool {
    username.split(',').any(|part| part.strip_prefix("session-").is_some_and(|id| !id.is_empty()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(value: serde_json::Value) -> ProxyConfigurationOptions {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn a_restored_apify_proxy_session_moves_to_the_current_endpoint() {
        let configuration = Configuration::default();
        let proxy = ProxyConfiguration::from_options(
            &configuration,
            &options(serde_json::json!({ "apifyProxyGroups": ["RESIDENTIAL"] })),
        )
        .unwrap();

        let old = ProxyInfo::from_url(
            Url::parse("http://groups-DATACENTER,session-abc,country-US:old@old-host.apify.net:8000").unwrap(),
        );
        let moved = proxy.restored_proxy_info("s", old).unwrap();
        assert_eq!((moved.hostname.as_str(), moved.port), ("proxy.apify.com", Some(8000)));
        assert_eq!(moved.username, "groups-DATACENTER,session-abc,country-US", "the proxy session is kept");

        let other = ProxyInfo::from_url(Url::parse("http://user:pass@elsewhere:8000").unwrap());
        assert_eq!(proxy.restored_proxy_info("s", other).unwrap().hostname, "elsewhere");
    }

    #[test]
    fn apify_proxy_urls() {
        let configuration = Configuration::default();
        let mut proxy = ProxyConfiguration::from_options(
            &configuration,
            &options(
                serde_json::json!({ "apifyProxyGroups": ["RESIDENTIAL", "GOOGLE_SERP"], "apifyProxyCountry": "US" }),
            ),
        )
        .unwrap();
        if let Kind::Apify { password, .. } = &mut proxy.kind {
            *password = "p@ss".to_owned();
        }
        assert_eq!(proxy.username("abc"), "groups-RESIDENTIAL+GOOGLE_SERP,session-abc,country-US");
        let url = proxy.apify_url("abc");
        assert_eq!(
            url.as_str(),
            "http://groups-RESIDENTIAL+GOOGLE_SERP,session-abc,country-US:p%40ss@proxy.apify.com:8000/"
        );

        let info = proxy.new_proxy_info("ignored").unwrap();
        assert_eq!((info.hostname.as_str(), info.port), ("proxy.apify.com", Some(8000)));
        assert!(info.username.contains("session-"));

        let subdivision = ProxyConfiguration::from_options(
            &configuration,
            &options(serde_json::json!({ "countryCode": "US", "subdivisionCode": "CA" })),
        )
        .unwrap();
        assert_eq!(subdivision.username("x"), "session-x,country-US_CA");
    }

    #[test]
    fn invalid_options() {
        let configuration = Configuration::default();
        let error = |value| ProxyConfiguration::from_options(&configuration, &options(value)).unwrap_err().to_string();
        assert!(error(serde_json::json!({ "subdivisionCode": "CA" })).contains("requires \"countryCode\""));
        assert!(error(serde_json::json!({ "countryCode": "usa" })).contains("countryCode"));
        assert!(error(serde_json::json!({ "groups": ["a b"] })).contains("group"));
        assert!(error(serde_json::json!({ "groups": ["A"], "proxyUrls": ["http://p:1"] })).contains("Cannot combine"));
    }

    #[tokio::test]
    async fn disabled_and_custom_proxies() {
        let configuration = Configuration::default();
        let client = crate::new_client(&configuration, None, Default::default());
        let none =
            ProxyConfiguration::new(&configuration, &client, options(serde_json::json!({ "useApifyProxy": false })));
        assert!(none.await.unwrap().is_none());

        let custom = options(serde_json::json!({ "useApifyProxy": false, "proxyUrls": ["http://a:1", "http://b:2"] }));
        let custom = ProxyConfiguration::new(&configuration, &client, custom).await.unwrap().unwrap();
        assert_eq!(custom.new_proxy_info("s").unwrap().hostname, "a");
        assert_eq!(custom.new_proxy_info("s").unwrap().hostname, "b");

        // Locally, no password and no token: no proxy.
        let apify = ProxyConfiguration::new(&configuration, &client, ProxyConfigurationOptions::default());
        assert!(apify.await.unwrap().is_none());
    }
}
