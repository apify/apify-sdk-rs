//! URL filters of Actor inputs: globs and pseudo-URLs with request options, as the `globs` and
//! `pseudoUrls` input fields send them (`createTransformRequestFunction` of the JS SDK).

use crawlee::Request;
use crawlee::utils::UrlPattern;
use crawlee::utils::patterns::UrlMatcher;
use indexmap::IndexMap;
use regex::Regex;
use serde::Deserialize;
use serde_json::{Map, Value};

/// Options applied to the requests a pattern matches.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", default)]
pub struct UrlPatternRequestOptions {
    pub method: Option<String>,
    pub payload: Option<String>,
    pub label: Option<String>,
    pub user_data: Option<Map<String, Value>>,
    pub headers: Option<IndexMap<String, String>>,
}

/// A glob, alone or with request options.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum GlobInput {
    Glob(String),
    WithOptions {
        glob: String,
        #[serde(flatten)]
        options: Box<UrlPatternRequestOptions>,
    },
}

/// A pseudo-URL, alone or with request options.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum PseudoUrlInput {
    Purl(String),
    WithOptions {
        purl: String,
        #[serde(flatten)]
        options: Box<UrlPatternRequestOptions>,
    },
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", default)]
pub struct UrlPatternFilters {
    pub globs: Vec<GlobInput>,
    pub pseudo_urls: Vec<PseudoUrlInput>,
}

#[derive(Debug, thiserror::Error)]
pub enum UrlFilterError {
    #[error("Invalid pseudoUrl pattern '{purl}': {message}")]
    PseudoUrl { purl: String, message: String },
    #[error(transparent)]
    Glob(#[from] crawlee::utils::patterns::PatternError),
}

/// The regular expression of a pseudo-URL: the URL, with `[regex]` sections, matched
/// case-insensitively as a whole (`purlToRegExp` of `@apify/pseudo_url`).
pub fn purl_to_regex(purl: &str) -> Result<Regex, UrlFilterError> {
    let trimmed = purl.trim();
    if trimmed.is_empty() {
        return Err(UrlFilterError::PseudoUrl {
            purl: purl.to_owned(),
            message: format!("Cannot parse PURL '{trimmed}': it must be an non-empty string"),
        });
    }
    let mut regex = String::from("(?i)^");
    let mut open_brackets = 0usize;
    for ch in trimmed.chars() {
        if ch == '[' && {
            open_brackets += 1;
            open_brackets == 1
        } {
            regex.push('(');
        } else if ch == ']' && open_brackets > 0 && {
            open_brackets -= 1;
            open_brackets == 0
        } {
            regex.push(')');
        } else if open_brackets > 0 || ch.is_ascii_alphanumeric() {
            // Inside a `[regex]` section, or a character that needs no escaping.
            regex.push(ch);
        } else {
            regex.push_str(&format!("\\x{{{:02x}}}", ch as u32));
        }
    }
    regex.push('$');
    Regex::new(&regex).map_err(|err| UrlFilterError::PseudoUrl { purl: purl.to_owned(), message: err.to_string() })
}

struct Pattern {
    matcher: UrlMatcher,
    options: UrlPatternRequestOptions,
}

/// Applies the options of the first pattern a request matches.
pub struct RequestTransform {
    patterns: Vec<Pattern>,
}

impl RequestTransform {
    /// The transform of `filters`, or `None` when they have no patterns (requests stay unchanged).
    /// Empty patterns are skipped.
    pub fn new(filters: &UrlPatternFilters) -> Result<Option<Self>, UrlFilterError> {
        let mut patterns = Vec::new();
        for glob in &filters.globs {
            let (glob, options) = match glob {
                GlobInput::Glob(glob) => (glob, UrlPatternRequestOptions::default()),
                GlobInput::WithOptions { glob, options } => (glob, (**options).clone()),
            };
            if glob.trim().is_empty() {
                continue;
            }
            patterns.push(Pattern { matcher: UrlMatcher::compile(&UrlPattern::glob(glob.trim()))?, options });
        }
        for purl in &filters.pseudo_urls {
            let (purl, options) = match purl {
                PseudoUrlInput::Purl(purl) => (purl, UrlPatternRequestOptions::default()),
                PseudoUrlInput::WithOptions { purl, options } => (purl, (**options).clone()),
            };
            if purl.trim().is_empty() {
                continue;
            }
            patterns.push(Pattern { matcher: UrlMatcher::Regex(purl_to_regex(purl)?), options });
        }
        Ok((!patterns.is_empty()).then_some(RequestTransform { patterns }))
    }

    /// The request with the options of the first matching pattern, or `None` when none matches.
    pub fn apply(&self, mut request: Request) -> Option<Request> {
        let pattern = self.patterns.iter().find(|pattern| pattern.matcher.is_match(&request.url))?;
        let options = &pattern.options;
        if let Some(method) = &options.method {
            request.method = method.to_ascii_uppercase();
        }
        if let Some(payload) = &options.payload {
            request.payload = Some(payload.clone());
        }
        if let Some(headers) = &options.headers {
            request.headers = headers.clone();
        }
        if let Some(user_data) = &options.user_data {
            request.user_data = user_data.clone();
        }
        if let Some(label) = &options.label {
            request.set_label(label.clone());
        }
        Some(request)
    }

    /// For [`EnqueueLinksOptions::transform`](crawlee::EnqueueLinksOptions::transform).
    pub fn into_fn(self) -> impl Fn(Request) -> Option<Request> + 'static {
        move |request| self.apply(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pseudo_urls_match_like_js() {
        let purl = purl_to_regex("http://www.example.com/pages/[(\\w|-)*]").unwrap();
        assert!(purl.is_match("http://www.example.com/pages/"));
        assert!(purl.is_match("HTTP://WWW.EXAMPLE.COM/pages/my-awesome-page"));
        assert!(!purl.is_match("http://www.example.com/pages/a/b"));
        assert!(!purl.is_match("http://www-example.com/pages/"), "the dot is literal");
        let query = purl_to_regex("http://www.example.com/search?do[\\x5B]load[\\x5D]=1").unwrap();
        assert!(query.is_match("http://www.example.com/search?do[load]=1"));
        assert_eq!(purl_to_regex("a.b[\\d+]").unwrap().as_str(), "(?i)^a\\x{2e}b(\\d+)$");
        assert!(purl_to_regex("  ").is_err());
    }

    #[test]
    fn the_first_matching_pattern_sets_the_options() {
        let filters: UrlPatternFilters = serde_json::from_value(serde_json::json!({
            "globs": ["https://a.dev/blog/**", { "glob": "https://a.dev/**", "label": "OTHER", "method": "post" }],
            "pseudoUrls": [{ "purl": "https://b.dev/[\\d+]", "userData": { "kind": "number" } }, ""],
        }))
        .unwrap();
        let transform = RequestTransform::new(&filters).unwrap().unwrap();
        let blog = transform.apply(Request::new("https://a.dev/blog/post")).unwrap();
        assert_eq!((blog.label(), blog.method.as_str()), (None, "GET"));
        let other = transform.apply(Request::new("https://A.dev/x")).unwrap();
        assert_eq!((other.label(), other.method.as_str()), (Some("OTHER"), "POST"));
        let number = transform.apply(Request::new("https://b.dev/42")).unwrap();
        assert_eq!(number.user_data["kind"], "number");
        assert!(transform.apply(Request::new("https://c.dev/")).is_none());

        assert!(RequestTransform::new(&UrlPatternFilters::default()).unwrap().is_none());
    }
}
