//! Google Sheets sharing links in the `requestsFromUrl` entries of the input, replaced with the
//! links of their CSV exports, as the JS SDK's `getInput` does (`google_sheets.ts`).

use std::sync::LazyLock;

use regex::Regex;
use serde_json::{Map, Value};

/// A sheet's link, its ID, and whether it already points at an export (`/gviz/...`, `/export...`).
/// JS writes the ID as `(?!e\/)(?:\w|-)+`: links published to the web
/// (`/spreadsheets/d/e/<id>/pub?output=csv`) are downloadable as they are. The `regex` crate has
/// no lookahead, so [`to_google_sheets_csv_url`] checks for them itself.
static GOOGLE_SHEETS_URL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(https://docs\.google\.com/spreadsheets/d/([A-Za-z0-9_-]+))(/(?:gviz|export)(?-u:\b))?").unwrap()
});

/// The link of the CSV export of a Google Sheets sharing link. Other URLs are returned unchanged.
pub fn to_google_sheets_csv_url(url: &str) -> String {
    let Some(captures) = GOOGLE_SHEETS_URL.captures(url) else { return url.to_owned() };
    let sheet = &captures[1];
    let published = &captures[2] == "e" && url[sheet.len()..].starts_with('/');
    if published || captures.get(3).is_some() {
        return url.to_owned();
    }
    format!("{sheet}/gviz/tq?tqx=out:csv")
}

/// Replaces Google Sheets sharing links in the `requestsFromUrl` entries of the input's array
/// fields (as the "Link remote text file" option of the `requestListSources` editor makes them)
/// with the links of their CSV exports.
pub fn fix_google_sheets_urls(mut input: Map<String, Value>) -> Map<String, Value> {
    for value in input.values_mut() {
        let Value::Array(items) = value else { continue };
        for item in items {
            if let Some(Value::String(url)) = item.get_mut("requestsFromUrl") {
                *url = to_google_sheets_csv_url(url);
            }
        }
    }
    input
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const ID: &str = "11UGSBOSXy5Ov2WEP9nr4kSIxQJmH18zh-5onKtBsovU";

    fn csv_url() -> String {
        format!("https://docs.google.com/spreadsheets/d/{ID}/gviz/tq?tqx=out:csv")
    }

    #[test]
    fn rewrites_sharing_links_to_the_csv_export() {
        for suffix in ["", "/", "/edit?usp=sharing", "/123123132", "/?q=blablabla", "/edit#gid=0"] {
            let url = format!("https://docs.google.com/spreadsheets/d/{ID}{suffix}");
            assert_eq!(to_google_sheets_csv_url(&url), csv_url(), "{url}");
        }
    }

    #[test]
    fn keeps_published_and_export_links_and_other_urls() {
        for url in [
            "https://docs.google.com/spreadsheets/d/e/2PACX-1vSabc_DEF-123/pub?gid=0&single=true&output=csv".to_owned(),
            format!("https://docs.google.com/spreadsheets/d/{ID}/gviz/tq?tqx=out:csv&sheet=Second"),
            format!("https://docs.google.com/spreadsheets/d/{ID}/export?format=csv&gid=123"),
            "https://example.com/urls.txt".to_owned(),
        ] {
            assert_eq!(to_google_sheets_csv_url(&url), url);
        }
    }

    #[test]
    fn an_id_that_only_starts_with_e_is_rewritten() {
        // `(?!e\/)` only rejects the `e/` of a published link.
        let url = "https://docs.google.com/spreadsheets/d/eabc/edit";
        assert_eq!(to_google_sheets_csv_url(url), "https://docs.google.com/spreadsheets/d/eabc/gviz/tq?tqx=out:csv");
        // `/exports` is no export link: `\b` needs the word to end there.
        let url = format!("https://docs.google.com/spreadsheets/d/{ID}/exports");
        assert_eq!(to_google_sheets_csv_url(&url), csv_url());
    }

    #[test]
    fn rewrites_only_the_requests_from_url_entries_of_array_fields() {
        let sharing_url = format!("https://docs.google.com/spreadsheets/d/{ID}/edit?usp=sharing");
        let input = json!({
            "startUrls": [
                { "url": sharing_url },
                { "requestsFromUrl": sharing_url, "method": "POST", "userData": { "label": "LIST" } },
                { "requestsFromUrl": "https://example.com/urls.txt" },
                "https://example.com",
            ],
            "sheetUrl": sharing_url,
            "nested": { "requestsFromUrl": sharing_url },
        });
        let Value::Object(input) = input else { unreachable!() };
        assert_eq!(
            Value::Object(fix_google_sheets_urls(input)),
            json!({
                "startUrls": [
                    { "url": sharing_url },
                    { "requestsFromUrl": csv_url(), "method": "POST", "userData": { "label": "LIST" } },
                    { "requestsFromUrl": "https://example.com/urls.txt" },
                    "https://example.com",
                ],
                "sheetUrl": sharing_url,
                "nested": { "requestsFromUrl": sharing_url },
            })
        );
    }
}
