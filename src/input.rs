//! The input of an Actor run: the `INPUT` record of the default key-value store, or locally an
//! `INPUT` / `INPUT.json` file in the working or default key-value store directory.

use std::path::Path;

use bytes::Bytes;
use serde_json::{Map, Value};

pub(crate) const JSON_CONTENT_TYPE: &str = "application/json; charset=utf-8";
pub(crate) const BINARY_CONTENT_TYPE: &str = "application/octet-stream";

/// Why the input could not be read (`ActorInputError` codes of the JS SDK).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActorInputErrorCode {
    NotFound,
    MultipleFiles,
    DecryptionFailed,
    ParseFailed,
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct ActorInputError {
    pub code: ActorInputErrorCode,
    pub message: String,
    #[source]
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl ActorInputError {
    pub(crate) fn new(code: ActorInputErrorCode, message: impl Into<String>) -> Self {
        ActorInputError { code, message: message.into(), source: None }
    }

    pub(crate) fn caused_by(mut self, source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        self.source = Some(source.into());
        self
    }
}

/// The input as it was stored.
#[derive(Clone, Debug, PartialEq)]
pub enum Input {
    Json(Value),
    Text(String),
    Binary(Bytes),
}

impl Input {
    /// The input as JSON: text is a JSON string; binary input is not JSON.
    pub fn into_json(self) -> Option<Value> {
        match self {
            Input::Json(value) => Some(value),
            Input::Text(text) => Some(Value::String(text)),
            Input::Binary(_) => None,
        }
    }
}

/// Parses a stored value by its content type, like `parseInputValue` of the JS SDK: JSON is
/// parsed, `text/*` is text, and binary values are parsed as JSON when they are JSON.
pub(crate) fn parse_input(value: Bytes, content_type: Option<&str>, source: &str) -> Result<Input, ActorInputError> {
    let media_type =
        content_type.and_then(|ct| ct.split(';').next()).map(|mt| mt.trim().to_ascii_lowercase()).unwrap_or_default();
    let parse_failed = |err: serde_json::Error| {
        ActorInputError::new(ActorInputErrorCode::ParseFailed, format!("The input in {source} is not valid JSON."))
            .caused_by(err)
    };
    if media_type == BINARY_CONTENT_TYPE {
        return Ok(serde_json::from_slice(&value).map(Input::Json).unwrap_or(Input::Binary(value)));
    }
    if media_type == "application/json" || media_type.ends_with("+json") {
        return serde_json::from_slice(&value).map(Input::Json).map_err(parse_failed);
    }
    if media_type.starts_with("text/") {
        return Ok(Input::Text(String::from_utf8_lossy(&value).into_owned()));
    }
    Ok(Input::Binary(value))
}

/// The input file in `directory` (described as `label` in messages): `<key>` or `<key>.json`, not both.
pub(crate) async fn read_input_file(
    directory: &Path,
    label: &str,
    input_key: &str,
) -> Result<Option<Input>, ActorInputError> {
    let candidates = [(input_key.to_owned(), BINARY_CONTENT_TYPE), (format!("{input_key}.json"), JSON_CONTENT_TYPE)];
    let mut found = Vec::new();
    for (filename, content_type) in candidates {
        let path = directory.join(&filename);
        if tokio::fs::metadata(&path).await.is_ok_and(|metadata| metadata.is_file()) {
            found.push((filename, content_type, path));
        }
    }
    if found.len() > 1 {
        let names: Vec<String> = found.iter().map(|(filename, _, _)| format!("\"{filename}\"")).collect();
        return Err(ActorInputError::new(
            ActorInputErrorCode::MultipleFiles,
            format!("Found multiple input files in the {label}: {}. Keep only one of them.", names.join(", ")),
        ));
    }
    let Some((filename, content_type, path)) = found.pop() else { return Ok(None) };
    let value = tokio::fs::read(&path).await.map_err(|err| {
        ActorInputError::new(ActorInputErrorCode::NotFound, format!("Failed to read {}", path.display())).caused_by(err)
    })?;
    parse_input(Bytes::from(value), Some(content_type), &format!("the \"{filename}\" file in the {label}")).map(Some)
}

/// Where the input schema is looked for when `.actor/actor.json` does not say.
const DEFAULT_INPUT_SCHEMA_PATHS: [&str; 4] =
    [".actor/INPUT_SCHEMA.json", "INPUT_SCHEMA.json", ".actor/input_schema.json", "input_schema.json"];

/// The Actor's input schema in `directory`.
#[derive(Debug, PartialEq)]
pub(crate) enum InputSchema {
    Found(Value),
    /// The Actor declares no input schema.
    NotDefined,
    /// `.actor/actor.json` declares one that could not be read.
    Missing,
}

fn read_json(path: &Path) -> Option<Value> {
    serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()
}

pub(crate) fn read_input_schema(directory: &Path) -> InputSchema {
    let actor_json = read_json(&directory.join(".actor/actor.json"));
    let declared = actor_json.as_ref().and_then(|config| config.get("input"));
    match declared {
        Some(schema @ Value::Object(_)) => return InputSchema::Found(schema.clone()),
        Some(Value::String(path)) => {
            return read_json(&directory.join(".actor").join(path)).map_or(InputSchema::Missing, InputSchema::Found);
        }
        _ => {}
    }
    for path in DEFAULT_INPUT_SCHEMA_PATHS {
        if let Some(schema) = read_json(&directory.join(path)) {
            return InputSchema::Found(schema);
        }
    }
    let declares_input = declared.is_some_and(|input| !matches!(input, Value::Null | Value::Bool(false)));
    if declares_input { InputSchema::Missing } else { InputSchema::NotDefined }
}

/// The input with the top-level defaults of the schema filled in.
pub(crate) fn apply_defaults(input: Map<String, Value>, schema: &Value) -> Map<String, Value> {
    let mut merged = Map::new();
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        for (key, field) in properties {
            if let Some(default) = field.get("default") {
                merged.insert(key.clone(), default.clone());
            }
        }
    }
    merged.extend(input);
    merged
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn values_are_parsed_by_content_type() {
        let parse = |value: &str, content_type| parse_input(Bytes::from(value.to_owned()), content_type, "test");
        assert_eq!(parse(r#"{"a":1}"#, Some("application/json; charset=utf-8")).unwrap(), Input::Json(json!({"a": 1})));
        assert_eq!(parse(r#"{"a":1}"#, Some(BINARY_CONTENT_TYPE)).unwrap(), Input::Json(json!({"a": 1})));
        assert_eq!(parse("raw", Some(BINARY_CONTENT_TYPE)).unwrap(), Input::Binary(Bytes::from("raw")));
        assert_eq!(parse("hello", Some("text/plain")).unwrap(), Input::Text("hello".to_owned()));
        let err = parse("{", Some("application/json")).unwrap_err();
        assert_eq!(err.code, ActorInputErrorCode::ParseFailed);
        assert_eq!(err.to_string(), "The input in test is not valid JSON.");
    }

    #[test]
    fn input_values_and_schema_defaults_golden() {
        let cases: Vec<Value> = serde_json::from_str(include_str!("../conformance/golden/input_values.json")).unwrap();
        for case in cases {
            let value = Bytes::from(case["value"].as_str().unwrap().to_owned());
            let parsed = parse_input(value, case["contentType"].as_str(), "test").unwrap();
            let expected = &case["expected"];
            match parsed {
                Input::Binary(bytes) => {
                    assert_eq!(expected["binary"], String::from_utf8_lossy(&bytes).as_ref(), "{case}")
                }
                Input::Text(text) => assert_eq!(expected["value"], text, "{case}"),
                Input::Json(json) => assert_eq!(expected["value"], json, "{case}"),
            }
        }
        let cases: Vec<Value> =
            serde_json::from_str(include_str!("../conformance/golden/schema_defaults.json")).unwrap();
        for case in cases {
            let merged = apply_defaults(case["input"].as_object().unwrap().clone(), &case["schema"]);
            assert_eq!(Value::Object(merged), case["expected"], "{case}");
        }
    }

    #[tokio::test]
    async fn input_files_in_the_working_directory() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read_input_file(dir.path(), "working directory", "INPUT").await.unwrap(), None);

        std::fs::write(dir.path().join("INPUT.json"), r#"{"a":1}"#).unwrap();
        assert_eq!(
            read_input_file(dir.path(), "working directory", "INPUT").await.unwrap(),
            Some(Input::Json(json!({"a": 1})))
        );

        std::fs::write(dir.path().join("INPUT"), "x").unwrap();
        let err = read_input_file(dir.path(), "working directory", "INPUT").await.unwrap_err();
        assert_eq!(err.code, ActorInputErrorCode::MultipleFiles);
        assert_eq!(
            err.to_string(),
            r#"Found multiple input files in the working directory: "INPUT", "INPUT.json". Keep only one of them."#
        );
    }

    #[test]
    fn schema_defaults_fill_in_top_level_fields() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read_input_schema(dir.path()), InputSchema::NotDefined);

        std::fs::create_dir(dir.path().join(".actor")).unwrap();
        std::fs::write(dir.path().join(".actor/actor.json"), r#"{"input": "./input_schema.json"}"#).unwrap();
        assert_eq!(read_input_schema(dir.path()), InputSchema::Missing);

        let schema = json!({ "properties": {
            "maxPages": { "type": "integer", "default": 10 },
            "start": { "type": "string", "default": "https://crawlee.dev" },
            "nested": { "type": "object", "default": { "a": 1 } },
            "noDefault": { "type": "string" },
        }});
        std::fs::write(dir.path().join(".actor/input_schema.json"), schema.to_string()).unwrap();
        let InputSchema::Found(found) = read_input_schema(dir.path()) else { panic!() };
        let input = json!({ "start": "https://apify.com", "nested": { "b": 2 } });
        let merged = apply_defaults(input.as_object().unwrap().clone(), &found);
        assert_eq!(
            Value::Object(merged),
            json!({ "maxPages": 10, "start": "https://apify.com", "nested": { "b": 2 } }),
            "only top-level defaults"
        );
    }
}
