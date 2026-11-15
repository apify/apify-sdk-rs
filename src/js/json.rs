//! A JSON value borrowed from the text it was read from: what the typed extractors copy into the
//! items as it is (see [`super::typed::Raw`]).
//!
//! A `serde_json::Value` allocates every string, every object key and the hash table of every
//! object, and the item copied from it is freed string by string after the push. [`Json`] borrows
//! the strings and keys from the response body instead (those without escapes; the others are
//! decoded into owned strings, as `Value` does), and keeps an object's entries in a `Vec`.
//!
//! It reads and writes the same as a `Value` (with `preserve_order`, without
//! `arbitrary_precision`): numbers are kept as a [`Number`], so `1.0`, `1e5` and integers past
//! `u64` come out as they did (`1.0`, `100000.0`, `1.2345678901234568e22`), strings come out with
//! the escapes `serde_json` writes, and a key repeated in an object keeps its first place and its
//! last value. It accepts and rejects the same bodies (it is read through the same `Deserializer`
//! calls).

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;

use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use serde_json::{Map, Number, Value};

use super::typed::Key;

/// A JSON value, its strings and keys borrowed from the input when they have no escapes.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum Json<'a> {
    #[default]
    Null,
    Bool(bool),
    Number(Number),
    String(Cow<'a, str>),
    Array(Vec<Json<'a>>),
    /// The entries in the order of their first appearance, each key once.
    Object(Vec<(Cow<'a, str>, Json<'a>)>),
}

impl<'a> Json<'a> {
    pub fn is_null(&self) -> bool {
        matches!(self, Json::Null)
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Json::String(text) => Some(text),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[Json<'a>]> {
        match self {
            Json::Array(items) => Some(items),
            _ => None,
        }
    }

    /// `value[key]` of an object.
    pub fn get(&self, key: &str) -> Option<&Json<'a>> {
        match self {
            Json::Object(entries) => entries.iter().find(|(name, _)| name == key).map(|(_, value)| value),
            _ => None,
        }
    }

    /// The value as a `Value` (copying its strings).
    pub fn to_value(&self) -> Value {
        match self {
            Json::Null => Value::Null,
            Json::Bool(value) => Value::Bool(*value),
            Json::Number(number) => Value::Number(number.clone()),
            Json::String(text) => Value::String(text.as_ref().to_owned()),
            Json::Array(items) => Value::Array(items.iter().map(Json::to_value).collect()),
            Json::Object(entries) => Value::Object(
                entries.iter().map(|(key, value)| (key.as_ref().to_owned(), value.to_value())).collect::<Map<_, _>>(),
            ),
        }
    }

    /// The value with its strings and keys copied out of the input, to outlive it.
    pub fn into_owned(self) -> Json<'static> {
        let owned = |text: Cow<str>| Cow::Owned(text.into_owned());
        match self {
            Json::Null => Json::Null,
            Json::Bool(value) => Json::Bool(value),
            Json::Number(number) => Json::Number(number),
            Json::String(text) => Json::String(owned(text)),
            Json::Array(items) => Json::Array(items.into_iter().map(Json::into_owned).collect()),
            Json::Object(entries) => {
                Json::Object(entries.into_iter().map(|(key, value)| (owned(key), value.into_owned())).collect())
            }
        }
    }

    /// The value as a `Value` (moving the strings it owns).
    pub fn into_value(self) -> Value {
        match self {
            Json::Null => Value::Null,
            Json::Bool(value) => Value::Bool(value),
            Json::Number(number) => Value::Number(number),
            Json::String(text) => Value::String(text.into_owned()),
            Json::Array(items) => Value::Array(items.into_iter().map(Json::into_value).collect()),
            Json::Object(entries) => Value::Object(
                entries.into_iter().map(|(key, value)| (key.into_owned(), value.into_value())).collect::<Map<_, _>>(),
            ),
        }
    }
}

impl From<Value> for Json<'static> {
    /// Moves the strings of the value.
    fn from(value: Value) -> Self {
        match value {
            Value::Null => Json::Null,
            Value::Bool(value) => Json::Bool(value),
            Value::Number(number) => Json::Number(number),
            Value::String(text) => Json::String(Cow::Owned(text)),
            Value::Array(items) => Json::Array(items.into_iter().map(Json::from).collect()),
            Value::Object(fields) => {
                Json::Object(fields.into_iter().map(|(key, value)| (Cow::Owned(key), Json::from(value))).collect())
            }
        }
    }
}

impl From<String> for Json<'_> {
    fn from(text: String) -> Self {
        Json::String(Cow::Owned(text))
    }
}

/// Above this many entries, an object being read finds its repeated keys through a map.
const SCANNED_ENTRIES: usize = 16;

impl<'de: 'a, 'a> Deserialize<'de> for Json<'a> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct JsonVisitor<'a>(PhantomData<Json<'a>>);

        // As `Value`'s own visitor reads a value.
        impl<'de: 'a, 'a> Visitor<'de> for JsonVisitor<'a> {
            type Value = Json<'a>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("any valid JSON value")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Json<'a>, E> {
                Ok(Json::Bool(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Json<'a>, E> {
                Ok(Json::Number(value.into()))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Json<'a>, E> {
                Ok(Json::Number(value.into()))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Json<'a>, E> {
                Ok(Number::from_f64(value).map_or(Json::Null, Json::Number))
            }

            fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Json<'a>, E> {
                Ok(Json::String(Cow::Borrowed(value)))
            }

            fn visit_str<E>(self, value: &str) -> Result<Json<'a>, E> {
                Ok(Json::String(Cow::Owned(value.to_owned())))
            }

            fn visit_string<E>(self, value: String) -> Result<Json<'a>, E> {
                Ok(Json::String(Cow::Owned(value)))
            }

            fn visit_none<E>(self) -> Result<Json<'a>, E> {
                Ok(Json::Null)
            }

            fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Json<'a>, D::Error> {
                Json::deserialize(deserializer)
            }

            fn visit_unit<E>(self) -> Result<Json<'a>, E> {
                Ok(Json::Null)
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Json<'a>, A::Error> {
                let mut items = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(64));
                while let Some(item) = seq.next_element()? {
                    items.push(item);
                }
                Ok(Json::Array(items))
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Json<'a>, A::Error> {
                let mut entries: Vec<(Cow<'a, str>, Json<'a>)> = Vec::new();
                // The places of the keys, once the object is too large to scan.
                let mut places: Option<HashMap<Cow<'a, str>, usize>> = None;
                while let Some(key) = map.next_key::<Key<'de>>()? {
                    let key: Cow<'a, str> = match key {
                        Key::Borrowed(key) => Cow::Borrowed(key),
                        Key::Owned(key) => Cow::Owned(key),
                    };
                    let value: Json<'a> = map.next_value()?;
                    // A repeated key keeps its place and takes the new value, as in a `Value`.
                    let place = match &places {
                        Some(places) => places.get(&key).copied(),
                        None => entries.iter().position(|(name, _)| *name == key),
                    };
                    match place {
                        Some(place) => entries[place].1 = value,
                        None => {
                            if let Some(places) = &mut places {
                                places.insert(key.clone(), entries.len());
                            }
                            entries.push((key, value));
                            if places.is_none() && entries.len() > SCANNED_ENTRIES {
                                places = Some(
                                    entries
                                        .iter()
                                        .enumerate()
                                        .map(|(place, (name, _))| (name.clone(), place))
                                        .collect(),
                                );
                            }
                        }
                    }
                }
                Ok(Json::Object(entries))
            }
        }

        deserializer.deserialize_any(JsonVisitor(PhantomData))
    }
}

/// Written as `Value` writes it.
impl Serialize for Json<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Json::Null => serializer.serialize_unit(),
            Json::Bool(value) => serializer.serialize_bool(*value),
            Json::Number(number) => number.serialize(serializer),
            Json::String(text) => serializer.serialize_str(text),
            Json::Array(items) => {
                let mut seq = serializer.serialize_seq(Some(items.len()))?;
                for item in items {
                    seq.serialize_element(item)?;
                }
                seq.end()
            }
            Json::Object(entries) => {
                let mut map = serializer.serialize_map(Some(entries.len()))?;
                for (key, value) in entries {
                    map.serialize_entry(key.as_ref(), value)?;
                }
                map.end()
            }
        }
    }
}

// The JS semantics of `crate::js` for `Json`s.

/// `value?.key`.
pub fn at<'r, 'a>(value: Option<&'r Json<'a>>, key: &str) -> Option<&'r Json<'a>> {
    value?.get(key)
}

/// `value?.[index]`.
pub fn index<'r, 'a>(value: Option<&'r Json<'a>>, index: usize) -> Option<&'r Json<'a>> {
    value?.as_array()?.get(index)
}

/// `value?.key?.key2...`.
pub fn path<'r, 'a>(value: Option<&'r Json<'a>>, keys: &[&str]) -> Option<&'r Json<'a>> {
    keys.iter().try_fold(value?, |value, key| value.get(key))
}

/// Not `undefined` and not `null`: `value ?? undefined`.
pub fn defined<'r, 'a>(value: Option<&'r Json<'a>>) -> Option<&'r Json<'a>> {
    value.filter(|value| !value.is_null())
}

pub fn truthy(value: Option<&Json>) -> bool {
    match value {
        None | Some(Json::Null) => false,
        Some(Json::Bool(value)) => *value,
        Some(Json::Number(number)) => number.as_f64().is_some_and(|number| number != 0.0 && !number.is_nan()),
        Some(Json::String(text)) => !text.is_empty(),
        Some(Json::Array(_) | Json::Object(_)) => true,
    }
}

/// `a || b`.
pub fn or<'a>(a: Option<&Json<'a>>, b: Option<&Json<'a>>) -> Option<Json<'a>> {
    if truthy(a) { a } else { b }.cloned()
}

/// `value || undefined`.
pub fn truthy_or_undefined<'a>(value: Option<&Json<'a>>) -> Option<Json<'a>> {
    value.filter(|value| truthy(Some(value))).cloned()
}

/// `a ?? b`.
pub fn coalesce<'a>(a: Option<&Json<'a>>, b: Option<&Json<'a>>) -> Option<Json<'a>> {
    defined(a).or(b).cloned()
}

/// `a === b` for primitive values (ids). Objects are never equal.
pub fn strict_equals(a: Option<&Json>, b: Option<&Json>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(Json::Null), Some(Json::Null)) => true,
        (Some(Json::Bool(a)), Some(Json::Bool(b))) => a == b,
        (Some(Json::Number(a)), Some(Json::Number(b))) => a.as_f64() == b.as_f64(),
        (Some(Json::String(a)), Some(Json::String(b))) => a == b,
        _ => false,
    }
}

/// `String(value)` for primitives.
pub fn to_js_string(value: Option<&Json>) -> String {
    match value {
        Some(Json::String(text)) => text.as_ref().to_owned(),
        // Not a string: no allocation, but for arrays and objects.
        other => super::to_js_string(other.map(Json::to_value).as_ref()),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::js;

    fn values() -> Vec<Value> {
        let mut values: Vec<Value> = serde_json::from_str(
            r#"[null, true, false, 0, -0, -0.0, 1, -1, 1.0, 1.5, 1e5, 1E-7, 0.1, 12345678901234567890123,
                18446744073709551615, 18446744073709551616, -9223372036854775808, -9223372036854775809,
                "", " ", "0", " 12 ", "x", "caf\u00e9", "a\"b\\c\n\u0001\/", "\ud83d\ude00",
                [], [0], {}, {"a": 1}]"#,
        )
        .unwrap();
        values.push(json!(f64::MAX));
        values
    }

    /// Reads, writes and converts as `Value` does.
    #[test]
    fn reads_and_writes_as_value() {
        for value in values() {
            let text = value.to_string();
            let json: Json = serde_json::from_str(&text).unwrap();
            assert_eq!(serde_json::to_string(&json).unwrap(), text, "{text}");
            assert_eq!(json.to_value(), value, "{text}");
            assert_eq!(json.clone().into_value(), value, "{text}");
            assert_eq!(Json::from(value.clone()), json, "{text}");
            assert_eq!(serde_json::to_value(&json).unwrap(), value, "{text}");
            // From a `Value`, borrowed and owned.
            assert_eq!(Json::deserialize(&value).unwrap(), json, "{text}");
            assert_eq!(Json::deserialize(value.clone()).unwrap(), json, "{text}");
        }
    }

    /// Bodies as an API may write them: escapes, numbers in any form, repeated keys, spacing.
    /// They come out as from a `Value`, byte for byte, and fail the same.
    #[test]
    fn written_as_from_a_value() {
        let many_keys = (0..40).map(|key| format!(r#""k{}":{key}"#, key % 25)).collect::<Vec<_>>().join(",");
        let deep = format!("{}{}", "[".repeat(200), "]".repeat(200));
        for text in [
            r#"{"a":1,"b":2,"a":3,"c":{"x":1,"x":null},"b":[1.0,1e5,-0,1E4]}"#,
            r#"{"a":1,"b":2,"a":3}"#,
            r#" { "url" : "https:\/\/x.com\/p?a=1&b=\u00e9" , "n" : 1.50 , "big" : 123456789012345678901234567890 } "#,
            r#"["caf\u00e9", "\ud83d\ude00", "\n\t\"\\", "\u001f\u007f", "é😀"]"#,
            r#"[1.0, 1e5, 1E5, 1e-5, -0, -0.0, 0.10000000000000001, 18446744073709551616, -9223372036854775809]"#,
            r#"{"\u0061":1,"a":2}"#,
            &format!("{{{many_keys}}}"),
            &deep,
            r#"{"a":"\ud800"}"#,
            r#"{"a":1e400}"#,
            r#"{"a":"\q"}"#,
            r#"{"a":1,}"#,
            r#"{"a":1} x"#,
            "",
        ] {
            match (serde_json::from_str::<Value>(text), serde_json::from_str::<Json>(text)) {
                (Ok(value), Ok(json)) => {
                    assert_eq!(serde_json::to_string(&json).unwrap(), serde_json::to_string(&value).unwrap(), "{text}")
                }
                (Err(expected), Err(actual)) => assert_eq!(actual.to_string(), expected.to_string(), "{text}"),
                (expected, actual) => panic!("{text}: {expected:?} but {actual:?}"),
            }
        }
        assert_eq!(
            serde_json::from_slice::<Json>(b"[\"\xff\"]").unwrap_err().to_string(),
            serde_json::from_slice::<Value>(b"[\"\xff\"]").unwrap_err().to_string()
        );
    }

    /// Strings without escapes are borrowed from the input; with escapes, decoded.
    #[test]
    fn borrows_strings_without_escapes() {
        let text = r#"{"plain":"https://x.com/a","escaped":"caf\u00e9","k\u0065y":1}"#;
        let json: Json = serde_json::from_str(text).unwrap();
        let Json::Object(entries) = &json else { panic!() };
        assert!(matches!(&entries[0], (Cow::Borrowed("plain"), Json::String(Cow::Borrowed("https://x.com/a")))));
        assert!(matches!(&entries[1].1, Json::String(Cow::Owned(text)) if text == "café"));
        assert!(matches!(&entries[2].0, Cow::Owned(key) if key == "key"));
        assert_eq!(json.get("key"), Some(&Json::Number(1.into())));
    }

    /// The JS semantics are those of `crate::js` on `Value`s.
    #[test]
    fn js_semantics_as_on_values() {
        let values = values();
        let jsons: Vec<Json> = values.iter().cloned().map(Json::from).collect();
        let pairs = || values.iter().map(Some).chain([None]).zip(jsons.iter().map(Some).chain([None]));
        for (value, json) in pairs() {
            assert_eq!(truthy(json), js::truthy(value), "{value:?}");
            assert_eq!(to_js_string(json), js::to_js_string(value), "{value:?}");
            assert_eq!(defined(json).map(Json::to_value).as_ref(), js::defined(value), "{value:?}");
            assert_eq!(truthy_or_undefined(json).map(Json::into_value), js::truthy_or_undefined(value), "{value:?}");
            for (other_value, other_json) in pairs() {
                assert_eq!(
                    strict_equals(json, other_json),
                    js::strict_equals(value, other_value),
                    "{value:?} {other_value:?}"
                );
                assert_eq!(or(json, other_json).map(Json::into_value), js::or(value, other_value));
                assert_eq!(coalesce(json, other_json).map(Json::into_value), js::coalesce(value, other_value));
            }
        }
        let value = json!({ "a": [{ "b": 1 }], "c": null });
        let json = Json::from(value.clone());
        assert_eq!(path(Some(&json), &["a"]).map(Json::to_value).as_ref(), js::path(Some(&value), &["a"]));
        assert_eq!(
            at(index(at(Some(&json), "a"), 0), "b").map(Json::to_value).as_ref(),
            js::at(js::index(js::at(Some(&value), "a"), 0), "b")
        );
        assert_eq!(path(Some(&json), &["a", "b"]), None);
        assert_eq!(at(Some(&json), "c"), Some(&Json::Null));
    }
}
