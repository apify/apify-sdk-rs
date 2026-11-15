//! The JavaScript semantics that Rust ports of JS Actors rely on, so that their items come out the
//! same as the JS Actor's: `undefined` (a missing value, left out of the JSON) as opposed to `null`,
//! truthiness, `??` and `||`, `String(value)` and the key order of object literals and spreads.
//!
//! - [`json`]: [`Json`](json::Json), a JSON value borrowed from the text it was read from, with the
//!   same semantics.
//! - [`typed`]: reading JSON into typed structs ([`js_object!`](crate::js_object)) as JS reads it.
//! - [`number`]: numbers as JS reads and writes them (`String(number)`, `Number(text)`,
//!   `parseFloat`, `parseInt`, `Number(x.toFixed(n))`, `Math.round`).

pub mod json;
pub mod number;
pub mod typed;

use std::borrow::Cow;

use chrono::{DateTime, SecondsFormat};
use indexmap::IndexMap;
use serde_json::{Map, Value};

use self::number::{js_number_to_string, js_string_to_number};

/// The text of a body, its invalid UTF-8 replaced (`String::from_utf8_lossy`). A valid body, the
/// usual one, is borrowed after `str::from_utf8`'s check, which reads a word at a time where the
/// replacing conversion reads a byte at a time.
pub fn text_of(bytes: &[u8]) -> Cow<'_, str> {
    match std::str::from_utf8(bytes) {
        Ok(text) => Cow::Borrowed(text),
        Err(_) => String::from_utf8_lossy(bytes),
    }
}

/// An object being built: `None` is `undefined`, which `JSON.stringify` leaves out. Setting a key
/// again keeps its position, as in JS.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Obj(IndexMap<String, Option<Value>>);

impl Obj {
    pub fn new() -> Self {
        Obj::default()
    }

    /// `obj.key = value`.
    pub fn set(&mut self, key: &str, value: Option<Value>) -> &mut Self {
        self.0.insert(key.to_owned(), value);
        self
    }

    /// `obj.key = value`, for a value that is never `undefined`.
    pub fn put(&mut self, key: &str, value: impl Into<Value>) -> &mut Self {
        self.set(key, Some(value.into()))
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key).and_then(Option::as_ref)
    }

    /// `{ ...this, ...other }`.
    pub fn spread(&mut self, other: Obj) -> &mut Self {
        for (key, value) in other.0 {
            self.0.insert(key, value);
        }
        self
    }

    /// `{ ...this, ...value }` for a JSON value (anything but an object spreads nothing).
    pub fn spread_value(&mut self, value: Option<&Value>) -> &mut Self {
        if let Some(Value::Object(map)) = value {
            for (key, value) in map {
                self.0.insert(key.clone(), Some(value.clone()));
            }
        }
        self
    }

    /// The object as `JSON.stringify` writes it.
    pub fn into_value(self) -> Value {
        Value::Object(self.0.into_iter().filter_map(|(key, value)| Some((key, value?))).collect::<Map<_, _>>())
    }
}

impl From<Obj> for Value {
    fn from(obj: Obj) -> Value {
        obj.into_value()
    }
}

/// `value?.key`.
pub fn at<'a>(value: Option<&'a Value>, key: &str) -> Option<&'a Value> {
    value?.as_object()?.get(key)
}

/// `value?.[index]`.
pub fn index(value: Option<&Value>, index: usize) -> Option<&Value> {
    value?.as_array()?.get(index)
}

/// `value?.key?.key2...`.
pub fn path<'a>(value: Option<&'a Value>, keys: &[&str]) -> Option<&'a Value> {
    keys.iter().try_fold(value?, |value, key| at(Some(value), key))
}

/// Not `undefined` and not `null` (`isDefined`), as an `Option`: `value ?? undefined`.
pub fn defined(value: Option<&Value>) -> Option<&Value> {
    value.filter(|value| !value.is_null())
}

pub fn truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_f64().is_some_and(|n| n != 0.0 && !n.is_nan()),
        Some(Value::String(s)) => !s.is_empty(),
        Some(Value::Array(_) | Value::Object(_)) => true,
    }
}

/// `a || b`.
pub fn or(a: Option<&Value>, b: Option<&Value>) -> Option<Value> {
    or_ref(a, b).cloned()
}

/// `a || b`, without copying the value.
pub fn or_ref<'a>(a: Option<&'a Value>, b: Option<&'a Value>) -> Option<&'a Value> {
    if truthy(a) { a } else { b }
}

/// `value || undefined`.
pub fn truthy_or_undefined(value: Option<&Value>) -> Option<Value> {
    value.filter(|value| truthy(Some(value))).cloned()
}

/// `a ?? b`.
pub fn coalesce(a: Option<&Value>, b: Option<&Value>) -> Option<Value> {
    defined(a).or(b).cloned()
}

/// `a === b` for the primitive values the extractors compare (ids). Objects are never equal.
pub fn strict_equals(a: Option<&Value>, b: Option<&Value>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(Value::Null), Some(Value::Null)) => true,
        (Some(Value::Bool(a)), Some(Value::Bool(b))) => a == b,
        (Some(Value::Number(a)), Some(Value::Number(b))) => a.as_f64() == b.as_f64(),
        (Some(Value::String(a)), Some(Value::String(b))) => a == b,
        _ => false,
    }
}

/// `String(value)` for primitives.
pub fn to_js_string(value: Option<&Value>) -> String {
    match value {
        None => "undefined".to_owned(),
        Some(Value::Null) => "null".to_owned(),
        Some(Value::Bool(b)) => b.to_string(),
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => match (n.as_i64(), n.as_u64(), n.as_f64()) {
            (Some(i), _, _) => i.to_string(),
            (_, Some(u), _) => u.to_string(),
            (_, _, Some(f)) => js_number_to_string(f),
            _ => n.to_string(),
        },
        Some(other) => other.to_string(),
    }
}

/// `+value` for numbers and numeric strings; `None` for `NaN`.
pub fn to_number(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => Some(js_string_to_number(s)).filter(|n| !n.is_nan()),
        Value::Bool(b) => Some(f64::from(u8::from(*b))),
        Value::Null => Some(0.0),
        _ => None,
    }
}

/// `unixSecsToISO`: seconds since the epoch as an ISO date (`2024-01-31T12:00:00.000Z`), other
/// truthy values as text, and `null` otherwise.
pub fn unix_secs_to_iso(value: Option<&Value>) -> Value {
    if let Some(secs) = to_number(value).filter(|secs| *secs != 0.0 && secs.is_finite())
        && let Some(date) = DateTime::from_timestamp_millis((secs * 1000.0).trunc() as i64)
    {
        return Value::String(date.to_rfc3339_opts(SecondsFormat::Millis, true));
    }
    if truthy(value) { Value::String(to_js_string(value)) } else { Value::Null }
}

/// `\s` of JS regular expressions (and what `String.prototype.trim` removes).
pub fn is_js_space(c: char) -> bool {
    matches!(
        c,
        '\t' | '\n' | '\u{0B}' | '\u{0C}' | '\r' | ' ' | '\u{A0}' | '\u{1680}' | '\u{2000}'
            ..='\u{200A}' | '\u{2028}' | '\u{2029}' | '\u{202F}' | '\u{205F}' | '\u{3000}' | '\u{FEFF}'
    )
}

pub fn js_trim(text: &str) -> &str {
    text.trim_matches(is_js_space)
}

/// `encodeURIComponent` (and `querystring.escape`, which escapes the same characters).
pub use crawlee::utils::encode_uri_component;

/// An array index key (`0` to `2^32 - 2`, canonical): JS objects list these first, ascending.
pub fn array_index(key: &str) -> Option<u32> {
    key.parse::<u32>().ok().filter(|index| *index != u32::MAX && index.to_string() == key)
}

/// The fields of an object in the order JS lists them (`Object.keys`): array indexes first,
/// ascending, then the rest in insertion order.
pub fn js_key_order(fields: Map<String, Value>) -> Map<String, Value> {
    if !fields.keys().any(|key| array_index(key).is_some()) {
        return fields;
    }
    let (mut indexes, rest): (Vec<_>, Vec<_>) = fields.into_iter().partition(|(key, _)| array_index(key).is_some());
    indexes.sort_by_key(|(key, _)| array_index(key));
    indexes.into_iter().chain(rest).collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn undefined_keys_are_left_out_but_keep_their_position() {
        let mut obj = Obj::new();
        obj.put("a", 1).set("b", None).put("c", Value::Null);
        let mut other = Obj::new();
        other.put("b", 2).set("a", None).put("d", 4);
        obj.spread(other);
        assert_eq!(serde_json::to_string(&obj.into_value()).unwrap(), r#"{"b":2,"c":null,"d":4}"#);
    }

    #[test]
    fn values() {
        assert_eq!(to_js_string(Some(&json!(12.0))), "12");
        assert_eq!(to_js_string(Some(&json!(1.5))), "1.5");
        assert_eq!(unix_secs_to_iso(Some(&json!(1700000000))), json!("2023-11-14T22:13:20.000Z"));
        assert_eq!(unix_secs_to_iso(Some(&json!("1700000000"))), json!("2023-11-14T22:13:20.000Z"));
        assert_eq!(unix_secs_to_iso(Some(&json!("yesterday"))), json!("yesterday"));
        assert_eq!(unix_secs_to_iso(None), Value::Null);
        assert_eq!(unix_secs_to_iso(Some(&json!(0))), Value::Null);
        assert!(!truthy(Some(&json!(""))) && truthy(Some(&json!([]))) && !truthy(Some(&json!(0))));
        assert!(strict_equals(None, None) && !strict_equals(None, Some(&Value::Null)));
    }

    #[test]
    fn string_of_numbers() {
        assert_eq!(to_js_string(Some(&json!(5.0))), "5"); // String(5.0)
        assert_eq!(to_js_string(Some(&json!(-0.0))), "0"); // String(-0)
        assert_eq!(to_js_string(Some(&json!(1e21))), "1e+21"); // String(1e21)
        assert_eq!(to_js_string(Some(&json!(1e-7))), "1e-7"); // String(1e-7)
        assert_eq!(to_js_string(Some(&json!(0.1 + 0.2))), "0.30000000000000004"); // String(0.1 + 0.2)
    }

    #[test]
    fn unary_plus() {
        assert_eq!(to_number(Some(&json!(" 12 "))), Some(12.0)); // +" 12 "
        assert_eq!(to_number(Some(&json!(""))), Some(0.0)); // +""
        assert_eq!(to_number(Some(&json!("\u{FEFF}7\u{A0}"))), Some(7.0)); // +"\uFEFF7\u00A0"
        assert_eq!(to_number(Some(&json!("0x1A"))), Some(26.0)); // +"0x1A"
        assert_eq!(to_number(Some(&json!("Infinity"))), Some(f64::INFINITY)); // +"Infinity"
        assert_eq!(to_number(Some(&json!("inf"))), None); // +"inf" is NaN
        assert_eq!(to_number(Some(&json!("12px"))), None); // +"12px" is NaN
        assert_eq!(to_number(Some(&json!(true))), Some(1.0)); // +true
        assert_eq!(to_number(Some(&Value::Null)), Some(0.0)); // +null
        assert_eq!(to_number(None), None); // +undefined is NaN
    }

    #[test]
    fn trims_js_white_space() {
        assert_eq!(js_trim("\u{FEFF}\u{A0}\t a b \u{3000}\n"), "a b"); // "\uFEFF\u00A0\t a b \u3000\n".trim()
        assert_eq!(js_trim("\u{85}a"), "\u{85}a"); // "\u0085a".trim(): NEL is not white space in JS
        assert!(is_js_space('\u{2028}') && !is_js_space('\u{200B}')); // /\s/.test("\u2028"), /\s/.test("\u200B")
    }

    #[test]
    fn encodes_uri_components() {
        assert_eq!(encode_uri_component("AZaz09-_.!~*'()"), "AZaz09-_.!~*'()"); // encodeURIComponent("AZaz09-_.!~*'()")
        // encodeURIComponent(";/?:@&=+$,# %")
        assert_eq!(encode_uri_component(";/?:@&=+$,# %"), "%3B%2F%3F%3A%40%26%3D%2B%24%2C%23%20%25");
        assert_eq!(encode_uri_component("café"), "caf%C3%A9"); // encodeURIComponent("café")
        assert_eq!(encode_uri_component("😀"), "%F0%9F%98%80"); // encodeURIComponent("😀")
        assert_eq!(encode_uri_component(r#"{"a":[1]}"#), "%7B%22a%22%3A%5B1%5D%7D"); // encodeURIComponent('{"a":[1]}')
    }

    #[test]
    fn lists_keys_as_object_keys_does() {
        let fields: Map<String, Value> =
            serde_json::from_str(r#"{"b":0,"2":0,"a":0,"10":0,"1":0,"01":0,"-1":0,"1.5":0}"#).unwrap();
        // Object.keys({ b: 0, 2: 0, a: 0, 10: 0, 1: 0, "01": 0, "-1": 0, "1.5": 0 })
        let expected = ["1", "2", "10", "b", "a", "01", "-1", "1.5"];
        assert_eq!(js_key_order(fields).keys().collect::<Vec<_>>(), expected);
        assert_eq!(array_index("4294967294"), Some(4_294_967_294));
        assert_eq!(array_index("4294967295"), None);
    }
}
