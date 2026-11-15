//! Reading JSON into typed structs with the semantics of [`crate::js`], so that the extractors can
//! skip building a `serde_json::Value` tree of a whole response.
//!
//! The types accept any JSON where the extractors read a field, the way JS reads it:
//!
//! - [`Raw`]: a field read as it is: `undefined` when missing, else its value (`null` included),
//!   borrowed from the input (see [`Json`]).
//! - [`Js`]: an object the extractors walk into. Any other value is kept as it is (its fields are
//!   `undefined`, its truthiness is that of the value).
//! - [`List`]: an array of items. Any other value has no items.
//!
//! The structs (see [`js_object!`]) skip the fields they do not name, but still parse them, so a
//! body is accepted or rejected exactly as when it is parsed into a `Value` (UTF-8, escapes,
//! number range, nesting depth). A field named twice keeps its last value, as in a `Value`.

use std::fmt;
use std::marker::PhantomData;

use serde::de::value::SeqAccessDeserializer;
use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Number, Value};

use super::json::Json;
use super::truthy;

/// A field read as it is: `None` when it is missing (`undefined`), else its value, borrowed from
/// the input (see [`Json`]).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Raw<'a>(pub Option<Json<'a>>);

impl<'a> Raw<'a> {
    /// `object.field`.
    pub fn get(&self) -> Option<&Json<'a>> {
        self.0.as_ref()
    }

    /// `object.field`, moved out.
    pub fn take(&mut self) -> Option<Json<'a>> {
        self.0.take()
    }
}

impl<'de: 'a, 'a> Deserialize<'de> for Raw<'a> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Json::deserialize(deserializer).map(|value| Raw(Some(value)))
    }
}

/// A field the extractors read as an object: missing, an object, or another value (`null`
/// included), whose fields are all `undefined`.
#[derive(Clone, Debug, Default)]
pub enum Js<T> {
    #[default]
    Absent,
    Object(T),
    Other(Value),
}

impl<T> Js<T> {
    /// The object, `None` when the value is not one.
    pub fn object(&self) -> Option<&T> {
        match self {
            Js::Object(object) => Some(object),
            _ => None,
        }
    }

    pub fn into_object(self) -> Option<T> {
        match self {
            Js::Object(object) => Some(object),
            _ => None,
        }
    }

    pub fn object_mut(&mut self) -> Option<&mut T> {
        match self {
            Js::Object(object) => Some(object),
            _ => None,
        }
    }

    pub fn is_absent(&self) -> bool {
        matches!(self, Js::Absent)
    }

    /// Truthiness, as [`crate::js::truthy`]: every object is truthy.
    pub fn truthy(&self) -> bool {
        match self {
            Js::Absent => false,
            Js::Object(_) => true,
            Js::Other(value) => truthy(Some(value)),
        }
    }
}

impl<T: Default> Js<T> {
    /// The object, or one without fields (what JS reads from any other value).
    pub fn into_object_or_default(self) -> T {
        self.into_object().unwrap_or_default()
    }
}

/// A struct read from the entries of a JSON object (see [`js_object!`]).
pub trait FromMap<'de>: Sized {
    fn from_map<A: MapAccess<'de>>(map: A) -> Result<Self, A::Error>;
}

impl<'de, T: FromMap<'de>> Deserialize<'de> for Js<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct JsVisitor<T>(PhantomData<T>);

        impl<'de, T: FromMap<'de>> Visitor<'de> for JsVisitor<T> {
            type Value = Js<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("any JSON value")
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Js<T>, A::Error> {
                T::from_map(map).map(Js::Object)
            }

            fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<Js<T>, A::Error> {
                Value::deserialize(SeqAccessDeserializer::new(seq)).map(Js::Other)
            }

            // The rest as `Value`'s own visitor makes them.
            fn visit_bool<E>(self, value: bool) -> Result<Js<T>, E> {
                Ok(Js::Other(Value::Bool(value)))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Js<T>, E> {
                Ok(Js::Other(Value::Number(value.into())))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Js<T>, E> {
                Ok(Js::Other(Value::Number(value.into())))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Js<T>, E> {
                Ok(Js::Other(Number::from_f64(value).map_or(Value::Null, Value::Number)))
            }

            fn visit_str<E>(self, value: &str) -> Result<Js<T>, E> {
                Ok(Js::Other(Value::String(value.to_owned())))
            }

            fn visit_string<E>(self, value: String) -> Result<Js<T>, E> {
                Ok(Js::Other(Value::String(value)))
            }

            fn visit_unit<E>(self) -> Result<Js<T>, E> {
                Ok(Js::Other(Value::Null))
            }

            fn visit_none<E>(self) -> Result<Js<T>, E> {
                Ok(Js::Other(Value::Null))
            }

            fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Js<T>, D::Error> {
                Js::deserialize(deserializer)
            }
        }

        deserializer.deserialize_any(JsVisitor(PhantomData))
    }
}

/// A field the extractors read as an array: missing, an array, or another value (no items).
#[derive(Clone, Debug, Default)]
pub enum List<T> {
    #[default]
    Absent,
    Array(Vec<T>),
    Other,
}

impl<T> List<T> {
    /// The items, `None` when the value is not an array (`value?.map` of JS).
    pub fn array(&self) -> Option<&[T]> {
        match self {
            List::Array(items) => Some(items),
            _ => None,
        }
    }

    /// The items, none when the value is not an array.
    pub fn items(&self) -> &[T] {
        self.array().unwrap_or_default()
    }

    pub fn items_mut(&mut self) -> &mut [T] {
        match self {
            List::Array(items) => items,
            _ => &mut [],
        }
    }

    /// The items, moved out.
    pub fn into_items(self) -> Vec<T> {
        match self {
            List::Array(items) => items,
            _ => Vec::new(),
        }
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for List<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ListVisitor<T>(PhantomData<T>);

        impl<'de, T: Deserialize<'de>> Visitor<'de> for ListVisitor<T> {
            type Value = List<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("any JSON value")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<List<T>, A::Error> {
                let mut items = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(64));
                while let Some(item) = seq.next_element()? {
                    items.push(item);
                }
                Ok(List::Array(items))
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<List<T>, A::Error> {
                SkipVisitor.visit_map(map).map(|_| List::Other)
            }

            fn visit_bool<E>(self, _: bool) -> Result<List<T>, E> {
                Ok(List::Other)
            }

            fn visit_i64<E>(self, _: i64) -> Result<List<T>, E> {
                Ok(List::Other)
            }

            fn visit_u64<E>(self, _: u64) -> Result<List<T>, E> {
                Ok(List::Other)
            }

            fn visit_f64<E>(self, _: f64) -> Result<List<T>, E> {
                Ok(List::Other)
            }

            fn visit_str<E>(self, _: &str) -> Result<List<T>, E> {
                Ok(List::Other)
            }

            fn visit_unit<E>(self) -> Result<List<T>, E> {
                Ok(List::Other)
            }

            fn visit_none<E>(self) -> Result<List<T>, E> {
                Ok(List::Other)
            }

            fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<List<T>, D::Error> {
                List::deserialize(deserializer)
            }
        }

        deserializer.deserialize_any(ListVisitor(PhantomData))
    }
}

/// A value read typed from an input, as one that owns its strings, to outlive the input: an answer
/// read in a function that returns it, while its body is dropped there.
pub trait IntoOwned {
    type Owned: 'static;

    fn into_owned(self) -> Self::Owned;
}

impl IntoOwned for Raw<'_> {
    type Owned = Raw<'static>;

    fn into_owned(self) -> Raw<'static> {
        Raw(self.0.map(Json::into_owned))
    }
}

impl<T: IntoOwned> IntoOwned for Js<T> {
    type Owned = Js<T::Owned>;

    fn into_owned(self) -> Js<T::Owned> {
        match self {
            Js::Absent => Js::Absent,
            Js::Object(object) => Js::Object(object.into_owned()),
            Js::Other(value) => Js::Other(value),
        }
    }
}

impl<T: IntoOwned> IntoOwned for List<T> {
    type Owned = List<T::Owned>;

    fn into_owned(self) -> List<T::Owned> {
        match self {
            List::Absent => List::Absent,
            List::Array(items) => List::Array(items.into_iter().map(T::into_owned).collect()),
            List::Other => List::Other,
        }
    }
}

impl<T: IntoOwned> IntoOwned for Option<T> {
    type Owned = Option<T::Owned>;

    fn into_owned(self) -> Option<T::Owned> {
        self.map(T::into_owned)
    }
}

/// A key of an object, borrowed from the input when it has no escapes.
pub enum Key<'de> {
    Borrowed(&'de str),
    Owned(String),
}

impl Key<'_> {
    pub fn as_str(&self) -> &str {
        match self {
            Key::Borrowed(key) => key,
            Key::Owned(key) => key,
        }
    }
}

impl<'de> Deserialize<'de> for Key<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct KeyVisitor;

        impl<'de> Visitor<'de> for KeyVisitor {
            type Value = Key<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an object key")
            }

            fn visit_borrowed_str<E>(self, key: &'de str) -> Result<Key<'de>, E> {
                Ok(Key::Borrowed(key))
            }

            fn visit_str<E>(self, key: &str) -> Result<Key<'de>, E> {
                Ok(Key::Owned(key.to_owned()))
            }

            fn visit_string<E>(self, key: String) -> Result<Key<'de>, E> {
                Ok(Key::Owned(key))
            }
        }

        deserializer.deserialize_str(KeyVisitor)
    }
}

/// A value nobody reads, parsed as fully as a `Value` would be (`IgnoredAny` would accept bodies
/// that `Value` rejects: lone surrogates, numbers out of range, any nesting depth) without
/// building anything.
pub struct Skip;

struct SkipVisitor;

impl<'de> Visitor<'de> for SkipVisitor {
    type Value = Skip;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("any JSON value")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Skip, A::Error> {
        while map.next_entry::<Skip, Skip>()?.is_some() {}
        Ok(Skip)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Skip, A::Error> {
        while seq.next_element::<Skip>()?.is_some() {}
        Ok(Skip)
    }

    fn visit_bool<E>(self, _: bool) -> Result<Skip, E> {
        Ok(Skip)
    }

    fn visit_i64<E>(self, _: i64) -> Result<Skip, E> {
        Ok(Skip)
    }

    fn visit_u64<E>(self, _: u64) -> Result<Skip, E> {
        Ok(Skip)
    }

    fn visit_f64<E>(self, _: f64) -> Result<Skip, E> {
        Ok(Skip)
    }

    fn visit_str<E>(self, _: &str) -> Result<Skip, E> {
        Ok(Skip)
    }

    fn visit_unit<E>(self) -> Result<Skip, E> {
        Ok(Skip)
    }

    fn visit_none<E>(self) -> Result<Skip, E> {
        Ok(Skip)
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Skip, D::Error> {
        Skip::deserialize(deserializer)
    }
}

impl<'de> Deserialize<'de> for Skip {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(SkipVisitor)
    }
}

/// A struct of the JSON fields the extractors read: `field: Type = "jsonKey"`, with the lifetime of
/// the input its fields borrow from. Read through [`Js`]; the fields it does not name are skipped
/// (see [`Skip`]), and a field named twice keeps its last value. [`IntoOwned`] gives the struct
/// that outlives its input.
///
/// The macro is exported at the crate root and re-exported here: `apify::js_object!` and
/// `apify::js::typed::js_object!` are the same macro.
///
/// ```
/// use apify::js::json::Json;
/// use apify::js::typed::{IntoOwned, Js, List, Raw, js_object};
///
/// js_object! {
///     pub struct Place<'a> {
///         title: Raw<'a> = "title",
///         tags: List<Raw<'a>> = "tags",
///     }
/// }
///
/// let text = String::from(r#"{"title":"Café","tags":["a",1],"other":{}}"#);
/// let place: Js<Place> = serde_json::from_str(&text).unwrap();
/// let place: Js<Place<'static>> = place.into_owned();
/// drop(text);
/// let place = place.into_object().unwrap();
/// assert_eq!(place.title.get().and_then(Json::as_str), Some("Café"));
/// assert_eq!(place.tags.items().len(), 2);
/// ```
#[macro_export]
macro_rules! js_object {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident<$lt:lifetime> {
            $($(#[$field_meta:meta])* $field:ident: $ty:ty = $key:literal,)*
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Debug, Default)]
        $vis struct $name<$lt> {
            $($(#[$field_meta])* pub $field: $ty,)*
        }

        #[cfg(test)]
        impl $name<'_> {
            /// The JSON keys the struct reads.
            #[allow(dead_code)]
            pub const KEYS: &'static [&'static str] = &[$($key),*];
        }

        impl<$lt> $crate::js::typed::IntoOwned for $name<$lt> {
            type Owned = $name<'static>;

            fn into_owned(self) -> $name<'static> {
                $name {
                    $($field: $crate::js::typed::IntoOwned::into_owned(self.$field),)*
                }
            }
        }

        impl<'de: $lt, $lt> $crate::js::typed::FromMap<'de> for $name<$lt> {
            fn from_map<A: $crate::js::typed::__private::MapAccess<'de>>(
                mut map: A,
            ) -> ::core::result::Result<Self, A::Error> {
                let mut object = Self::default();
                while let Some(key) = map.next_key::<$crate::js::typed::Key<'de>>()? {
                    match key.as_str() {
                        $($key => object.$field = map.next_value()?,)*
                        _ => {
                            map.next_value::<$crate::js::typed::Skip>()?;
                        }
                    }
                }
                ::core::result::Result::Ok(object)
            }
        }
    };
}

pub use crate::js_object;

/// What [`js_object!`] expands to, so that crates using it need not depend on `serde`.
#[doc(hidden)]
pub mod __private {
    pub use serde::de::MapAccess;
}

/// Reads a value as `T`, borrowing its strings (the fallback paths and tests build `Value`s).
/// Cannot fail: every typed field takes any JSON value.
pub fn read<'a, T: FromMap<'a>>(value: &'a Value) -> Js<T> {
    Js::deserialize(value).unwrap_or_else(|_| Js::Other(value.clone()))
}

/// Reads a value as `T`, moving its strings, for a `T` that outlives the value. Cannot fail (see
/// [`read`]); were it to, the value would read as missing, which the extractors read as they read
/// a value that is not an object.
pub fn read_owned<T: FromMap<'static>>(value: Value) -> Js<T> {
    Js::deserialize(value).unwrap_or_default()
}

/// `JSON.parse(text)` read as `T` (see [`js_object!`]), owning what it read (see [`IntoOwned`]):
/// typed straight from the text, or, should that fail (not expected: the typed fields take any
/// JSON), through a `Value`, which also gives the error of a text that is not JSON.
pub fn parse_owned<'a, T>(text: &'a str) -> serde_json::Result<Js<T::Owned>>
where
    T: FromMap<'a> + IntoOwned,
    T::Owned: FromMap<'static>,
{
    match serde_json::from_str::<Js<T>>(text) {
        Ok(answer) => Ok(answer.into_owned()),
        Err(_) => serde_json::from_str::<Value>(text).map(read_owned),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    js_object! {
        struct Inner<'a> {
            name: Raw<'a> = "name",
        }
    }

    js_object! {
        struct Sample<'a> {
            name: Raw<'a> = "name",
            nested: Js<Inner<'a>> = "nested",
            items: List<Js<Inner<'a>>> = "items",
        }
    }

    fn parse(text: &str) -> Result<Js<Sample<'_>>, serde_json::Error> {
        serde_json::from_str(text)
    }

    #[test]
    fn reads_fields_as_js_does() {
        let sample =
            parse(r#"{"name":null,"nested":"x","items":{"a":1},"other":[1,{"b":2}]}"#).unwrap().into_object().unwrap();
        assert_eq!(sample.name.get(), Some(&Json::Null));
        assert!(sample.nested.truthy() && sample.nested.object().is_none());
        assert!(sample.items.array().is_none());

        let sample = parse(r#"{"nested":{"name":1},"nested":{"name":2},"items":[{"name":"a"},3]}"#)
            .unwrap()
            .into_object()
            .unwrap();
        assert!(
            sample.name.get().is_none() && sample.nested.object().unwrap().name.get() == Some(&Json::from(json!(2)))
        );
        assert_eq!(sample.items.items().len(), 2);
        assert!(sample.items.items()[1].object().is_none());

        assert!(matches!(parse("[]").unwrap(), Js::Other(Value::Array(_))));
        assert!(!parse("0").unwrap().truthy() && parse("{}").unwrap().truthy());
    }

    #[test]
    fn outlives_its_input_once_owned() {
        let text = String::from(r#"{"name":"a\u00e9","nested":{"name":[1,{"k":"v"}]},"items":[{"name":null},2]}"#);
        let sample = parse(&text).unwrap();
        let expected = format!("{sample:?}");
        let owned: Js<Sample<'static>> = sample.into_owned();
        drop(text);
        assert_eq!(format!("{owned:?}"), expected);
        let sample = owned.into_object().unwrap();
        assert_eq!(sample.name.get(), Some(&Json::from("aé".to_owned())));
        assert!(matches!(sample.nested.object().unwrap().name.get(), Some(Json::Array(items)) if items.len() == 2));
    }

    #[test]
    fn parses_owned_or_fails_as_value() {
        let sample = parse_owned::<Sample>(r#"{"name":"a","items":[{"name":1}]}"#).unwrap();
        let sample = sample.into_object().unwrap();
        assert_eq!(sample.name.get(), Some(&Json::from("a".to_owned())));
        assert_eq!(sample.items.items().len(), 1);
        for text in ["", "{", r#"{"a":"\ud800"}"#, "[] x"] {
            assert_eq!(
                parse_owned::<Sample>(text).unwrap_err().to_string(),
                serde_json::from_str::<Value>(text).unwrap_err().to_string()
            );
        }
    }

    #[test]
    fn rejects_what_value_rejects() {
        for text in [
            r#"{"other":"\ud800"}"#,
            r#"{"other":1e400}"#,
            &format!(r#"{{"other":{}{}}}"#, "[".repeat(200), "]".repeat(200)),
            r#"{"other":"\x"}"#,
            r#"{"name":1} x"#,
            "",
        ] {
            assert!(serde_json::from_str::<Value>(text).is_err(), "{text}");
            assert!(parse(text).is_err(), "{text}");
        }
        assert!(serde_json::from_slice::<Js<Sample>>(b"{\"other\":\"\xff\"}").is_err());
    }
}
