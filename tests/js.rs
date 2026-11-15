//! `apify::js` used from another crate: the `js_object!` macro expands outside the SDK, through
//! both of its paths, without the crate depending on `serde` itself.

use apify::js::json::Json;
use apify::js::typed::{IntoOwned, Js, List, Raw, read, read_owned};
use apify::js::{Obj, coalesce, to_js_string, truthy};
use serde_json::{Value, json};

apify::js_object! {
    /// A struct declared through the root path of the macro.
    struct Review<'a> {
        text: Raw<'a> = "text",
        stars: Raw<'a> = "stars",
    }
}

apify::js::typed::js_object! {
    pub struct Place<'a> {
        title: Raw<'a> = "title",
        location: Js<Location<'a>> = "location",
        reviews: List<Js<Review<'a>>> = "reviews",
    }
}

use apify::js::typed::js_object;

js_object! {
    pub(crate) struct Location<'a> {
        lat: Raw<'a> = "lat",
        lng: Raw<'a> = "lng",
    }
}

#[test]
fn reads_typed_structs_declared_outside_the_sdk() {
    let text = String::from(
        r#"{"title":"Café","location":{"lat":50.08,"lng":14.42},"reviews":[{"text":"ok","stars":5},null],"x":[1]}"#,
    );
    let place: Js<Place> = serde_json::from_str(&text).unwrap();
    let place: Js<Place<'static>> = place.into_owned();
    drop(text);

    let place = place.into_object().unwrap();
    assert_eq!(place.title.get().and_then(Json::as_str), Some("Café"));
    let location = place.location.object().unwrap();
    assert_eq!(location.lat.get().map(Json::to_value), Some(json!(50.08)));
    assert_eq!(place.reviews.items().len(), 2);
    let review = place.reviews.items()[0].object().unwrap();
    assert_eq!(review.text.get().and_then(Json::as_str), Some("ok"));
    assert!(!place.reviews.items()[1].truthy());
}

#[test]
fn reads_values_as_js_does() {
    let value = json!({ "title": null, "location": "unknown", "reviews": {} });
    let place = read::<Place>(&value).into_object().unwrap();
    assert_eq!(place.title.get(), Some(&Json::Null));
    assert!(place.location.object().is_none() && place.location.truthy());
    assert!(place.reviews.array().is_none());
    assert!(matches!(read_owned::<Place>(json!([1])), Js::Other(Value::Array(_))));
}

#[test]
fn builds_items_as_js_does() {
    let input = json!({ "name": "", "count": 0, "alias": "a" });
    let mut item = Obj::new();
    item.set("name", coalesce(input.get("name"), input.get("alias")))
        .put("hasCount", truthy(input.get("count")))
        .set("missing", None)
        .put("count", to_js_string(input.get("count")));
    assert_eq!(item.into_value().to_string(), r#"{"name":"","hasCount":false,"count":"0"}"#);
}
