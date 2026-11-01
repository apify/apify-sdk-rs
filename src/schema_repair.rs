//! Repairs of items a dataset's schema rejected, the rules of `pushDataWithSchemaRepair` of
//! `@apify/actor-utils`: a field that fails the schema is removed, a missing required field gets
//! an empty placeholder (`null` first, then `""`, `[]`, `{}` or `null` once the next rejection
//! tells its type), and a property the schema does not allow is removed. Nothing else is made up:
//! a field that must be a number or a boolean, or match a pattern or an enum, is not filled in,
//! and the item cannot be repaired then.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

/// How many fields a log line names at most.
const MAX_LOGGED_FIELDS: usize = 20;

/// An item being repaired over rounds of rejections, with what the earlier rounds taught.
pub(crate) struct RepairedItem {
    pub(crate) value: Value,
    /// The fields filled with a placeholder, as JSON pointers: a later type error on one of them
    /// picks the placeholder's type instead of removing the field.
    placeholders: HashSet<String>,
    /// The fields whose type has no placeholder, with the error that showed it: a later
    /// `required` error on one of them cannot be repaired.
    unfillable: HashMap<String, Value>,
}

impl RepairedItem {
    pub(crate) fn new(value: Value) -> Self {
        RepairedItem { value, placeholders: HashSet::new(), unfillable: HashMap::new() }
    }

    /// Repairs the item for the validation errors of a rejection. `Err` holds the errors that
    /// keep it from being repaired; the item is left as it was then.
    pub(crate) fn repair(&mut self, errors: &[Value]) -> Result<(), Vec<Value>> {
        let mut repaired = self.value.clone();
        let mut changed = false;
        let mut blocking = Vec::new();
        // One value per placeholder field, from all of its errors at once (see `plan_placeholders`).
        let plan = plan_placeholders(errors, &self.placeholders);
        let mut filled = HashSet::new();

        for error in sort_for_repair(errors) {
            let instance_path = text(error, "instancePath");
            let path = parse_json_pointer(instance_path);
            let keyword = text(error, "keyword");
            let param = |name: &str| error.get("params").and_then(|params| params.get(name)).and_then(Value::as_str);

            // A missing required field: `null` until the next rejection tells its type. The path
            // is the object that misses it.
            if keyword == "required"
                && let Some(missing) = param("missingProperty")
            {
                let target: Vec<String> = path.iter().cloned().chain([missing.to_owned()]).collect();
                let pointer = to_json_pointer(&target);
                if let Some(unfillable) = self.unfillable.get(&pointer) {
                    blocking.push(unfillable.clone());
                    continue;
                }
                if set_at_path(&mut repaired, &target, Value::Null) {
                    self.placeholders.insert(pointer);
                    changed = true;
                }
                continue;
            }
            // A property the schema does not allow, named in the error of its object.
            if keyword == "additionalProperties"
                && let Some(property) = param("additionalProperty")
            {
                let target: Vec<String> = path.iter().cloned().chain([property.to_owned()]).collect();
                changed |= delete_at_path(&mut repaired, &target);
                continue;
            }
            // The item itself has the wrong shape: there is no field to remove.
            if path.is_empty() {
                blocking.push(error.clone());
                continue;
            }
            // A placeholder of an earlier round: the value the errors ask for, if there is one.
            if self.placeholders.contains(instance_path) {
                let Some(value) = plan.get(instance_path) else {
                    blocking.push(error.clone());
                    continue;
                };
                if filled.insert(instance_path.to_owned()) {
                    changed |= set_at_path(&mut repaired, &path, value.clone());
                }
                continue;
            }
            // The item's own field fails the schema: removed. A required one comes back as a
            // placeholder in the next round, unless its type has none.
            changed |= delete_at_path(&mut repaired, &path);
            if keyword == "type" && placeholder_for(error).is_none() {
                self.unfillable.insert(instance_path.to_owned(), error.clone());
            }
        }

        if !blocking.is_empty() {
            return Err(blocking);
        }
        // Nothing to act on: the same item would be rejected the same way.
        if !changed {
            return Err(errors.to_vec());
        }
        self.value = repaired;
        Ok(())
    }
}

fn text<'a>(error: &'a Value, field: &str) -> &'a str {
    error.get(field).and_then(Value::as_str).unwrap_or_default()
}

/// The placeholder of each placeholder field, considering all of its errors together: a nullable
/// object reports `type: object`, `type: null` and `anyOf` at once, and only some of them suggest
/// a value. `null` wins, as it commits to no value at all; otherwise the first one.
fn plan_placeholders(errors: &[Value], placeholders: &HashSet<String>) -> HashMap<String, Value> {
    let mut plan: HashMap<String, Value> = HashMap::new();
    for error in errors {
        let instance_path = text(error, "instancePath");
        if instance_path.is_empty() || !placeholders.contains(instance_path) {
            continue;
        }
        if plan.get(instance_path).is_some_and(Value::is_null) {
            continue;
        }
        if let Some(value) = placeholder_for(error)
            && (value.is_null() || !plan.contains_key(instance_path))
        {
            plan.insert(instance_path.to_owned(), value);
        }
    }
    plan
}

/// An empty value of the type a `type` error asks for: `null` when the type allows it, else `""`,
/// `[]` or `{}`. Numbers and booleans have none: `0` and `false` would read as real data.
fn placeholder_for(error: &Value) -> Option<Value> {
    if text(error, "keyword") != "type" {
        return None;
    }
    let types: Vec<&str> = match error.get("params").and_then(|params| params.get("type")) {
        Some(Value::String(single)) => vec![single.as_str()],
        Some(Value::Array(types)) => types.iter().filter_map(Value::as_str).collect(),
        _ => return None,
    };
    if types.len() > 1 && types.contains(&"null") {
        return Some(Value::Null);
    }
    types.iter().find_map(|kind| match *kind {
        "null" => Some(Value::Null),
        "string" => Some(Value::String(String::new())),
        "array" => Some(Value::Array(Vec::new())),
        "object" => Some(Value::Object(serde_json::Map::new())),
        _ => None,
    })
}

/// The errors in an order in which repairs do not move each other's fields: of sibling array
/// elements the highest index first, and deeper fields before the ones that hold them.
fn sort_for_repair(errors: &[Value]) -> Vec<&Value> {
    let mut sorted: Vec<(Vec<String>, &Value)> =
        errors.iter().map(|error| (parse_json_pointer(text(error, "instancePath")), error)).collect();
    sorted.sort_by(|(a, _), (b, _)| {
        for (a, b) in a.iter().zip(b) {
            if a == b {
                continue;
            }
            return match (a.parse::<usize>(), b.parse::<usize>()) {
                (Ok(a), Ok(b)) => b.cmp(&a),
                _ => a.cmp(b),
            };
        }
        b.len().cmp(&a.len())
    });
    sorted.into_iter().map(|(_, error)| error).collect()
}

/// The segments of a JSON pointer (RFC 6901): `"/tags/0"` is `["tags", "0"]`.
fn parse_json_pointer(pointer: &str) -> Vec<String> {
    if pointer.is_empty() {
        return Vec::new();
    }
    pointer.split('/').skip(1).map(|segment| segment.replace("~1", "/").replace("~0", "~")).collect()
}

fn to_json_pointer(path: &[String]) -> String {
    path.iter().map(|segment| format!("/{}", escape_segment(segment))).collect()
}

fn escape_segment(segment: &str) -> String {
    segment.replace('~', "~0").replace('/', "~1")
}

/// The value at `path`, whose parents exist.
fn parent_of<'a>(value: &'a mut Value, path: &[String]) -> Option<&'a mut Value> {
    let mut cursor = value;
    for key in path {
        cursor = match cursor {
            Value::Object(object) => object.get_mut(key)?,
            Value::Array(array) => array.get_mut(key.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cursor)
}

fn delete_at_path(value: &mut Value, path: &[String]) -> bool {
    let Some((last, parents)) = path.split_last() else { return false };
    match parent_of(value, parents) {
        Some(Value::Object(object)) => object.shift_remove(last).is_some(),
        Some(Value::Array(array)) => match last.parse::<usize>() {
            Ok(index) if index < array.len() => {
                array.remove(index);
                true
            }
            _ => false,
        },
        _ => false,
    }
}

fn set_at_path(value: &mut Value, path: &[String], new: Value) -> bool {
    let Some((last, parents)) = path.split_last() else { return false };
    match parent_of(value, parents) {
        Some(Value::Object(object)) => {
            object.insert(last.clone(), new);
            true
        }
        Some(Value::Array(array)) => match last.parse::<usize>() {
            Ok(index) if index < array.len() => {
                array[index] = new;
                true
            }
            _ => false,
        },
        _ => false,
    }
}

/// Which field fails and how, for the log: `/tags/[] (type string)`. Array indices are collapsed,
/// as the same field fails on many elements and items.
pub(crate) fn field_issue(error: &Value) -> String {
    let param = |name: &str| error.get("params").and_then(|params| params.get(name)).and_then(Value::as_str);
    let mut path = text(error, "instancePath").to_owned();
    if let Some(child) = param("missingProperty").or_else(|| param("additionalProperty")) {
        path = format!("{path}/{}", escape_segment(child));
    }
    let path: Vec<&str> = path
        .split('/')
        .map(|segment| if !segment.is_empty() && segment.bytes().all(|b| b.is_ascii_digit()) { "[]" } else { segment })
        .collect();
    let path = path.join("/");
    let keyword = text(error, "keyword");
    let described = match error.get("params").and_then(|params| params.get("type")) {
        Some(Value::String(kind)) if keyword == "type" => format!("type {kind}"),
        Some(Value::Array(kinds)) if keyword == "type" => {
            format!("type {}", kinds.iter().filter_map(Value::as_str).collect::<Vec<_>>().join("|"))
        }
        _ => keyword.to_owned(),
    };
    format!("{} ({described})", if path.is_empty() { "(item root)" } else { &path })
}

/// A bounded, sorted list of field issues for one log line.
pub(crate) fn format_fields(fields: &std::collections::BTreeSet<String>) -> String {
    let shown: Vec<&str> = fields.iter().take(MAX_LOGGED_FIELDS).map(String::as_str).collect();
    match fields.len().saturating_sub(MAX_LOGGED_FIELDS) {
        0 => shown.join(", "),
        more => format!("{} (+{more} more)", shown.join(", ")),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn error(keyword: &str, instance_path: &str, params: Value) -> Value {
        json!({ "keyword": keyword, "instancePath": instance_path, "params": params, "message": "" })
    }

    #[test]
    fn a_missing_required_string_gets_a_placeholder_over_two_rounds() {
        let mut item = RepairedItem::new(json!({ "followers": 2 }));
        let required = error("required", "", json!({ "missingProperty": "url" }));
        assert!(item.repair(&[required]).is_ok());
        assert_eq!(item.value, json!({ "followers": 2, "url": null }));
        // The next rejection says which type it has to be.
        let type_error = error("type", "/url", json!({ "type": "string" }));
        assert!(item.repair(&[type_error]).is_ok());
        assert_eq!(item.value, json!({ "followers": 2, "url": "" }));
    }

    #[test]
    fn fields_that_fail_the_schema_and_unknown_properties_are_removed() {
        let mut item = RepairedItem::new(json!({ "url": "a", "followers": "many", "extra": 1, "tags": [1, "x", 2] }));
        let errors = [
            error("type", "/followers", json!({ "type": "integer" })),
            error("additionalProperties", "", json!({ "additionalProperty": "extra" })),
            error("type", "/tags/0", json!({ "type": "string" })),
            error("type", "/tags/2", json!({ "type": "string" })),
        ];
        assert!(item.repair(&errors).is_ok());
        assert_eq!(item.value, json!({ "url": "a", "tags": ["x"] }));
    }

    #[test]
    fn values_are_not_made_up_for_numbers_or_the_whole_item() {
        // A required number: it was removed for its type, so a placeholder cannot satisfy it.
        let mut item = RepairedItem::new(json!({ "url": "a", "followers": "many" }));
        assert!(item.repair(&[error("type", "/followers", json!({ "type": "integer" }))]).is_ok());
        let required = error("required", "", json!({ "missingProperty": "followers" }));
        assert_eq!(item.repair(&[required]).unwrap_err()[0]["keyword"], "type");

        // An error about the item itself.
        let mut item = RepairedItem::new(json!([1]));
        let root = error("type", "", json!({ "type": "object" }));
        assert_eq!(item.repair(std::slice::from_ref(&root)).unwrap_err(), [root]);

        // Nothing to act on: the same push would fail the same way.
        let mut item = RepairedItem::new(json!({ "url": "a" }));
        let pattern = error("pattern", "/missing", json!({ "pattern": "^x" }));
        assert!(item.repair(&[pattern]).is_err());
    }

    #[test]
    fn a_placeholder_prefers_null_when_the_type_allows_it() {
        let mut item = RepairedItem::new(json!({}));
        assert!(item.repair(&[error("required", "", json!({ "missingProperty": "bio" }))]).is_ok());
        let errors = [error("type", "/bio", json!({ "type": ["string", "null"] })), error("anyOf", "/bio", json!({}))];
        assert!(item.repair(&errors).is_ok());
        assert_eq!(item.value, json!({ "bio": null }));
    }

    #[test]
    fn field_labels_for_the_log() {
        let labels: Vec<String> = [
            error("required", "/owner", json!({ "missingProperty": "id" })),
            error("type", "/tags/3", json!({ "type": "string" })),
            error("additionalProperties", "", json!({ "additionalProperty": "a/b" })),
        ]
        .iter()
        .map(field_issue)
        .collect();
        assert_eq!(labels, ["/owner/id (required)", "/tags/[] (type string)", "/a~1b (additionalProperties)"]);
    }
}
