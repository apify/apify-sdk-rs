//! Numbers as JS reads and writes them: `String(number)`, `Number(text)`, `parseFloat`,
//! `parseInt`, `Number(x.toFixed(digits))`, `Math.round`, and JSON numbers from JS numbers.

use std::sync::LazyLock;

use regex::Regex;
use serde_json::{Value, json};

use super::{is_js_space, js_trim};

/// `String(number)`: the shortest text that reads back as the same double, as JS writes it.
pub fn js_number_to_string(number: f64) -> String {
    if number.is_nan() {
        return "NaN".to_owned();
    }
    if number.is_infinite() {
        return if number > 0.0 { "Infinity" } else { "-Infinity" }.to_owned();
    }
    if number == 0.0 {
        return "0".to_owned();
    }
    if (1e-6..1e21).contains(&number.abs()) {
        // Rust's `Display` is the shortest round-trip text without an exponent.
        return number.to_string();
    }
    // `1e+21`, `1.5e-7`.
    let text = format!("{number:e}");
    match text.split_once('e') {
        Some((mantissa, exponent)) if !exponent.starts_with('-') => format!("{mantissa}e+{exponent}"),
        _ => text,
    }
}

/// A JSON number from a JS number: integers without a fraction (`30`, not `30.0`), and `NaN` and
/// the infinities as `null` (as `JSON.stringify` writes them).
pub fn js_number(number: f64) -> Value {
    if number.fract() == 0.0 && number.abs() < 9_007_199_254_740_992.0 {
        // `-0` is `0` in JSON.
        return json!(number as i64);
    }
    serde_json::Number::from_f64(number).map_or(Value::Null, Value::Number)
}

/// `Number(text)` of a string: the whole (trimmed) text as a number, `0` for blank text, and
/// `NaN` otherwise.
pub fn js_string_to_number(text: &str) -> f64 {
    static DECIMAL: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$").expect("a valid regex")
    });
    let text = js_trim(text);
    if text.is_empty() {
        return 0.0;
    }
    for (prefix, radix) in [("0x", 16), ("0X", 16), ("0o", 8), ("0O", 8), ("0b", 2), ("0B", 2)] {
        if let Some(digits) = text.strip_prefix(prefix) {
            if digits.is_empty() || !digits.chars().all(|c| c.is_digit(radix)) {
                return f64::NAN;
            }
            return digits
                .chars()
                .fold(0.0, |value, digit| value * f64::from(radix) + f64::from(digit.to_digit(radix).unwrap_or(0)));
        }
    }
    let unsigned = text.strip_prefix(['+', '-']).unwrap_or(text);
    if unsigned == "Infinity" {
        return if text.starts_with('-') { f64::NEG_INFINITY } else { f64::INFINITY };
    }
    if DECIMAL.is_match(text) { text.parse().unwrap_or(f64::NAN) } else { f64::NAN }
}

/// `parseFloat(text)`: the longest decimal prefix of the text, after leading white space.
pub fn js_parse_float(text: &str) -> f64 {
    static DECIMAL_PREFIX: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?").expect("a valid regex"));
    let text = text.trim_start_matches(is_js_space);
    let unsigned = text.strip_prefix(['+', '-']).unwrap_or(text);
    if unsigned.starts_with("Infinity") {
        return if text.starts_with('-') { f64::NEG_INFINITY } else { f64::INFINITY };
    }
    DECIMAL_PREFIX.find(text).and_then(|prefix| prefix.as_str().parse().ok()).unwrap_or(f64::NAN)
}

/// `parseInt(text, 10)`: the leading integer of the text, after leading white space.
pub fn js_parse_int(text: &str) -> f64 {
    let text = text.trim_start_matches(is_js_space);
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let digits: String = unsigned.chars().take_while(char::is_ascii_digit).collect();
    if digits.is_empty() {
        return f64::NAN;
    }
    let value: f64 = digits.parse().unwrap_or(f64::NAN);
    if negative { -value } else { value }
}

/// `Number(x.toFixed(digits))` of JavaScript: rounds the exact value of `x`, halves away from zero.
pub fn js_to_fixed(x: f64, digits: usize) -> f64 {
    if !x.is_finite() || x.abs() >= 1e21 {
        return x;
    }
    // The exact decimal expansion of an f64 has at most 1074 fractional digits.
    let exact = format!("{:.1074}", x.abs());
    let (int_part, frac) = exact.split_once('.').expect("a fractional part");
    let mut digits_str: Vec<u8> = int_part.bytes().chain(frac.bytes().take(digits)).collect();
    if frac.as_bytes()[digits] >= b'5' {
        // Increment the decimal number in `digits_str`.
        let mut i = digits_str.len();
        loop {
            if i == 0 {
                digits_str.insert(0, b'1');
                break;
            }
            i -= 1;
            if digits_str[i] == b'9' {
                digits_str[i] = b'0';
            } else {
                digits_str[i] += 1;
                break;
            }
        }
    }
    let split = digits_str.len() - digits;
    let text = format!(
        "{}{}.{}",
        if x < 0.0 { "-" } else { "" },
        std::str::from_utf8(&digits_str[..split]).expect("digits"),
        std::str::from_utf8(&digits_str[split..]).expect("digits")
    );
    text.parse().expect("a number")
}

/// `Math.round(x)`: the nearest integer, a half rounded up toward +∞ (`-2.5` to `-2`), unlike
/// `f64::round`, which rounds halves away from zero. `-0` for `-0.5 <= x < 0`, as in JS.
pub fn math_round(x: f64) -> f64 {
    if !x.is_finite() || x == 0.0 {
        return x;
    }
    if (-0.5..0.0).contains(&x) {
        return -0.0;
    }
    let floor = x.floor();
    // Exact: `x - floor` is the fraction of `x`. `(x + 0.5).floor()` would round
    // 0.49999999999999994 up.
    if x - floor >= 0.5 { floor + 1.0 } else { floor }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn js_numbers() {
        assert_eq!(js_string_to_number(" 0x1A "), 26.0);
        assert_eq!(js_string_to_number("1e3"), 1000.0);
        assert!(js_string_to_number("12a").is_nan());
        assert!(js_string_to_number("inf").is_nan());
        assert_eq!(js_number_to_string(1e21), "1e+21");
        assert_eq!(js_number_to_string(1.5e-7), "1.5e-7");
        assert_eq!(js_number_to_string(3283172519183047513.0), "3283172519183047700");
    }

    #[test]
    fn string_of_number() {
        assert_eq!(js_number_to_string(5.0), "5"); // String(5)
        assert_eq!(js_number_to_string(-5.0), "-5"); // String(-5)
        assert_eq!(js_number_to_string(-0.0), "0"); // String(-0)
        assert_eq!(js_number_to_string(0.1 + 0.2), "0.30000000000000004"); // String(0.1 + 0.2)
        assert_eq!(js_number_to_string(1.0 / 3.0), "0.3333333333333333"); // String(1 / 3)
        assert_eq!(js_number_to_string(123.456), "123.456"); // String(123.456)
        assert_eq!(js_number_to_string(1e20), "100000000000000000000"); // String(1e20)
        assert_eq!(js_number_to_string(1e21), "1e+21"); // String(1e21)
        assert_eq!(js_number_to_string(-1.5e300), "-1.5e+300"); // String(-1.5e300)
        assert_eq!(js_number_to_string(1e-6), "0.000001"); // String(1e-6)
        assert_eq!(js_number_to_string(1.5e-6), "0.0000015"); // String(1.5e-6)
        assert_eq!(js_number_to_string(1e-7), "1e-7"); // String(1e-7)
        assert_eq!(js_number_to_string(-1.25e-10), "-1.25e-10"); // String(-1.25e-10)
        assert_eq!(js_number_to_string(5e-324), "5e-324"); // String(Number.MIN_VALUE)
        assert_eq!(js_number_to_string(f64::MAX), "1.7976931348623157e+308"); // String(Number.MAX_VALUE)
        assert_eq!(js_number_to_string(2f64.powi(53) + 1.0), "9007199254740992"); // String(2 ** 53 + 1)
        assert_eq!(js_number_to_string(f64::NAN), "NaN"); // String(NaN)
        assert_eq!(js_number_to_string(f64::NEG_INFINITY), "-Infinity"); // String(-Infinity)
    }

    #[test]
    fn json_numbers() {
        assert_eq!(js_number(30.0).to_string(), "30"); // JSON.stringify(30)
        assert_eq!(js_number(-0.0).to_string(), "0"); // JSON.stringify(-0)
        assert_eq!(js_number(1.5).to_string(), "1.5"); // JSON.stringify(1.5)
        assert_eq!(js_number(f64::NAN), Value::Null); // JSON.stringify(NaN)
        assert_eq!(js_number(f64::INFINITY), Value::Null); // JSON.stringify(Infinity)
    }

    #[test]
    fn number_of_string() {
        assert_eq!(js_string_to_number(""), 0.0); // Number("")
        assert_eq!(js_string_to_number(" \n\t"), 0.0); // Number(" \n\t")
        assert_eq!(js_string_to_number(" 12 "), 12.0); // Number(" 12 ")
        assert_eq!(js_string_to_number("-1.5e3"), -1500.0); // Number("-1.5e3")
        assert_eq!(js_string_to_number(".5"), 0.5); // Number(".5")
        assert_eq!(js_string_to_number("5."), 5.0); // Number("5.")
        assert_eq!(js_string_to_number("0b101"), 5.0); // Number("0b101")
        assert_eq!(js_string_to_number("0o17"), 15.0); // Number("0o17")
        assert_eq!(js_string_to_number("-Infinity"), f64::NEG_INFINITY); // Number("-Infinity")
        assert!(js_string_to_number("-0x10").is_nan()); // Number("-0x10")
        assert!(js_string_to_number("0x").is_nan()); // Number("0x")
        assert!(js_string_to_number("1_000").is_nan()); // Number("1_000")
        assert!(js_string_to_number("infinity").is_nan()); // Number("infinity")
        assert!(js_string_to_number(".").is_nan()); // Number(".")
    }

    #[test]
    fn parse_float_and_int() {
        assert_eq!(js_parse_float("  2.75abc"), 2.75); // parseFloat("  2.75abc")
        assert_eq!(js_parse_float("1e"), 1.0); // parseFloat("1e")
        assert_eq!(js_parse_float("-.5x"), -0.5); // parseFloat("-.5x")
        assert_eq!(js_parse_float("1.5e3px"), 1500.0); // parseFloat("1.5e3px")
        assert_eq!(js_parse_float("Infinityx"), f64::INFINITY); // parseFloat("Infinityx")
        assert!(js_parse_float("0x10") == 0.0); // parseFloat("0x10")
        assert!(js_parse_float("x1").is_nan()); // parseFloat("x1")
        assert!(js_parse_float(".").is_nan()); // parseFloat(".")
        assert_eq!(js_parse_int("  -12.9px"), -12.0); // parseInt("  -12.9px", 10)
        assert_eq!(js_parse_int("+7"), 7.0); // parseInt("+7", 10)
        assert_eq!(js_parse_int("0x10"), 0.0); // parseInt("0x10", 10)
        assert!(js_parse_int("-0").is_sign_negative()); // Object.is(parseInt("-0", 10), -0)
        assert!(js_parse_int("px").is_nan()); // parseInt("px", 10)
        assert!(js_parse_int("- 1").is_nan()); // parseInt("- 1", 10)
    }

    #[test]
    fn to_fixed_rounds_like_javascript() {
        // Values checked with node: Number(x.toFixed(n)).
        assert_eq!(js_to_fixed(0.25, 1), 0.3, "a tie rounds up, not to even");
        assert_eq!(js_to_fixed(1.005, 2), 1.0, "1.005 is below 1.005 in binary");
        assert_eq!(js_to_fixed(4.99999999999, 4), 5.0);
        assert_eq!(js_to_fixed(-2.5, 0), -3.0);
        assert_eq!(js_to_fixed(0.1 + 0.2, 6), 0.3);
        assert_eq!(js_to_fixed(9.9999995, 6), 9.999999);
        assert_eq!(js_to_fixed(f64::INFINITY, 4), f64::INFINITY);
        assert_eq!(js_to_fixed(1.45, 1), 1.4); // Number((1.45).toFixed(1)): 1.45 is below 1.45 in binary
        assert_eq!(js_to_fixed(1.5, 0), 2.0); // Number((1.5).toFixed(0))
        assert_eq!(js_to_fixed(123.456, 2), 123.46); // Number((123.456).toFixed(2))
    }

    #[test]
    fn to_fixed_golden() {
        let cases: Vec<Value> = serde_json::from_str(include_str!("../../conformance/golden/to_fixed.json")).unwrap();
        for case in cases {
            let (x, digits) = (case["x"].as_f64().unwrap(), case["digits"].as_u64().unwrap() as usize);
            assert_eq!(js_to_fixed(x, digits), case["expected"].as_f64().unwrap(), "{x}.toFixed({digits})");
        }
    }

    #[test]
    fn rounds_as_math_round() {
        assert_eq!(math_round(2.5), 3.0); // Math.round(2.5)
        assert_eq!(math_round(-2.5), -2.0); // Math.round(-2.5)
        assert_eq!(math_round(-2.6), -3.0); // Math.round(-2.6)
        assert_eq!(math_round(2.4), 2.0); // Math.round(2.4)
        assert_eq!(math_round(0.49999999999999994), 0.0); // Math.round(0.49999999999999994)
        assert_eq!(math_round(4503599627370495.5), 4503599627370496.0); // Math.round(2 ** 52 - 0.5)
        assert_eq!(math_round(9007199254740991.0), 9007199254740991.0); // Math.round(2 ** 53 - 1)
        assert_eq!(math_round(1e300), 1e300); // Math.round(1e300)
        assert!(math_round(-0.5).is_sign_negative() && math_round(-0.5) == 0.0); // Object.is(Math.round(-0.5), -0)
        assert!(math_round(-0.2).is_sign_negative()); // Object.is(Math.round(-0.2), -0)
        assert!(math_round(0.2).is_sign_positive()); // Object.is(Math.round(0.2), 0)
        assert!(math_round(-0.0).is_sign_negative()); // Object.is(Math.round(-0), -0)
        assert!(math_round(f64::NAN).is_nan()); // Math.round(NaN)
        assert_eq!(math_round(f64::NEG_INFINITY), f64::NEG_INFINITY); // Math.round(-Infinity)
        // The rounding JS code does on percentages and coordinates.
        assert_eq!(math_round(0.125 * 1000.0) / 10.0, 12.5); // Math.round(0.125 * 1000) / 10
        assert_eq!(math_round(1.005 * 100.0) / 100.0, 1.0); // Math.round(1.005 * 100) / 100
    }
}
