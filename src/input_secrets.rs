//! Secret input fields: the platform encrypts the fields an input schema marks `isSecret`, and
//! the run decrypts them with the private key the platform passes in
//! `APIFY_INPUT_SECRETS_PRIVATE_KEY_FILE` (a base64-encoded PEM) and `..._PASSPHRASE`. A port of
//! `decryptInputSecrets` of `@apify/input_secrets`.
//!
//! An encrypted value is `ENCRYPTED_VALUE:<password>:<value>` (a string) or
//! `ENCRYPTED_JSON:[<schema hash>:]<password>:<value>` (JSON). The password is an AES-256-GCM key
//! and a 16-byte IV, encrypted with RSA-OAEP (SHA-1); the value is encrypted with them, its 16-byte
//! authentication tag appended.
//!
//! RSA is done by aws-lc-rs (constant time); the RustCrypto `rsa` crate has a timing side channel
//! (RUSTSEC-2023-0071).

use aes_gcm::aead::consts::U16;
use aes_gcm::aead::{Aead, KeyInit};
use aws_lc_rs::rsa::{OAEP_SHA1_MGF1SHA1, OaepPrivateDecryptingKey, PrivateDecryptingKey};
use base64::Engine as _;
use cbc::cipher::block_padding::Pkcs7;
use cbc::cipher::{BlockDecryptMut, KeyIvInit};
use md5::{Digest, Md5};
use serde_json::{Map, Value};

const ENCRYPTED_STRING_VALUE_PREFIX: &str = "ENCRYPTED_VALUE";
const ENCRYPTED_JSON_VALUE_PREFIX: &str = "ENCRYPTED_JSON";
const ENCRYPTION_KEY_LENGTH: usize = 32;
const ENCRYPTION_IV_LENGTH: usize = 16;
const AUTH_TAG_LENGTH: usize = 16;

type Aes256Gcm16 = aes_gcm::AesGcm<aes::Aes256, U16>;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct SecretsError(String);

fn error(message: impl Into<String>) -> SecretsError {
    SecretsError(message.into())
}

/// The private key the input secrets are encrypted for.
pub struct InputSecretsKey(OaepPrivateDecryptingKey);

impl std::fmt::Debug for InputSecretsKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InputSecretsKey")
    }
}

/// A PEM block: its label, its headers and its body.
struct Pem {
    label: String,
    headers: Vec<(String, String)>,
    der: Vec<u8>,
}

fn parse_pem(pem: &str) -> Result<Pem, SecretsError> {
    let mut lines = pem.lines().map(str::trim).filter(|line| !line.is_empty());
    let begin = lines.next().ok_or_else(|| error("the private key is empty"))?;
    let label = begin
        .strip_prefix("-----BEGIN ")
        .and_then(|rest| rest.strip_suffix("-----"))
        .ok_or_else(|| error("the private key is not a PEM"))?
        .to_owned();
    let mut headers = Vec::new();
    let mut body = String::new();
    for line in lines {
        if line.starts_with("-----END ") {
            let der = base64::engine::general_purpose::STANDARD
                .decode(body)
                .map_err(|err| error(format!("the private key PEM is not base64: {err}")))?;
            return Ok(Pem { label, headers, der });
        }
        match line.split_once(':') {
            Some((name, value)) if body.is_empty() => headers.push((name.trim().to_owned(), value.trim().to_owned())),
            _ => body.push_str(line),
        }
    }
    Err(error("the private key PEM has no end"))
}

/// OpenSSL's `EVP_BytesToKey` with MD5 and one iteration: the key of a legacy encrypted PEM.
fn evp_bytes_to_key(passphrase: &[u8], salt: &[u8], length: usize) -> Vec<u8> {
    let mut key = Vec::with_capacity(length + 16);
    let mut previous: Vec<u8> = Vec::new();
    while key.len() < length {
        let mut hasher = Md5::new();
        hasher.update(&previous);
        hasher.update(passphrase);
        hasher.update(salt);
        previous = hasher.finalize().to_vec();
        key.extend_from_slice(&previous);
    }
    key.truncate(length);
    key
}

/// Decrypts the body of a legacy encrypted PEM (`Proc-Type: 4,ENCRYPTED`).
fn decrypt_legacy_pem(dek_info: &str, passphrase: &[u8], der: &[u8]) -> Result<Vec<u8>, SecretsError> {
    let (cipher, iv_hex) = dek_info.split_once(',').ok_or_else(|| error("invalid DEK-Info"))?;
    let iv = (0..iv_hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(iv_hex.get(i..i + 2).unwrap_or("x"), 16))
        .collect::<Result<Vec<u8>, _>>()
        .map_err(|_| error("invalid DEK-Info IV"))?;
    let salt = iv.get(..8).ok_or_else(|| error("invalid DEK-Info IV"))?;
    let wrong = |_| error("decrypting the private key failed (wrong passphrase?)");
    let mut buffer = der.to_vec();
    let plain = match cipher {
        "DES-EDE3-CBC" => {
            let key = evp_bytes_to_key(passphrase, salt, 24);
            cbc::Decryptor::<des::TdesEde3>::new_from_slices(&key, &iv)
                .map_err(|_| error("invalid DES-EDE3-CBC IV"))?
                .decrypt_padded_mut::<Pkcs7>(&mut buffer)
                .map_err(wrong)?
                .to_vec()
        }
        "AES-128-CBC" | "AES-192-CBC" | "AES-256-CBC" => {
            let bits: usize = cipher[4..7].parse().expect("a key size");
            let key = evp_bytes_to_key(passphrase, salt, bits / 8);
            let decrypted = match bits {
                128 => cbc::Decryptor::<aes::Aes128>::new_from_slices(&key, &iv)
                    .map_err(|_| error("invalid AES IV"))?
                    .decrypt_padded_mut::<Pkcs7>(&mut buffer),
                192 => cbc::Decryptor::<aes::Aes192>::new_from_slices(&key, &iv)
                    .map_err(|_| error("invalid AES IV"))?
                    .decrypt_padded_mut::<Pkcs7>(&mut buffer),
                _ => cbc::Decryptor::<aes::Aes256>::new_from_slices(&key, &iv)
                    .map_err(|_| error("invalid AES IV"))?
                    .decrypt_padded_mut::<Pkcs7>(&mut buffer),
            };
            decrypted.map_err(wrong)?.to_vec()
        }
        other => return Err(error(format!("unsupported private key encryption {other}"))),
    };
    Ok(plain)
}

fn der_length(length: usize) -> Vec<u8> {
    if length < 0x80 {
        return vec![length as u8];
    }
    let bytes: Vec<u8> = length.to_be_bytes().into_iter().skip_while(|&byte| byte == 0).collect();
    let mut encoded = vec![0x80 | bytes.len() as u8];
    encoded.extend(bytes);
    encoded
}

/// Wraps a PKCS#1 `RSAPrivateKey` into a PKCS#8 `PrivateKeyInfo`.
fn pkcs1_to_pkcs8(pkcs1: &[u8]) -> Vec<u8> {
    // version 0, AlgorithmIdentifier { rsaEncryption, NULL }
    const HEADER: [u8; 18] =
        [0x02, 0x01, 0x00, 0x30, 0x0d, 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01, 0x05, 0x00];
    let mut octets = vec![0x04];
    octets.extend(der_length(pkcs1.len()));
    octets.extend_from_slice(pkcs1);
    let mut content = HEADER.to_vec();
    content.extend(octets);
    let mut der = vec![0x30];
    der.extend(der_length(content.len()));
    der.extend(content);
    der
}

impl InputSecretsKey {
    /// The key from the values of `APIFY_INPUT_SECRETS_PRIVATE_KEY_FILE` (base64 of a PEM) and
    /// `APIFY_INPUT_SECRETS_PRIVATE_KEY_PASSPHRASE`. Supported: RSA keys in PKCS#1 (legacy
    /// encrypted with DES-EDE3-CBC or AES-CBC, or not encrypted) and unencrypted PKCS#8.
    pub fn from_base64_pem(key_file: &str, passphrase: &str) -> Result<Self, SecretsError> {
        let pem = base64::engine::general_purpose::STANDARD
            .decode(key_file.trim())
            .map_err(|err| error(format!("the private key is not base64: {err}")))?;
        let pem = String::from_utf8(pem).map_err(|_| error("the private key PEM is not text"))?;
        let Pem { label, headers, der } = parse_pem(&pem)?;
        let header = |name: &str| headers.iter().find(|(key, _)| key == name).map(|(_, value)| value.as_str());
        let pkcs8 = match label.as_str() {
            "RSA PRIVATE KEY" => {
                let pkcs1 = match header("Proc-Type") {
                    Some(proc_type) if proc_type.contains("ENCRYPTED") => {
                        let dek_info = header("DEK-Info").ok_or_else(|| error("the encrypted PEM has no DEK-Info"))?;
                        decrypt_legacy_pem(dek_info, passphrase.as_bytes(), &der)?
                    }
                    _ => der,
                };
                pkcs1_to_pkcs8(&pkcs1)
            }
            "PRIVATE KEY" => der,
            other => return Err(error(format!("unsupported private key type {other}"))),
        };
        let key = PrivateDecryptingKey::from_pkcs8(&pkcs8)
            .map_err(|err| error(format!("invalid private key (wrong passphrase?): {err}")))?;
        let key = OaepPrivateDecryptingKey::new(key).map_err(|_| error("unsupported private key"))?;
        Ok(InputSecretsKey(key))
    }

    /// `privateDecrypt` of `@apify/utilities`.
    fn decrypt(&self, encrypted_password: &str, encrypted_value: &str) -> Result<String, SecretsError> {
        let engine = base64::engine::general_purpose::STANDARD;
        let encrypted_password = engine.decode(encrypted_password).map_err(|err| error(err.to_string()))?;
        let encrypted_value = engine.decode(encrypted_value).map_err(|err| error(err.to_string()))?;
        let mut password = vec![0; self.0.min_output_size()];
        let password = self
            .0
            .decrypt(&OAEP_SHA1_MGF1SHA1, &encrypted_password, &mut password, None)
            .map_err(|_| error("decrypting the password failed"))?;
        if password.len() != ENCRYPTION_KEY_LENGTH + ENCRYPTION_IV_LENGTH {
            return Err(error("privateDecrypt: Decryption failed, invalid password length!"));
        }
        if encrypted_value.len() < AUTH_TAG_LENGTH {
            return Err(error("the encrypted value is too short"));
        }
        let (key, iv) = password.split_at(ENCRYPTION_KEY_LENGTH);
        let cipher = Aes256Gcm16::new_from_slice(key).map_err(|_| error("invalid key"))?;
        let plain = cipher
            .decrypt(aes_gcm::Nonce::<U16>::from_slice(iv), encrypted_value.as_slice())
            .map_err(|_| error("decrypting the value failed"))?;
        String::from_utf8(plain).map_err(|_| error("the decrypted value is not UTF-8"))
    }
}

/// `prefix`, `password`, `value` of an encrypted value (the regex of `@apify/input_secrets`).
fn parse_encrypted(value: &str) -> Option<(&str, &str, &str)> {
    let is_base64 = |part: &str| {
        let trimmed = part.trim_end_matches('=');
        part.len() - trimmed.len() <= 3
            && trimmed.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'+' | b'/'))
    };
    let parts: Vec<&str> = value.split(':').collect();
    let (prefix, password, encrypted) = match parts.as_slice() {
        [prefix, password, value] => (*prefix, *password, *value),
        [prefix, hash, password, value] if is_base64(hash) => (*prefix, *password, *value),
        _ => return None,
    };
    let known = prefix == ENCRYPTED_STRING_VALUE_PREFIX || prefix == ENCRYPTED_JSON_VALUE_PREFIX;
    (known && is_base64(password) && is_base64(encrypted)).then_some((prefix, password, encrypted))
}

/// Decrypts the encrypted string fields of `input`; the others stay as they are.
pub fn decrypt_input_secrets(
    input: Map<String, Value>,
    key: &InputSecretsKey,
) -> Result<Map<String, Value>, SecretsError> {
    let mut decrypted = input;
    for (field, value) in decrypted.iter_mut() {
        let Value::String(text) = value else { continue };
        let Some((prefix, password, encrypted)) = parse_encrypted(text) else { continue };
        let fail = |err: SecretsError| {
            error(format!(
                "The input field \"{field}\" could not be decrypted. Try updating the field's value in the input \
                 editor. Decryption error: {err}"
            ))
        };
        let plain = key.decrypt(password, encrypted).map_err(fail)?;
        *value = if prefix == ENCRYPTED_JSON_VALUE_PREFIX {
            serde_json::from_str(&plain).map_err(|err| fail(error(err.to_string())))?
        } else {
            Value::String(plain)
        };
    }
    Ok(decrypted)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encrypted by `@apify/input_secrets` (see `tests/fixtures/input_secrets.json`).
    fn fixture() -> Value {
        serde_json::from_str(include_str!("../tests/fixtures/input_secrets.json")).unwrap()
    }

    #[test]
    fn decrypts_values_encrypted_by_the_js_library() {
        let fixture = fixture();
        let key = InputSecretsKey::from_base64_pem(
            fixture["privateKeyFile"].as_str().unwrap(),
            fixture["passphrase"].as_str().unwrap(),
        )
        .unwrap();
        let encrypted = fixture["encrypted"].as_object().unwrap().clone();
        let decrypted = decrypt_input_secrets(encrypted, &key).unwrap();
        assert_eq!(Value::Object(decrypted), fixture["decrypted"]);
    }

    #[test]
    fn a_wrong_passphrase_or_a_tampered_value_fails() {
        let fixture = fixture();
        let file = fixture["privateKeyFile"].as_str().unwrap();
        assert!(InputSecretsKey::from_base64_pem(file, "wrong-passphrase").is_err());

        let key = InputSecretsKey::from_base64_pem(file, "pwd1234").unwrap();
        let mut encrypted = fixture["encrypted"].as_object().unwrap().clone();
        let secret = encrypted["secret"].as_str().unwrap();
        // Flip the last base64 character of the value (inside the authentication tag).
        let last = if secret.ends_with('A') { 'B' } else { 'A' };
        let tampered = format!("{}{last}", &secret[..secret.len() - 1]);
        encrypted.insert("secret".to_owned(), Value::String(tampered));
        let err = decrypt_input_secrets(encrypted, &key).unwrap_err();
        assert!(err.to_string().starts_with("The input field \"secret\" could not be decrypted."), "{err}");
    }

    /// The key of the fixture in other PEM formats (see `tests/fixtures/input_secrets_key_formats.json`).
    fn key_formats() -> Value {
        serde_json::from_str(include_str!("../tests/fixtures/input_secrets_key_formats.json")).unwrap()
    }

    #[test]
    fn the_same_key_decrypts_the_values_in_every_supported_format() {
        let fixture = fixture();
        let formats = key_formats();
        for format in ["pkcs1", "pkcs1Aes256Cbc", "pkcs1Aes128Cbc", "pkcs8"] {
            let key = InputSecretsKey::from_base64_pem(formats[format].as_str().unwrap(), "pwd1234")
                .unwrap_or_else(|err| panic!("{format}: {err}"));
            let encrypted = fixture["encrypted"].as_object().unwrap().clone();
            let decrypted = decrypt_input_secrets(encrypted, &key).unwrap();
            assert_eq!(Value::Object(decrypted), fixture["decrypted"], "{format}");
        }
    }

    #[test]
    fn an_aes_encrypted_key_with_a_wrong_passphrase_fails() {
        let formats = key_formats();
        for format in ["pkcs1Aes256Cbc", "pkcs1Aes128Cbc"] {
            assert!(InputSecretsKey::from_base64_pem(formats[format].as_str().unwrap(), "wrong").is_err(), "{format}");
        }
    }

    /// An encrypted PKCS#8 key (`ENCRYPTED PRIVATE KEY`) is refused with its type, where Node's
    /// `createPrivateKey` would decrypt it. The platform sends legacy encrypted PKCS#1 keys.
    #[test]
    fn an_encrypted_pkcs8_key_is_refused_with_its_type() {
        let file = key_formats()["encryptedPkcs8"].as_str().unwrap().to_owned();
        let err = InputSecretsKey::from_base64_pem(&file, "pwd1234").unwrap_err();
        assert_eq!(err.to_string(), "unsupported private key type ENCRYPTED PRIVATE KEY");
    }

    #[test]
    fn only_encrypted_values_are_touched() {
        assert!(parse_encrypted("ENCRYPTED_VALUE:abc=:def").is_some());
        assert!(parse_encrypted("ENCRYPTED_JSON:5be0c04285:abc:def").is_some());
        assert!(parse_encrypted("ENCRYPTED_VALUE:a b:def").is_none());
        assert!(parse_encrypted("NOT_ENCRYPTED:abc:def").is_none());
        assert!(parse_encrypted("hello").is_none());
    }
}
