# Allowed differences from the JS SDK

Behavior that intentionally differs from the JS SDK (`apify` on npm). Everything else should match.

## Configuration

- **"At home" has one source.** The JS SDK reads `APIFY_IS_AT_HOME` both through `Configuration`
  (coerced like a boolean, so `0` and `false` are false) and directly with `!!process.env[...]` (so
  any non-empty value is true). Here it is always `Configuration::is_at_home`.
- **Invalid numbers are ignored.** A value such as `APIFY_PROXY_PORT=abc` falls back to the default
  with a warning. The JS SDK throws when the configuration is constructed.

## Storages

- **Listed keys have an empty content type.** The API does not report content types when it lists
  the keys of a key-value store. The JS SDK leaves the field undefined; crawlee-rs requires a
  string, so it is empty.

## Actor lifecycle

- **The final status message is awaited.** The JS SDK gives the request that sets the terminal
  status message 1 ms before exiting. Here it gets up to 1 s (the status message timeout), since
  a process exit in Rust cuts off requests in flight.
- **`fail` cannot exit with code 0.** `ExitOptions::exit_code` has no "not set" value, so `fail`
  takes 0 (the default) as not set and exits with 1; the JS SDK's `fail({ exitCode: 0 })` exits
  with 0.
- **The Actor's functions panic before `Actor::init()`.** Calling them first is a bug in the code;
  the JS SDK warns and continues with an uninitialized Actor. `Actor::on` and `Actor::off` are the
  exception: as in JS, listeners added before `init` are queued and attached by it.
- **`get_env` falls back to the `APIFY_*` variables.** The JS SDK reads both generations of
  variables into one object, and a missing `ACTOR_*` variable overwrites its `APIFY_*` predecessor
  with `null`. Here the `ACTOR_*` variable is read first and the `APIFY_*` one is the fallback.
- **`use_state` uses the Actor's services.** The JS SDK opens the store with the global
  configuration, whatever the Actor was configured with.
- **Shared request queues can prolong locks.** `extend_request_processing_time` prolongs the
  platform lock of a request in progress; the JS SDK never prolongs locks.

- **A redirected log chunk without a newline waits for the rest of its line.** The JS
  `StreamedLog` logs such a chunk without its last character, which looks like a bug; here the
  line is logged once it is complete (or when the stream ends).

## Opt-in extras

- **Buffered dataset writes (off by default).** With `CRAWLEE_DATASET_BUFFER=1` (an extra of
  crawlee-rs), pushes to the default dataset are collected and stored together, and a crawler
  marks a request as handled only once its items are stored. Pay-per-event charges are still
  reserved when items are pushed, so the budget stops a run exactly as in JS, but the charge calls
  are sent once per flush with the counts summed by event, and the local charging log has one
  entry per event per flush. Without the variable, every push is stored and charged at once, as
  in the JS SDK.

## Input

- **Encrypted PKCS#8 keys are not supported for input secrets.** The platform's key is a legacy
  encrypted PKCS#1 PEM (DES-EDE3-CBC), which is supported, as are AES-CBC encrypted and
  unencrypted keys. Node.js also reads `BEGIN ENCRYPTED PRIVATE KEY` PEMs.
- **Pseudo-URLs escape characters above U+00FF correctly.** `purlToRegExp` writes them as
  `\xHHHH`, which a JavaScript regex reads as `\xHH` followed by two literal characters, so such
  pseudo-URLs never match. Here they match the character.
- **Pseudo-URL `[regex]` sections use the syntax of the `regex` crate.** Lookarounds and
  backreferences of JavaScript regexes are not supported.
