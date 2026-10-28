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
- **A single-consumer request queue lists its requests only until it knows all of them.** The
  platform bills one read per listed request. The JS SDK lists the queue head (at most 1,000
  requests, the ones in progress included) whenever its local head is nearly empty, twice per
  `isFinished`, which the crawler calls after every request and every 500 ms. So a run with 1,000
  requests in progress paid up to 2,000 reads per check, and once 1,000 were in progress the
  listing showed none of the others: a resurrected run could not run more than about 1,000
  requests at once.
  Here the queue knows every unhandled request from its first add to a queue without unhandled
  requests (the prefetch that deduplicates added requests shows that). Otherwise (a resurrected
  run, a queue filled before the run), it lists all pending requests once with the request
  listing's `pending` filter, in the queue's order and with their data (so no request is read
  again): 500 per page, the next page in the background while the current one is processed. After that it lists the head only
  when it has nothing left (no request in its local head or in progress), in `is_finished`,
  which also returns `false` without a listing while anything is left. The queue is the run's
  own: requests another client adds after that are fetched only once this client has run out of
  its own, at that last listing, not as soon as the platform head shows them.

## Actor lifecycle

- **`init` makes its API calls at the same time.** The JS SDK connects to the platform events
  websocket, then loads the tracked child runs, then reads the run for its pricing, one after
  another. Here the three are made at once, and an error of the websocket is still reported first.
- **The default storages and some records are read ahead during `init`.** On the platform,
  `init` starts reading the metadata of the default dataset and key-value store (which opening
  them reads), the input record, the tracked child runs and the records of
  `InitOptions::prefetch_records` in the background, and the first read of each takes what was
  read. What is written through the SDK before it is read (a record, items of the dataset) is
  read again; what another client changes in the meantime is not. The JS SDK reads each when
  asked.
- **Status messages can be set in the background.** `Actor::set_status_message_in_background` is
  `Actor.setStatusMessage(...)` without `await`: the message is logged at once and the run updated
  later. Updates are sent one at a time in the order they were made, and an update is dropped when
  a later one was sent already (in JS, concurrent updates can land in any order); `exit` waits for
  them before the terminal status message.
- **The API clients share one HTTP client.** Every API client of the SDK (the Actor's, the
  storages', those of `Actor::new_client`) sends its requests through the same connection pool.
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

## Input

- **Encrypted PKCS#8 keys are not supported for input secrets.** The platform's key is a legacy
  encrypted PKCS#1 PEM (DES-EDE3-CBC), which is supported, as are AES-CBC encrypted and
  unencrypted keys. Node.js also reads `BEGIN ENCRYPTED PRIVATE KEY` PEMs.
- **Pseudo-URLs escape characters above U+00FF correctly.** `purlToRegExp` writes them as
  `\xHHHH`, which a JavaScript regex reads as `\xHH` followed by two literal characters, so such
  pseudo-URLs never match. Here they match the character.
- **Pseudo-URL `[regex]` sections use the syntax of the `regex` crate.** Lookarounds and
  backreferences of JavaScript regexes are not supported.
