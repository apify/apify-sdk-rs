//! The log of the Actor (`log` of `@apify/log` in JS): a `tracing` subscriber writing to stdout,
//! at the level of `APIFY_LOG_LEVEL` (or `CRAWLEE_LOG_LEVEL`), which [`set_level`] changes later
//! (`log.setLevel`).
//!
//! The level applies to the Actor's own crates, crawlee-rs and this SDK, the crates that log
//! through `log` in JS. The crates underneath (the HTTP stack) log at `info` at most. `RUST_LOG`,
//! when set, replaces all of that until [`set_level`].
//!
//! An Actor that reads its log level from its input knows it only after [`Actor::init`](crate::Actor::init)
//! (crate::Actor::init), whose debug lines (its API calls, for one) would be lost.
//! [`LogOptions::buffer_startup_debug`] keeps them until the level is set: [`set_level`] at debug
//! logs them, with the time they were made, and any other level, or
//! [`discard_startup_debug`], drops them.

use std::fmt::Write as _;
use std::io::{IsTerminal as _, Write as _};
use std::str::FromStr;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::filter::{EnvFilter, FilterExt as _, FilterFn, Targets};
use tracing_subscriber::layer::{Context, SubscriberExt as _};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::{Layer, Registry, reload};

/// The crates that log through `log` in JS, besides the Actor's own.
const LOG_CRATES: [&str; 2] = ["crawlee", "apify"];

/// A level of the log (`log.LEVELS` in JS).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Off,
    Error,
    Warning,
    Info,
    Debug,
}

impl LogLevel {
    fn directive(self) -> &'static str {
        match self {
            LogLevel::Off => "off",
            LogLevel::Error => "error",
            LogLevel::Warning => "warn",
            LogLevel::Info => "info",
            LogLevel::Debug => "debug",
        }
    }
}

/// A level by its name or number in JS (`OFF`/`0` to `PERF`/`6`). `SOFT_FAIL` logs the errors,
/// and `PERF` the debug lines: tracing has neither.
impl FromStr for LogLevel {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value.trim().to_ascii_uppercase().as_str() {
            "OFF" | "0" => LogLevel::Off,
            "ERROR" | "1" | "SOFT_FAIL" | "2" => LogLevel::Error,
            "WARNING" | "3" => LogLevel::Warning,
            "INFO" | "4" => LogLevel::Info,
            "DEBUG" | "5" | "PERF" | "6" => LogLevel::Debug,
            _ => return Err(format!("unknown log level '{value}'")),
        })
    }
}

/// How [`init`] sets up the log.
#[derive(Debug, Clone, Default)]
pub struct LogOptions {
    /// The crates the level applies to, besides crawlee-rs and the SDK: the Actor's own, by the
    /// names their logs have (`my_actor`).
    pub crates: Vec<String>,
    /// Keep the debug lines of those crates until [`set_level`] (or [`discard_startup_debug`]),
    /// when the environment does not set the level.
    pub buffer_startup_debug: bool,
}

struct Log {
    filter: reload::Handle<EnvFilter, Registry>,
    crates: Vec<String>,
}

static LOG: OnceLock<Log> = OnceLock::new();

/// Installs the log as the global subscriber. Fails if a global subscriber is installed already.
pub fn init(options: LogOptions) -> anyhow::Result<()> {
    let crates: Vec<String> = LOG_CRATES.iter().map(|name| (*name).to_owned()).chain(options.crates).collect();
    let env_level = ["APIFY_LOG_LEVEL", "CRAWLEE_LOG_LEVEL"]
        .into_iter()
        .find_map(|name| std::env::var(name).ok().filter(|value| !value.is_empty()));
    let filter = match (EnvFilter::try_from_default_env(), env_level) {
        (Ok(filter), _) => filter,
        (Err(_), Some(level)) => EnvFilter::new(directives(level.parse().map_err(anyhow::Error::msg)?, &crates)),
        (Err(_), None) => {
            if options.buffer_startup_debug {
                BUFFERING.store(true, Ordering::Relaxed);
            }
            EnvFilter::new(directives(LogLevel::Info, &crates))
        }
    };
    let (filter, handle) = reload::Layer::new(filter);
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_ansi(std::io::stdout().is_terminal()).with_filter(filter))
        .with(startup_debug(&crates)?)
        .try_init()?;
    let _ = LOG.set(Log { filter: handle, crates });
    Ok(())
}

/// Sets the level of the log (`log.setLevel`). The debug lines kept since the start
/// ([`LogOptions::buffer_startup_debug`]) are logged first at debug level, and dropped at any
/// other.
pub fn set_level(level: LogLevel) {
    if level == LogLevel::Debug {
        flush_startup_debug();
    } else {
        discard_startup_debug();
    }
    if let Some(log) = LOG.get() {
        let _ = log.filter.modify(|filter| *filter = EnvFilter::new(directives(level, &log.crates)));
    }
}

/// Drops the debug lines kept since the start, and keeps no more, leaving the level as it is.
pub fn discard_startup_debug() {
    BUFFERING.store(false, Ordering::Relaxed);
    STARTUP_LINES.lock().clear();
}

/// The filter directives of `level` for `crates`; the other crates log at `info` at most.
fn directives(level: LogLevel, crates: &[String]) -> String {
    let others = level.min(LogLevel::Info).directive();
    std::iter::once(others.to_owned())
        .chain(crates.iter().map(|name| format!("{name}={}", level.directive())))
        .collect::<Vec<_>>()
        .join(",")
}

/// Whether debug lines are still kept.
static BUFFERING: AtomicBool = AtomicBool::new(false);
static STARTUP_LINES: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn flush_startup_debug() {
    BUFFERING.store(false, Ordering::Relaxed);
    let lines = std::mem::take(&mut *STARTUP_LINES.lock());
    let mut stdout = std::io::stdout().lock();
    for line in lines {
        let _ = writeln!(stdout, "{line}");
    }
}

/// The layer that keeps the debug lines of `crates` while [`BUFFERING`].
fn startup_debug<S>(crates: &[String]) -> anyhow::Result<impl Layer<S>>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    let targets: Targets = directives(LogLevel::Debug, crates).parse()?;
    let buffering = FilterFn::new(|metadata| *metadata.level() == Level::DEBUG && BUFFERING.load(Ordering::Relaxed));
    Ok(StartupDebug.with_filter(targets.and(buffering)))
}

/// Keeps the debug lines, as the log would write them.
struct StartupDebug;

impl<S: Subscriber + for<'span> LookupSpan<'span>> Layer<S> for StartupDebug {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut line =
            format!("{} DEBUG {}: ", chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ"), event.metadata().target());
        event.record(&mut LineVisitor(&mut line));
        // Lines made while the level is being set are dropped with the rest.
        if BUFFERING.load(Ordering::Relaxed) {
            STARTUP_LINES.lock().push(line);
        }
    }
}

/// Writes the message, then the other fields as `name=value`.
struct LineVisitor<'a>(&'a mut String);

impl Visit for LineVisitor<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.0, "{value:?}");
        } else {
            let _ = write!(self.0, " {}={value:?}", field.name());
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.0.push_str(value);
        } else {
            let _ = write!(self.0, " {}={value}", field.name());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levels_are_read_by_their_js_names_and_numbers() {
        assert_eq!("DEBUG".parse(), Ok(LogLevel::Debug));
        assert_eq!("warning".parse(), Ok(LogLevel::Warning));
        assert_eq!("4".parse(), Ok(LogLevel::Info));
        assert_eq!("SOFT_FAIL".parse(), Ok(LogLevel::Error));
        assert_eq!("PERF".parse(), Ok(LogLevel::Debug));
        assert!("verbose".parse::<LogLevel>().is_err());
    }

    #[test]
    fn the_level_applies_to_the_actor_crawlee_and_the_sdk() {
        let crates = ["crawlee".to_owned(), "apify".to_owned(), "my_actor".to_owned()];
        assert_eq!(directives(LogLevel::Debug, &crates), "info,crawlee=debug,apify=debug,my_actor=debug");
        assert_eq!(directives(LogLevel::Warning, &crates), "warn,crawlee=warn,apify=warn,my_actor=warn");
        assert!(EnvFilter::try_new(directives(LogLevel::Off, &crates)).is_ok());
    }

    #[test]
    fn keeps_the_debug_lines_of_the_crates_until_the_level_is_set() {
        let layer = startup_debug(&["kept".to_owned()]).unwrap();
        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            BUFFERING.store(true, Ordering::Relaxed);
            tracing::debug!(target: "kept", count = 3, "API call");
            tracing::info!(target: "kept", "an info line is logged as usual");
            tracing::debug!(target: "other", "not a crate of the level");
            let lines = STARTUP_LINES.lock().clone();
            assert_eq!(lines.len(), 1, "{lines:?}");
            assert!(lines[0].ends_with(" DEBUG kept: API call count=3"), "{}", lines[0]);
            set_level(LogLevel::Debug);
            assert!(STARTUP_LINES.lock().is_empty());
            tracing::debug!(target: "kept", "after the level is set");
            assert!(STARTUP_LINES.lock().is_empty());
        });
    }
}
