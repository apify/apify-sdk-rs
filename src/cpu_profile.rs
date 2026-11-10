//! A sampling CPU profile of the run, for finding where the CPU time goes (HTTP, TLS, parsing,
//! extraction...), with the `cpu-profile` feature (Linux). [`start_from_env`] starts it at the
//! start of the process when `PROFILE_CPU=1`, and [`start`] from anywhere (when the Actor's input
//! asks for it, say); [`save`] at the end of the run saves it. `PROFILE_CPU_HZ` sets the sampling
//! frequency (997 by default, an upper bound: the kernel's scheduler tick, often 250 Hz, limits
//! it).
//!
//! At the end of the run, the default key-value store gets:
//! - `CPU_PROFILE.svg`: a flamegraph of all threads, to open in a browser.
//! - `CPU_PROFILE.pb`: the profile in pprof's format, for `go tool pprof` or https://pprof.me.
//!
//! The log gets the share of the samples of each group of crates (TLS, HTTP, decompression, JSON,
//! HTML, the Actor's own crate...) and the functions with the most samples of their own (self time).
//!
//! The stacks are unwound from the unwind tables (`.eh_frame`) without allocating, so that taking
//! a sample is safe in a signal handler, and named from the symbol table: functions inlined into
//! another count as part of it. The release build has no debug info, whose parsing at the end of
//! the run would need more memory than the smallest runs have; for inlined functions, profile a
//! build with debug info locally.
//!
//! The profiler keeps every distinct stack of every thread with its full depth, which grows
//! without bound in a long run. So every [`FLUSH_INTERVAL`] its stacks move to [`Stacks`], which
//! keeps a code address once, merges threads of the same name and names each address once, and
//! the profiler starts over.

use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use crawlee::{KeyValueStore, StorageIdentifier};
use pprof::protos::Message;

use crate::Actor;

/// Samples are taken per CPU time, not wall time, and a run mostly waits on the network: 99 Hz
/// leaves a short run with too few samples. 997 rather than 1000 not to sample in lockstep with
/// periodic work.
const DEFAULT_FREQUENCY_HZ: i32 = 997;
const FLUSH_INTERVAL: Duration = Duration::from_secs(15);
/// The flamegraph folds call paths with a smaller share of the samples into their caller: they
/// would be too narrow to see, and a long run has many.
const MIN_FLAMEGRAPH_SHARE: f64 = 0.0005;
/// How many functions the log lists by self time.
const TOP_FUNCTIONS: usize = 20;
/// How many crates the log names in each group.
const CRATES_PER_GROUP: usize = 4;

struct Profiler {
    /// The Actor's own crate, a group of its own.
    actor_crate: String,
    /// `None` when restarting it failed: the stacks so far are still saved.
    guard: Option<pprof::ProfilerGuard<'static>>,
    frequency: i32,
    started: SystemTime,
    cpu_time_at_start: Option<Duration>,
    stacks: Stacks,
}

/// The running profile, saved by [`save`] at the end of the run.
static PROFILE: parking_lot::Mutex<Option<Profiler>> = parking_lot::Mutex::new(None);

/// Starts profiling when `PROFILE_CPU` is set (to anything but `0` or `false`), to cover the whole
/// run from its start. Call it before the runtime starts its threads (see [`warm_up`]).
/// `actor_crate` is the Actor's own crate (`env!("CARGO_PKG_NAME")`), whose samples the log
/// counts apart.
pub fn start_from_env(actor_crate: &str) {
    let enabled = std::env::var("PROFILE_CPU").is_ok_and(|value| !matches!(value.as_str(), "" | "0" | "false"));
    if enabled {
        start(actor_crate);
    }
}

/// Starts profiling all threads, unless a profile is running already. Started once the Actor's
/// input is read, the runtime's threads exist, but they are mostly idle, so the samples of
/// [`warm_up`] still land on this thread.
pub fn start(actor_crate: &str) {
    let mut profile = PROFILE.lock();
    if profile.is_some() {
        return;
    }
    let frequency =
        std::env::var("PROFILE_CPU_HZ").ok().and_then(|value| value.parse().ok()).unwrap_or(DEFAULT_FREQUENCY_HZ);
    let Some(guard) = new_guard(frequency) else { return };
    warm_up(frequency);
    *profile = Some(Profiler {
        actor_crate: actor_crate.to_owned(),
        guard: Some(guard),
        frequency,
        started: SystemTime::now(),
        cpu_time_at_start: process_cpu_time(),
        stacks: Stacks::default(),
    });
    drop(profile);
    let flusher = std::thread::Builder::new().name("cpu-profile".to_owned()).spawn(|| {
        loop {
            std::thread::sleep(FLUSH_INTERVAL);
            match PROFILE.lock().as_mut() {
                Some(profiler) => profiler.flush(true),
                None => return,
            }
        }
    });
    if let Err(err) = flusher {
        tracing::warn!("Failed to start the CPU profile's flushing: {err}");
    }
    tracing::info!("CPU profiling at {frequency} Hz");
}

/// Stops the running profile, if any, and saves it. Failures are logged, not returned: the
/// profile must not fail the run.
pub async fn save() {
    let profiler = PROFILE.lock().take();
    let Some(mut profiler) = profiler else { return };
    profiler.flush(false);
    let stacks = &profiler.stacks;
    tracing::info!("CPU profile: {} distinct stacks of {} functions", stacks.counts.len(), stacks.names.len());
    let cpu_time = profiler.cpu_time_at_start.zip(process_cpu_time()).map(|(start, end)| end.saturating_sub(start));
    let period = sampling_period(stacks.samples(), cpu_time, profiler.frequency);
    log_summary(stacks, &profiler.actor_crate, cpu_time, period);

    let svg = flamegraph(stacks).unwrap_or_else(|err| {
        tracing::warn!("Failed to render the CPU flamegraph: {err}");
        Vec::new()
    });
    let pb = pprof_profile(stacks, period, profiler.started).encode_to_vec();
    if let Err(err) = store(svg, pb).await {
        tracing::warn!("Failed to save the CPU profile: {err}");
    }
}

/// The CPU time of the process so far, user and system, from `/proc/self/stat` (Linux).
fn process_cpu_time() -> Option<Duration> {
    // Clock ticks of `/proc` (`USER_HZ`), 100 on Linux.
    const TICKS_PER_SECOND: u64 = 100;
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // After the command name, which is in parentheses and can hold spaces, come the fields from
    // the 3rd (state); utime and stime are the 14th and 15th.
    let mut fields = stat.get(stat.rfind(')')? + 1..)?.split_whitespace().skip(11);
    let user: u64 = fields.next()?.parse().ok()?;
    let system: u64 = fields.next()?.parse().ok()?;
    Some(Duration::from_millis((user + system) * 1000 / TICKS_PER_SECOND))
}

/// The CPU time a sample stands for. The profiler's timer only fires on the kernel's scheduler
/// tick (often 250 Hz), so the frequency asked for is an upper bound: the CPU time the samples
/// were taken over gives the real period.
fn sampling_period(samples: u64, cpu_time: Option<Duration>, frequency: i32) -> Duration {
    match cpu_time {
        Some(cpu_time) if samples > 0 && !cpu_time.is_zero() => cpu_time / samples.min(u64::from(u32::MAX)) as u32,
        _ => Duration::from_secs(1) / frequency.max(1).unsigned_abs(),
    }
}

fn new_guard(frequency: i32) -> Option<pprof::ProfilerGuard<'static>> {
    match pprof::ProfilerGuardBuilder::default().frequency(frequency).build() {
        Ok(guard) => Some(guard),
        Err(err) => {
            tracing::warn!("Failed to start CPU profiling: {err}");
            None
        }
    }
}

/// The unwinder loads the unwind tables of the binary and the libraries on the first sample, in
/// the signal handler, which allocates. Spinning (without allocating) for a few sampling periods
/// makes the first samples land here, on a thread that holds no lock, rather than in another
/// thread's `malloc`. So profiling must start while the other threads are idle: before the
/// runtime starts, or before the crawl. The tables stay loaded when the profiler restarts.
fn warm_up(frequency: i32) {
    let period_ms = 1000 / u64::from(frequency.max(1).unsigned_abs());
    let duration = Duration::from_millis((3 * period_ms).clamp(10, 1000));
    let start = std::time::Instant::now();
    while start.elapsed() < duration {
        std::hint::spin_loop();
    }
}

impl Profiler {
    /// Moves the profiler's stacks to [`Stacks`] and, with `restart`, starts the profiler over.
    fn flush(&mut self, restart: bool) {
        let Some(guard) = self.guard.take() else { return };
        let report = guard.report().build_unresolved();
        drop(guard);
        if restart {
            self.guard = new_guard(self.frequency);
        }
        match report {
            Ok(report) => self.stacks.add(&report),
            Err(err) => tracing::warn!("Failed to collect the CPU profile's samples: {err}"),
        }
    }
}

/// The samples so far, compactly: each code address is named once, a location is the list of
/// functions at an address (the functions inlined there, the innermost first and the function
/// the code belongs to last; without debug info, only that one), and a stack is a list of
/// locations, the leaf first. Threads of the same name (the runtime's workers) are merged.
#[derive(Default)]
struct Stacks {
    /// Function and thread names.
    names: Vec<String>,
    name_ids: HashMap<String, u32>,
    locations: Vec<Box<[u32]>>,
    location_ids: HashMap<Box<[u32]>, u32>,
    location_of_address: HashMap<usize, u32>,
    /// The mapped files, for naming the addresses that have no symbol.
    modules: Vec<Module>,
    /// Sample counts by thread name and stack.
    counts: HashMap<(u32, Box<[u32]>), u64>,
}

impl Stacks {
    fn add(&mut self, report: &pprof::UnresolvedReport) {
        for (frames, &count) in &report.data {
            let thread = String::from_utf8_lossy(&frames.thread_name[..frames.thread_name_length]).into_owned();
            let thread = self.name_id(&thread);
            let stack: Box<[u32]> = frames.frames.iter().map(|frame| self.location_of(frame.ip)).collect();
            *self.counts.entry((thread, stack)).or_default() += count.unsigned_abs() as u64;
        }
    }

    fn name_id(&mut self, name: &str) -> u32 {
        if let Some(&id) = self.name_ids.get(name) {
            return id;
        }
        let id = self.names.len() as u32;
        self.names.push(name.to_owned());
        self.name_ids.insert(name.to_owned(), id);
        id
    }

    fn location_of(&mut self, address: usize) -> u32 {
        if let Some(&id) = self.location_of_address.get(&address) {
            return id;
        }
        let mut names = Vec::new();
        backtrace::resolve(address as *mut std::ffi::c_void, |symbol| {
            names.push(symbol.name().map_or_else(|| "Unknown".to_owned(), |name| format!("{name:#}")));
        });
        if names.is_empty() {
            // No symbol: code of a library that exports none for it (libc's `memcpy` and
            // `memset` variants), or of the vDSO. Named by its file, it still passes its time to
            // its caller in the summary's groups.
            names.push(format!("[{}]", self.module_of(address).unwrap_or("unknown")));
        }
        let functions: Box<[u32]> = names.iter().map(|name| self.name_id(name)).collect();
        let next = self.locations.len() as u32;
        let id = *self.location_ids.entry(functions.clone()).or_insert(next);
        if id == next {
            self.locations.push(functions);
        }
        self.location_of_address.insert(address, id);
        id
    }

    fn module_of(&mut self, address: usize) -> Option<&str> {
        let find = |modules: &[Module]| modules.iter().position(|module| module.range.contains(&address));
        let index = match find(&self.modules) {
            Some(index) => index,
            None => {
                // Mapped since the last read (a library loaded later), or not mapped.
                self.modules = modules();
                find(&self.modules)?
            }
        };
        Some(&self.modules[index].name)
    }

    /// The function the code of a location belongs to.
    fn function_at(&self, location: u32) -> Option<&str> {
        let function = *self.locations[location as usize].last()?;
        Some(&self.names[function as usize])
    }

    fn samples(&self) -> u64 {
        self.counts.values().sum()
    }
}

struct Module {
    range: std::ops::Range<usize>,
    /// The file name, or the kernel's name (`[vdso]`).
    name: String,
}

/// The mapped files of the process, from `/proc/self/maps` (Linux).
fn modules() -> Vec<Module> {
    let maps = std::fs::read_to_string("/proc/self/maps").unwrap_or_default();
    maps.lines().filter_map(module_of_maps_line).collect()
}

/// `start-end perms offset dev inode path`, with a path.
fn module_of_maps_line(line: &str) -> Option<Module> {
    let mut fields = line.split_whitespace();
    let (start, end) = fields.next()?.split_once('-')?;
    let path = fields.nth(4)?;
    Some(Module {
        range: usize::from_str_radix(start, 16).ok()?..usize::from_str_radix(end, 16).ok()?,
        name: path.rsplit('/').next().unwrap_or(path).to_owned(),
    })
}

/// A flamegraph of the stacks, by thread name, with the call paths of less than
/// [`MIN_FLAMEGRAPH_SHARE`] of the samples folded into their caller.
fn flamegraph(stacks: &Stacks) -> anyhow::Result<Vec<u8>> {
    /// A node of the call tree. Few children each, so a list (by name) is smaller than a map;
    /// a long run has many nodes.
    #[derive(Default)]
    struct Node {
        children: Vec<(u32, u32)>,
        samples: u64,
        own_samples: u64,
    }
    fn fold(nodes: &[Node], node: usize, path: &mut Vec<u32>, min: u64, names: &[String], lines: &mut Vec<String>) {
        let mut own_samples = nodes[node].own_samples;
        for &(name, child) in &nodes[node].children {
            let child = child as usize;
            if nodes[child].samples < min {
                own_samples += nodes[child].samples;
                continue;
            }
            path.push(name);
            fold(nodes, child, path, min, names, lines);
            path.pop();
        }
        if own_samples > 0 && !path.is_empty() {
            // `;` separates the frames of a line; Rust names have it in array types.
            let frames: Vec<String> = path.iter().map(|&name| names[name as usize].replace(';', ":")).collect();
            lines.push(format!("{} {own_samples}", frames.join(";")));
        }
    }

    // The call tree, by thread name and then from the root of the stacks.
    let mut nodes = vec![Node::default()];
    for ((thread, stack), &count) in &stacks.counts {
        let path = std::iter::once(*thread)
            .chain(stack.iter().rev().flat_map(|&location| stacks.locations[location as usize].iter().rev().copied()));
        let mut node = 0;
        nodes[node].samples += count;
        for name in path {
            let child = nodes[node].children.iter().find(|(child_name, _)| *child_name == name);
            node = match child {
                Some(&(_, child)) => child as usize,
                None => {
                    nodes.push(Node::default());
                    let child = nodes.len() - 1;
                    nodes[node].children.push((name, child as u32));
                    child
                }
            };
            nodes[node].samples += count;
        }
        nodes[node].own_samples += count;
    }
    let min = ((nodes[0].samples as f64 * MIN_FLAMEGRAPH_SHARE) as u64).max(1);
    let mut lines = Vec::new();
    fold(&nodes, 0, &mut Vec::new(), min, &stacks.names, &mut lines);
    if lines.is_empty() {
        return Ok(Vec::new());
    }
    let mut options = pprof::flamegraph::Options::default();
    options.title = "CPU profile".to_owned();
    options.count_name = "samples".to_owned();
    let mut svg = Vec::new();
    pprof::flamegraph::from_lines(&mut options, lines.iter().map(String::as_str), &mut svg)?;
    Ok(svg)
}

/// The stacks in pprof's format, laid out as pprof's own reports: a location per function, and
/// the thread name as a label of each sample.
fn pprof_profile(stacks: &Stacks, period: Duration, started: SystemTime) -> pprof::protos::Profile {
    use pprof::protos::{Function, Label, Line, Location, Profile, Sample, ValueType};

    // The names, after the empty string that must come first, then the profile's own strings.
    let id = |name: u32| u64::from(name) + 1;
    let mut string_table = vec![String::new()];
    string_table.extend(stacks.names.iter().cloned());
    let mut string = |value: &str| {
        string_table.push(value.to_owned());
        string_table.len() as i64 - 1
    };
    let (samples, count, cpu, nanoseconds, thread) =
        (string("samples"), string("count"), string("cpu"), string("nanoseconds"), string("thread"));

    let functions: BTreeSet<u32> = stacks.locations.iter().flatten().copied().collect();
    let period = period.as_nanos() as i64;
    let time = ValueType { ty: cpu, unit: nanoseconds };
    Profile {
        sample_type: vec![ValueType { ty: samples, unit: count }, time.clone()],
        sample: stacks
            .counts
            .iter()
            .map(|((thread_name, stack), &samples)| Sample {
                location_id: stack
                    .iter()
                    .flat_map(|&location| stacks.locations[location as usize].iter().map(|&name| id(name)))
                    .collect(),
                value: vec![samples as i64, samples as i64 * period],
                label: vec![Label { key: thread, str: id(*thread_name) as i64, ..Default::default() }],
            })
            .collect(),
        function: functions
            .iter()
            .map(|&name| Function {
                id: id(name),
                name: id(name) as i64,
                system_name: id(name) as i64,
                ..Default::default()
            })
            .collect(),
        location: functions
            .iter()
            .map(|&name| Location {
                id: id(name),
                line: vec![Line { function_id: id(name), line: 0 }],
                ..Default::default()
            })
            .collect(),
        string_table,
        time_nanos: started.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_nanos() as i64,
        duration_nanos: started.elapsed().unwrap_or_default().as_nanos() as i64,
        period_type: Some(time),
        period,
        ..Default::default()
    }
}

async fn store(svg: Vec<u8>, pb: Vec<u8>) -> anyhow::Result<()> {
    let store: KeyValueStore = Actor::open_key_value_store(&StorageIdentifier::Default, Default::default()).await?;
    if !svg.is_empty() {
        store.set_bytes("CPU_PROFILE.svg", Bytes::from(svg), Some("image/svg+xml")).await?;
    }
    if !pb.is_empty() {
        store.set_bytes("CPU_PROFILE.pb", Bytes::from(pb), Some("application/octet-stream")).await?;
    }
    tracing::info!(
        "Saved the CPU profile to the CPU_PROFILE.svg and CPU_PROFILE.pb records of the default key-value store"
    );
    Ok(())
}

/// The groups of crates of the summary. A sample counts for the crate of the innermost function
/// on its stack that has one: the standard library, libc and the kernel pass their time to their
/// caller, so that `malloc` called by serde_json counts as JSON. Code inlined into a function
/// counts for that function's crate.
const GROUPS: &[(&str, &[&str])] = &[
    (
        "TLS",
        &[
            "rustls",
            "aws-lc-rs",
            "aws-lc-sys",
            "ring",
            "rustls-webpki",
            "rustls-pki-types",
            "rustls-native-certs",
            "rustls-platform-verifier",
            "webpki-roots",
        ],
    ),
    (
        "HTTP",
        &[
            "hyper",
            "hyper-util",
            "h2",
            "h3",
            "h3-quinn",
            "quinn",
            "quinn-proto",
            "quinn-udp",
            "http",
            "http-body",
            "http-body-util",
            "httparse",
            "reqwest",
            "impit",
            "tower",
            "tower-http",
            "hickory-proto",
            "hickory-resolver",
            "socket2",
            "crawlee-http-client",
            "crawlee-impit",
            "cookie_store",
            "cookie",
            "url",
            "idna",
            "percent-encoding",
            "form_urlencoded",
            "bytes",
        ],
    ),
    (
        "Decompression",
        &[
            "brotli",
            "brotli-decompressor",
            "flate2",
            "miniz_oxide",
            "zstd",
            "zstd-safe",
            "zstd-sys",
            "async-compression",
            "compression-codecs",
            "alloc-no-stdlib",
            "alloc-stdlib",
        ],
    ),
    ("JSON", &["serde_json", "serde", "serde_core", "indexmap"]),
    (
        "HTML",
        &[
            "html5ever",
            "markup5ever",
            "scraper",
            "selectors",
            "cssparser",
            "lol_html",
            "tendril",
            "string_cache",
            "ego-tree",
        ],
    ),
    (
        "Crawlee & SDK",
        &[
            "crawlee",
            "crawlee-core",
            "crawlee-basic",
            "crawlee-http",
            "crawlee-utils",
            "crawlee-storage",
            "apify",
            "apify-sdk-rs",
            "apify-client",
            "apify-client-rust",
        ],
    ),
    (
        "Async runtime",
        &[
            "tokio",
            "tokio-util",
            "mio",
            "futures-util",
            "futures-core",
            "futures-channel",
            "parking_lot",
            "parking_lot_core",
        ],
    ),
    ("Logging", &["tracing", "tracing-core", "tracing-subscriber"]),
];
/// The group of the Actor's own crate.
const THIS_ACTOR: &str = "This Actor";
const OTHER: &str = "Other";
const UNATTRIBUTED: &str = "Unattributed (standard library, libc, kernel)";

/// Prefixes of the C and assembly functions of crates, which have no Rust path. aws-lc's
/// functions carry its version in the prefix (`aws_lc_0_45_0_aes_gcm_decrypt_avx512`).
const C_PREFIXES: &[(&str, &str)] = &[
    ("aws_lc_", "aws_lc_sys"),
    ("jent_", "aws_lc_sys"),
    ("ZSTD_", "zstd_sys"),
    ("HUF_", "zstd_sys"),
    ("FSE_", "zstd_sys"),
];

/// The crate of a function, from the root of its demangled path (`<serde_json::de::Deserializer<R>
/// as serde::de::Deserializer>::deserialize_any` is serde_json's), or the prefix of a C function.
/// `None` for the standard library and for C functions it does not know (libc's `malloc`,
/// `memcpy`), whose time goes to their caller.
fn crate_of(function: &str) -> Option<String> {
    static ROOT: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"([A-Za-z_][A-Za-z0-9_]*)::").unwrap());
    let Some(root) = ROOT.captures(function) else {
        return C_PREFIXES
            .iter()
            .find(|(prefix, _)| function.starts_with(prefix))
            .map(|(_, krate)| (*krate).to_owned());
    };
    match &root[1] {
        "core" | "alloc" | "std" => None,
        krate => Some(krate.to_owned()),
    }
}

/// The group of a crate, by its name as in Cargo.toml or as in paths (`-` as `_`).
fn group_of(krate: &str, actor_crate: &str) -> &'static str {
    let normalize = |name: &str| name.replace('-', "_");
    let krate = normalize(krate);
    if krate == normalize(actor_crate) {
        return THIS_ACTOR;
    }
    GROUPS
        .iter()
        .find(|(_, crates)| crates.iter().any(|name| normalize(name) == krate))
        .map_or(OTHER, |(group, _)| group)
}

/// The share of the samples of each group and crate, and of the functions by self time, largest
/// first.
struct GroupShare {
    group: &'static str,
    share: f64,
    /// The crates of the group with their shares.
    crates: Vec<(String, f64)>,
}

struct Summary {
    samples: u64,
    groups: Vec<GroupShare>,
    functions: Vec<(String, f64)>,
}

fn summarize(stacks: &Stacks, actor_crate: &str) -> Summary {
    let mut groups: HashMap<&'static str, GroupCount> = HashMap::new();
    let mut functions: HashMap<&str, u64> = HashMap::new();
    let all = stacks.samples();
    for ((_, stack), &count) in &stacks.counts {
        // Self time goes to the function the leaf's code belongs to, as the innermost function
        // inlined there is often a helper (`black_box`, an iterator's `next`).
        if let Some(function) = stack.first().and_then(|&leaf| stacks.function_at(leaf)) {
            *functions.entry(function).or_default() += count;
        }
        let krate = stack.iter().filter_map(|&location| stacks.function_at(location)).find_map(crate_of);
        let group =
            groups.entry(krate.as_deref().map_or(UNATTRIBUTED, |krate| group_of(krate, actor_crate))).or_default();
        group.samples += count;
        if let Some(krate) = krate {
            *group.crates.entry(krate).or_default() += count;
        }
    }
    let share = |count: u64| count as f64 * 100.0 / all.max(1) as f64;
    let largest_first = |counts: Vec<(String, u64)>| {
        let mut counts = counts;
        counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        counts
    };

    let mut groups: Vec<_> = groups.into_iter().collect();
    groups.sort_by(|a, b| b.1.samples.cmp(&a.1.samples).then_with(|| a.0.cmp(b.0)));
    let groups = groups
        .into_iter()
        .map(|(group, count)| GroupShare {
            group,
            share: share(count.samples),
            crates: largest_first(count.crates.into_iter().collect())
                .into_iter()
                .map(|(krate, count)| (krate, share(count)))
                .collect(),
        })
        .collect();
    let mut functions = largest_first(functions.into_iter().map(|(name, count)| (name.to_owned(), count)).collect());
    functions.truncate(TOP_FUNCTIONS);
    Summary {
        samples: all,
        groups,
        functions: functions.into_iter().map(|(name, count)| (name, share(count))).collect(),
    }
}

#[derive(Default)]
struct GroupCount {
    samples: u64,
    crates: HashMap<String, u64>,
}

fn log_summary(stacks: &Stacks, actor_crate: &str, cpu_time: Option<Duration>, period: Duration) {
    let summary = summarize(stacks, actor_crate);
    let cpu_time = cpu_time.map_or_else(|| "unknown".to_owned(), |time| format!("{:.1}", time.as_secs_f64()));
    tracing::info!(
        "CPU profile: {} samples over {cpu_time} CPU-seconds, one per {:.1} ms of CPU time",
        summary.samples,
        period.as_secs_f64() * 1000.0
    );
    if summary.samples == 0 {
        return;
    }
    let groups: Vec<String> = summary
        .groups
        .iter()
        .map(|GroupShare { group, share, crates }| {
            let crates: Vec<String> =
                crates.iter().take(CRATES_PER_GROUP).map(|(krate, share)| format!("{krate} {share:.1}%")).collect();
            if crates.is_empty() {
                format!("{share:5.1}%  {group}")
            } else {
                format!("{share:5.1}%  {group} ({})", crates.join(", "))
            }
        })
        .collect();
    tracing::info!("CPU profile by group of crates:\n{}", groups.join("\n"));
    let functions: Vec<String> =
        summary.functions.iter().map(|(name, share)| format!("{share:5.1}%  {name}")).collect();
    tracing::info!("CPU profile, top functions by self time:\n{}", functions.join("\n"));
}

#[cfg(test)]
mod tests {
    use std::hint::black_box;
    use std::time::Instant;

    use super::*;

    #[inline(never)]
    fn hot_function_for_the_profile(seed: u64) -> u64 {
        (0..10_000u64).fold(seed, |acc, i| black_box(acc.wrapping_mul(31).wrapping_add(i)))
    }

    fn spin_in_the_hot_function(duration: Duration) {
        let start = Instant::now();
        let mut value = 0;
        while start.elapsed() < duration {
            value = hot_function_for_the_profile(value);
        }
        black_box(value);
    }

    /// Sampled and flushed twice (as the flushing thread does), then summarized, drawn and encoded.
    #[test]
    fn the_hot_function_leads_and_counts_for_this_actor() {
        let mut profiler = Profiler {
            actor_crate: env!("CARGO_PKG_NAME").to_owned(),
            guard: new_guard(1000),
            frequency: 1000,
            started: SystemTime::now(),
            cpu_time_at_start: process_cpu_time(),
            stacks: Stacks::default(),
        };
        warm_up(1000);
        spin_in_the_hot_function(Duration::from_millis(300));
        profiler.flush(true);
        spin_in_the_hot_function(Duration::from_millis(300));
        profiler.flush(false);
        assert!(profiler.guard.is_none());

        let summary = summarize(&profiler.stacks, &profiler.actor_crate);
        // At least a 100 Hz scheduler tick, over 0.6 s, from both rounds.
        assert!(summary.samples > 40, "{}", summary.samples);
        // In a debug build the loop's iterator functions are not inlined, so they lead by self
        // time (and their names carry no generic arguments); the standard library still passes
        // their samples to the hot function's crate in the groups.
        if !cfg!(debug_assertions) {
            let (leader, share) = &summary.functions[0];
            assert!(leader.ends_with("hot_function_for_the_profile"), "{:#?}", summary.functions);
            assert!(*share > 50.0, "{:#?}", summary.functions);
        }
        let GroupShare { group, share, .. } = &summary.groups[0];
        assert_eq!(*group, THIS_ACTOR);
        assert!(*share > 50.0, "{share}");

        let svg = String::from_utf8(flamegraph(&profiler.stacks).unwrap()).unwrap();
        assert!(svg.contains("hot_function_for_the_profile"));
        let profile = pprof::protos::Profile::decode(
            pprof_profile(&profiler.stacks, Duration::from_millis(1), profiler.started).encode_to_vec().as_slice(),
        )
        .unwrap();
        let samples: i64 = profile.sample.iter().map(|sample| sample.value[0]).sum();
        assert_eq!(samples as u64, summary.samples);
        let named = profile
            .function
            .iter()
            .any(|function| profile.string_table[function.name as usize].contains("hot_function_for_the_profile"));
        assert!(named);
    }

    #[test]
    fn modules_of_maps_lines() {
        let module = module_of_maps_line(
            "7f3a1c200000-7f3a1c3a8000 r-xp 00028000 08:01 1835 /usr/lib/x86_64-linux-gnu/libc.so.6",
        )
        .unwrap();
        assert_eq!(module.range, 0x7f3a1c200000..0x7f3a1c3a8000);
        assert_eq!(module.name, "libc.so.6");
        let vdso = module_of_maps_line("7ffd5a3f2000-7ffd5a3f4000 r-xp 00000000 00:00 0 [vdso]").unwrap();
        assert_eq!(vdso.name, "[vdso]");
        assert!(module_of_maps_line("7f3a1c3a8000-7f3a1c3ac000 rw-p 00000000 00:00 0").is_none());

        // An address of this binary is in one of its modules.
        let mut stacks = Stacks::default();
        let address = modules_of_maps_lines as fn() as usize;
        assert!(stacks.module_of(address).is_some());
    }

    #[test]
    fn cpu_time_and_sampling_period() {
        let before = process_cpu_time().unwrap();
        spin_in_the_hot_function(Duration::from_millis(100));
        let spent = process_cpu_time().unwrap() - before;
        assert!(spent >= Duration::from_millis(50), "{spent:?}");

        let period = sampling_period(250, Some(Duration::from_secs(1)), 997);
        assert_eq!(period, Duration::from_millis(4));
        assert_eq!(sampling_period(0, Some(Duration::from_secs(1)), 1000), Duration::from_millis(1));
        assert_eq!(sampling_period(10, None, 1000), Duration::from_millis(1));
    }

    #[test]
    fn crates_of_functions() {
        let cases = [
            (
                "<serde_json::de::Deserializer<serde_json::read::SliceRead> as serde_core::de::Deserializer>::deserialize_any",
                Some("serde_json"),
            ),
            ("<&mut serde_json::ser::Serializer<W>>::serialize_str", Some("serde_json")),
            ("tokio::runtime::scheduler::multi_thread::worker::Context::run", Some("tokio")),
            ("<hyper::proto::h1::conn::Conn<I, B, T>>::poll_read_head", Some("hyper")),
            ("my_actor::extractors::extract_profile", Some("my_actor")),
            ("<alloc::string::String as core::clone::Clone>::clone", None),
            ("<core::pin::Pin<P> as core::future::Future>::poll", None),
            ("std::sys::pal::unix::time::Timespec::now", None),
            ("aws_lc_0_45_0_aes_gcm_decrypt_avx512", Some("aws_lc_sys")),
            ("ZSTD_decompressStream", Some("zstd_sys")),
            ("malloc", None),
            ("__memmove_avx512_unaligned_erms", None),
        ];
        for (function, krate) in cases {
            assert_eq!(crate_of(function).as_deref(), krate, "{function}");
        }
        assert_eq!(group_of("aws_lc_sys", "my_actor"), "TLS");
        assert_eq!(group_of("tower_http", "my_actor"), "HTTP");
        assert_eq!(group_of("serde_json", "my_actor"), "JSON");
        assert_eq!(group_of("my-actor", "my-actor"), THIS_ACTOR);
        assert_eq!(group_of("apify", "my_actor"), "Crawlee & SDK");
        assert_eq!(group_of("chrono", "my_actor"), OTHER);
    }
}
