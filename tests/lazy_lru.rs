//! The request queues' caches of 1,000,000 requests allocate as they fill, not up front. Alone in
//! its test binary, as it counts what the whole process allocates.

use std::alloc::System;

use apify::lru::LazyLru;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

#[test]
fn a_large_cache_allocates_nothing_up_front() {
    let region = Region::new(GLOBAL);
    let cache = LazyLru::<String, String>::new(1_000_000);
    let allocated = region.change().bytes_allocated;
    assert!(allocated < 1024, "{allocated} bytes");
    drop(cache);
}
