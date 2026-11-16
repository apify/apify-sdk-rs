//! An LRU cache that allocates as it fills: `lru::LruCache::new(cap)` reserves its whole capacity
//! up front, which for the request queues' 1,000,000 requests is about 40 MB before anything is
//! cached (JS's `LruCache` grows as it fills).

use std::borrow::Borrow;
use std::hash::Hash;

#[derive(Debug)]
pub struct LazyLru<K: Hash + Eq, V> {
    inner: lru::LruCache<K, V>,
    capacity: usize,
}

impl<K: Hash + Eq, V> LazyLru<K, V> {
    pub fn new(capacity: usize) -> Self {
        LazyLru { inner: lru::LruCache::unbounded(), capacity: capacity.max(1) }
    }

    pub fn get<Q: Hash + Eq + ?Sized>(&mut self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
    {
        self.inner.get(key)
    }

    /// Caches `value`, evicting the least recently used entry when the cache is over capacity.
    pub fn put(&mut self, key: K, value: V) -> Option<V> {
        let old = self.inner.put(key, value);
        if self.inner.len() > self.capacity {
            self.inner.pop_lru();
        }
        old
    }

    pub fn pop<Q: Hash + Eq + ?Sized>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
    {
        self.inner.pop(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_least_recently_used_entry_is_evicted_over_capacity() {
        let mut cache = LazyLru::new(2);
        cache.put("a", 1);
        cache.put("b", 2);
        assert_eq!(cache.get(&"a"), Some(&1));
        assert_eq!(cache.put("c", 3), None);
        assert_eq!(cache.get(&"b"), None, "b was the least recently used");
        assert_eq!(cache.get(&"a"), Some(&1));
        assert_eq!(cache.put("a", 4), Some(1), "a key already cached is replaced");
        assert_eq!(cache.pop(&"a"), Some(4));
        assert_eq!(cache.get(&"c"), Some(&3));
    }
}
