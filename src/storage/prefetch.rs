//! Reads made ahead of time: a storage's metadata or a record that a run will surely read, started
//! during `Actor::init` so that the run does not wait for the API when it reads them.

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;

use crawlee::core::StorageResult;
use parking_lot::Mutex;

/// A read made ahead: its result, or `None` when it failed (it is then made again).
type Read<T> = Arc<tokio::sync::OnceCell<Option<T>>>;

/// Reads made ahead, by what they read. Each serves the first read of the same thing only, so a
/// later read gets the current value; a write discards the read ahead with [`forget`](Self::forget).
pub(crate) struct Prefetched<K, T>(Mutex<HashMap<K, Read<T>>>);

impl<K, T> Default for Prefetched<K, T> {
    fn default() -> Self {
        Prefetched(Mutex::new(HashMap::new()))
    }
}

impl<K: Eq + Hash, T: Clone + Send + Sync + 'static> Prefetched<K, T> {
    /// Starts `read` in the background.
    pub(crate) fn start<F>(&self, key: K, read: F)
    where
        F: Future<Output = StorageResult<T>> + Send + 'static,
    {
        let cell = Read::default();
        self.0.lock().insert(key, cell.clone());
        tokio::spawn(async move {
            cell.get_or_init(|| async { read.await.ok() }).await;
        });
    }

    /// The result of the read made ahead for `key`, waiting for it if it is in flight, or else of
    /// `read` (when there is none, or it failed).
    pub(crate) async fn take_or<F>(&self, key: &K, read: impl Fn() -> F) -> StorageResult<T>
    where
        F: Future<Output = StorageResult<T>>,
    {
        let cell = {
            let mut reads = self.0.lock();
            if reads.is_empty() { None } else { reads.remove(key) }
        };
        // Done already, or in flight (and waited for), or not started yet (and made here).
        if let Some(cell) = cell
            && let Some(value) = cell.get_or_init(|| async { read().await.ok() }).await
        {
            return Ok(value.clone());
        }
        read().await
    }

    /// Discards the read made ahead for `key`, when what it read changes.
    pub(crate) fn forget(&self, key: &K) {
        let mut reads = self.0.lock();
        if !reads.is_empty() {
            reads.remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crawlee::core::errors::StorageError;

    use super::*;

    #[tokio::test]
    async fn a_read_ahead_serves_the_first_read_only() {
        let prefetched = Prefetched::<&str, u32>::default();
        prefetched.start("a", async { Ok(1) });
        tokio::task::yield_now().await;
        let reads = AtomicUsize::new(0);
        let read = || async {
            reads.fetch_add(1, Ordering::SeqCst);
            Ok(2)
        };
        assert_eq!(prefetched.take_or(&"a", read).await.unwrap(), 1);
        assert_eq!(prefetched.take_or(&"a", read).await.unwrap(), 2);
        assert_eq!(prefetched.take_or(&"b", read).await.unwrap(), 2);
        assert_eq!(reads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_failed_or_forgotten_read_ahead_is_read_again() {
        let prefetched = Prefetched::<&str, u32>::default();
        prefetched.start("a", async { Err(StorageError::Backend("boom".into())) });
        prefetched.start("b", async { Ok(1) });
        tokio::task::yield_now().await;
        prefetched.forget(&"b");
        assert_eq!(prefetched.take_or(&"a", || async { Ok(2) }).await.unwrap(), 2);
        assert_eq!(prefetched.take_or(&"b", || async { Ok(3) }).await.unwrap(), 3);
    }

    #[tokio::test]
    async fn a_read_ahead_in_flight_is_waited_for() {
        let prefetched = Prefetched::<&str, u32>::default();
        let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
        prefetched.start("a", async move {
            receiver.await.unwrap();
            Ok(1)
        });
        tokio::task::yield_now().await;
        let read = tokio::spawn(async move { prefetched.take_or(&"a", || async { Ok(2) }).await.unwrap() });
        sender.send(()).unwrap();
        assert_eq!(read.await.unwrap(), 1);
    }
}
