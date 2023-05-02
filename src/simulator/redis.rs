use std::{
    collections::HashMap,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use tokio::sync::Mutex;
use tracing::error;

use crate::{clock::Clock, redis, Lock};

/// Fake Redis that can be used in simulations.
#[derive(Debug)]
pub(crate) struct Redis {
    /// The config of this fake redis.
    config: Config,
    clock: Arc<dyn Clock>,
    /// The key value pairs.
    entries: Mutex<HashMap<String, Entry>>,
}

#[derive(Debug)]
pub(crate) struct Config {
    /// A fake address to be used as this Redis address.
    pub(crate) address: String,
    /// The chance of a call to `Redis::lock` failing.
    pub(crate) lock_failure_chance: usize,
    /// The chance of a call to `Redis::release_lock` failing.
    pub(crate) relase_lock_failure_chance: usize,
}

#[derive(Debug)]
struct Entry {
    value: String,
    ttl: Duration,
    inserted_at: u64,
}

impl Redis {
    pub(crate) fn new(config: Config, clock: Arc<dyn Clock>) -> Self {
        Self {
            config,
            clock,
            entries: Mutex::new(HashMap::new()),
        }
    }

    async fn remove_expired_entries(&self) {
        let mut entries = self.entries.lock().await;
        entries.retain(|_key, value| {
            self.clock.now() - value.inserted_at < value.ttl.as_millis() as u64
        });
    }
}

#[async_trait]
impl redis::Redis for Redis {
    async fn lock(&self, lock: &Lock, ttl: Duration) -> Result<(), ()> {
        let mut entries = self.entries.lock().await;

        if entries.get(&lock.key).is_some() {
            return Err(());
        }

        entries.insert(
            lock.key.clone(),
            Entry {
                value: lock.value(),
                ttl,
                inserted_at: self.clock.now(),
            },
        );

        Ok(())
    }

    async fn release_lock(&self, lock: &Lock) -> Result<(), ()> {
        let mut entries = self.entries.lock().await;

        if let Some(entry) = entries.get(&lock.key) {
            if entry.value == lock.value() {
                entries.remove(&lock.key);
            }
        }

        Ok(())
    }

    fn address(&self) -> &str {
        &self.config.address
    }
}

/// Calls a method to remove expired entries from the fake redis periodically.
async fn cleaner(redis: Weak<Redis>) {
    loop {
        match redis.upgrade() {
            None => {
                error!("cleaner: redis Arc has been dropped, will exit cleaner loop");
            }
            Some(redis) => {
                redis.remove_expired_entries().await;
            }
        }
    }
}
