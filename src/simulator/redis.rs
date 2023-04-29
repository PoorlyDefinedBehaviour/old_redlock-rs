//! Contains a fake Redis that can be used for simulations.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::{redis, Lock};

#[derive(Debug)]
pub(crate) struct Redis {
    /// The config of this fake redis.
    config: Config,
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
    inserted_at: Instant,
}

impl Redis {
    pub(crate) fn new(config: Config) -> Self {
        Self {
            config,
            entries: Mutex::new(HashMap::new()),
        }
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
                inserted_at: Instant::now(),
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
