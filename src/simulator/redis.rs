use std::{
    collections::HashMap,
    sync::{Arc, Weak},
    time::Duration,
};

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use tokio::sync::Mutex;
use tracing::error;

use crate::{clock::Clock, r#async::Async, random::RandomGenerator, redis, Lock};

use super::r#async;

/// Fake Redis that can be used in simulations.
#[derive(Debug)]
pub(crate) struct Redis {
    /// The config of this fake redis.
    pub(crate) config: Config,
    random: Arc<dyn RandomGenerator>,
    clock: Arc<dyn Clock>,
    /// The key value pairs.
    pub(crate) entries: Mutex<HashMap<String, Entry>>,
}

#[derive(Debug)]
pub(crate) struct Config {
    /// The server id.
    pub(crate) id: usize,
    /// A fake address to be used as this Redis address.
    pub(crate) address: String,
    /// The chance of a call to `Redis::lock` failing.
    pub(crate) lock_failure_chance: f64,
    /// The chance of the message saying that a call to .lock() succeed being lost.
    pub(crate) lock_response_message_loss_chance: f64,
    /// The chance of a call to `Redis::release_lock` failing.
    pub(crate) try_relase_lock_failure_chance: f64,
    /// The chance of an ack being lost.
    pub(crate) message_loss_chance: f64,
}

#[derive(Debug)]
pub(crate) struct Entry {
    pub(crate) value: String,
    pub(crate) ttl: Duration,
    pub(crate) inserted_at: u64,
}

impl Redis {
    pub(crate) async fn new(
        config: Config,
        clock: Arc<dyn Clock>,
        r#async: Arc<r#async::Async>,
        random: Arc<dyn RandomGenerator>,
    ) -> Arc<Self> {
        let redis = Arc::new(Self {
            config,
            clock,
            random,
            entries: Mutex::new(HashMap::new()),
        });

        let r#async_clone = Arc::clone(&r#async);
        r#async
            .spawn(cleaner(r#async_clone, Arc::downgrade(&redis)))
            .await;

        redis
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
    async fn lock(&self, lock: &Lock, ttl: Duration) -> Result<()> {
        if self.random.gen_bool(self.config.lock_failure_chance) {
            bail!(
                "simulated: redis_server={} redis server failed before trying to lock key",
                self.config.id
            )
        }

        let mut entries = self.entries.lock().await;

        if entries.get(&lock.key).is_some() {
            return Err(anyhow!("key is already set"));
        }

        entries.insert(
            lock.key.clone(),
            Entry {
                value: lock.value(),
                ttl,
                inserted_at: self.clock.now(),
            },
        );

        if self
            .random
            .gen_bool(self.config.lock_response_message_loss_chance)
        {
            bail!(
                "simulated: redis_server={} lock acquired but response message is will be lost",
                self.config.id
            )
        }

        Ok(())
    }

    async fn release_lock(&self, lock: &Lock) -> Result<()> {
        if self
            .random
            .gen_bool(self.config.try_relase_lock_failure_chance)
        {
            bail!(
                "simulated: redis_server={} redis server failed before trying to release lock",
                self.config.id
            )
        }

        let mut entries = self.entries.lock().await;

        if let Some(entry) = entries.get(&lock.key) {
            println!(
                "aaaaaa redis_server={} release_lock entry.value == lock.value() {:?}",
                self.config.id,
                entry.value == lock.value()
            );
            if entry.value == lock.value() {
                entries.remove(&lock.key);
            }
        }

        if self.random.gen_bool(self.config.message_loss_chance) {
            bail!("simulated: redis_server={} released lock but redis serve wasn't able to send a response",self.config.id)
        }

        Ok(())
    }

    fn address(&self) -> &str {
        &self.config.address
    }
}

/// Calls a method to remove expired entries from the fake redis periodically.
async fn cleaner(r#async: Arc<r#async::Async>, redis: Weak<Redis>) {
    loop {
        match redis.upgrade() {
            None => {
                error!("cleaner: redis Arc has been dropped, will exit cleaner loop");
            }
            Some(redis) => {
                redis.remove_expired_entries().await;
            }
        }
        r#async.sleep(Duration::from_millis(1)).await;
    }
}
