use std::{sync::Arc, time::Duration};

use clock::Clock;
use random::RandomGenerator;
use redis::Redis;
use tracing::error;
mod r#async;
mod clock;
mod random;
mod redis;
#[cfg(test)]
mod simulator;

#[derive(Debug)]
pub struct RedLock<Async>
where
    Async: r#async::Async,
{
    config: Config,
    /// A random number generator.
    random: Arc<dyn RandomGenerator>,
    /// List of redis servers used to acquire a lock.
    redis_servers: Vec<Arc<dyn Redis>>,
    /// Can be used to get the current time.
    clock: Arc<dyn Clock>,
    /// Contains functions to deal with async code.
    r#async: Async,
}

#[derive(Debug)]
pub struct Config {
    /// The id of this node.
    /// Must be unique in the set of clients contacting the same set of redis servers.
    pub node_id: String,
    /// The client will wait between 0 and `max_lock_retry_interval` before retrying to acquire a lock after failing to acquire it.
    pub max_lock_retry_interval: Duration,
    /// The amount of time to wait for a response for a acquire lock request sent to a redis server.
    pub acquire_lock_timeout: Duration,
    /// The amount of time to wait for a response for a release lock request sent to a redis server.
    pub release_lock_timeout: Duration,
    /// The amount of time that an acquired lock is valid for.
    pub lock_ttl: Duration,
    /// The amount of clock drift to subtract from the lock validity time.
    pub clock_drift: Duration,
    /// The minimum lock validity time to consider a lock acquired.
    pub min_lock_validity: Duration,
}

/// An opaque value that can be used by the client to unlock a key.
#[derive(Debug, Clone)]
pub struct Lock {
    /// The key given by the client.
    key: String,
    /// The id of this node. Must be unique between every client using the same redis servers.
    node_id: String,
    /// A random number.
    random_number: u64,
}

impl Lock {
    /// Returns a value that can be used to set as the value of an acquired locked.
    fn value(&self) -> String {
        format!("{}:{}", self.node_id, self.random_number)
    }
}

impl<Async> RedLock<Async>
where
    Async: r#async::Async + 'static,
{
    pub fn new(
        config: Config,
        random: Arc<dyn RandomGenerator>,
        redis_servers: Vec<Arc<dyn Redis>>,
        clock: Arc<dyn Clock>,
        r#async: Async,
    ) -> Self {
        assert!(
            config.lock_ttl > config.min_lock_validity,
            "the lock ttl must be greater than the minimum lock validity time"
        );
        assert!(
            config.lock_ttl > config.clock_drift,
            "the lock ttl must be greater than the total clock drift time"
        );

        Self {
            config,
            random,
            redis_servers,
            clock,
            r#async,
        }
    }

    fn majority(&self) -> usize {
        self.redis_servers.len() / 2 + 1
    }

    pub async fn retry_until_locked(&self, key: String) -> Lock {
        let lock = Lock {
            key,
            node_id: self.config.node_id.clone(),
            random_number: self.random.gen_u64(),
        };

        loop {
            // If the lock has been acquired in the majority of servers, just return the lock.
            if self.lock(&lock).await {
                return lock;
            }

            // The lock wasn't acquired on the majority, so we need to release the lock
            // on the servers that we are able to acquire the lock.
            // Note that we try to release the lock from every server because
            // we may have acquired a lock but didn't receive a response.
            if !self.release(&lock).await {
                error!("unable to release acquired locks");
            }

            // TODO: add jitter
            self.r#async
                .sleep(self.config.max_lock_retry_interval)
                .await;
        }
    }

    /// Tries to acquire a lock on at least the majority of servers.
    async fn lock(&self, lock: &Lock) -> bool {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(self.redis_servers.len());

        let _task_handle = tokio::spawn({
            let futures = self.redis_servers.iter().cloned().map(|redis_server| {
                let sender = sender.clone();
                let lock = lock.clone();
                let lock_ttl = self.config.lock_ttl;

                self.r#async
                    .clone()
                    .timeout(self.config.acquire_lock_timeout, async move {
                        if let Err(err) = redis_server.lock(&lock, lock_ttl).await {
                            error!(?err, "error calling set_nx_px");
                        };

                        if let Err(err) = sender.send(true).await {
                            error!(
                            ?err,
                            redis_server = redis_server.address(),
                            "acquire lock on redis server but got error sending signal to channel"
                        );
                        };
                    })
            });

            futures::future::join_all(futures)
        });

        let start = self.clock.now();

        let mut locks_acquired = 0;
        while receiver.recv().await.is_some() {
            locks_acquired += 1;
            println!("aaaaaa locks_acquired {:?}", locks_acquired);

            if locks_acquired >= self.majority() {
                let time_passed = self.clock.now() - start;

                let validity_time = self.config.lock_ttl.as_millis() as i64
                    - time_passed as i64
                    - self.config.clock_drift.as_millis() as i64;

                dbg!(&validity_time);

                // If we took too long to acquire the lock on
                // the majority of servers, the lock is not valid.
                return validity_time >= self.config.min_lock_validity.as_millis() as i64;
            }
        }

        false
    }

    /// Releases the lock. Returns `true` when the lock is released
    /// at least in the majority of servers.
    async fn release(&self, lock: &Lock) -> bool {
        let futures = self.redis_servers.iter().cloned().map(|redis_server| {
            tokio::time::timeout(self.config.release_lock_timeout, async move {
                if let Err(err) = redis_server.release_lock(lock).await {
                    error!(?err, "error calling release_lock");
                };
            })
        });

        let results = futures::future::join_all(futures).await;

        let release_count = results.into_iter().filter(|result| result.is_ok()).count();

        release_count >= self.majority()
    }
}