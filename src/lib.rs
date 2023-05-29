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
    r#async: Arc<Async>,
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
    /// The lock ttl.
    ttl: u64,
    /// When the lock was acquired.
    acquired_at: u64,
}

impl Lock {
    /// Returns a value that can be used to set as the value of an acquired locked.
    fn value(&self) -> String {
        format!("{}:{}", self.node_id, self.random_number)
    }

    /// Returns `true` when the lock is expired.
    pub fn is_expired(&self, current_time: u64) -> bool {
        current_time - self.acquired_at > self.ttl
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
        r#async: Arc<Async>,
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
        let mut lock = Lock {
            key,
            node_id: self.config.node_id.clone(),
            random_number: self.random.gen_u64(),
            ttl: 0,
            acquired_at: 0,
        };

        loop {
            // If the lock has been acquired in the majority of servers, just return the lock.
            if self.lock(&mut lock).await {
                println!("aaaaaa LOCK ACQUIRED lock={:?}", lock);
                return lock;
            }

            // The lock wasn't acquired on the majority, so we need to release the lock
            // on the servers that we are able to acquire the lock.
            // Note that we try to release the lock from every server because
            // we may have acquired a lock but didn't receive a response.
            if !self.release(&lock).await {
                error!("unable to release acquired locks");
            }

            let duration = Duration::from_millis(
                self.random
                    .gen_in_range(0, self.config.max_lock_retry_interval.as_millis() as u64),
            );

            self.r#async.sleep(duration).await;
        }
    }

    /// Tries to acquire a lock on at least the majority of servers.
    async fn lock(&self, lock: &mut Lock) -> bool {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(self.redis_servers.len());

        let mut futures = Vec::with_capacity(self.redis_servers.len());

        for redis_server in self.redis_servers.iter() {
            let redis_server = redis_server.clone();
            let sender = sender.clone();
            let lock = lock.clone();
            let lock_ttl = self.config.lock_ttl;
            let r#async = Arc::clone(&self.r#async);
            let node_id = self.config.node_id.clone();

            futures.push(
                r#async.timeout(self.config.acquire_lock_timeout, async move {
                    println!("aaaaaa client={} will call redis_server.lock()", node_id);
                    if let Err(err) = redis_server.lock(&lock, lock_ttl).await {
                        error!(?err, "error calling set_nx_px");
                        return;
                    };
                    println!("aaaaaa client={} redis_server.lock() returned", node_id);

                    if let Err(err) = sender.send(true).await {
                        error!(
                            ?err,
                            "acquired lock on redis server but got error sending signal to channel"
                        );
                    };
                }),
            );
        }

        self.r#async
            .spawn(async {
                futures::future::join_all(futures).await;
            })
            .await;

        let start = self.clock.now();

        let mut locks_acquired = 0;

        // Drop the first sender that was created so receiver.recv() does not block forever
        // because there's one Sender that has not been dropped.
        drop(sender);
        while receiver.recv().await.is_some() {
            locks_acquired += 1;

            if locks_acquired >= self.majority() {
                let time_passed = self.clock.now() - start;

                let validity_time = self.config.lock_ttl.as_millis() as i64
                    - time_passed as i64
                    - self.config.clock_drift.as_millis() as i64;

                // If we took too long to acquire the lock on
                // the majority of servers, the lock is not valid.
                if validity_time < self.config.min_lock_validity.as_millis() as i64 {
                    println!("aaaaaa client={} acquire lock on majority but the lock is too old to be valid", self.config.node_id);
                    return false;
                }

                lock.ttl = validity_time as u64;
                lock.acquired_at = start;

                return true;
            }
        }

        println!(
            "aaaaaa client {} did not acquire lock, lock() returning false",
            self.config.node_id
        );
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
