use std::{sync::Arc, time::Duration};

use futures::{FutureExt, StreamExt};
use rand::Rng;

use crate::{random::ChaCha8RandomGenerator, RedLock};

use self::{
    r#async::{AsyncRuntime, Stepper},
    shuffle_iterator::ShuffleIterator,
};
mod r#async;
mod clock;
mod redis;
mod shuffle_iterator;

pub(crate) struct Config {
    pub(crate) simulation: SimulationConfig,
    pub(crate) redis: RedisConfig,
    pub(crate) clients: ClientsConfig,
}

pub(crate) struct SimulationConfig {
    /// How many steps the simulation should take.
    pub(crate) steps: usize,
}

pub(crate) struct RedisConfig {
    // TODO: delays, message loss, etc
    /// The number of redis servers in the cluster.
    pub(crate) num_servers: usize,
    /// The chance of a call to `Redis::lock` failing.
    pub(crate) lock_failure_chance: usize,
    /// The chance of a call to `Redis::release_lock` failing.
    pub(crate) relase_lock_failure_chance: usize,
}

pub(crate) struct ClientsConfig {
    // TODO: delays, message loss, etc
    /// The number of clients that will try to acquire locks in the redis cluster.
    pub(crate) num_clients: usize,
}

pub(crate) async fn run<F>(config: Config, mut assertion_fn: F)
where
    F: FnMut(&[Arc<dyn crate::Redis>], &[RedLock<r#async::Async>]),
{
    let seed: u64 = rand::thread_rng().gen();

    let async_runtime = Arc::new(AsyncRuntime::new());

    // Create several independent redis servers.
    let redis_servers: Vec<_> = (0..config.redis.num_servers)
        .map(|i| {
            let redis: Arc<dyn crate::Redis> = Arc::new(redis::Redis::new(redis::Config {
                address: format!("127.0.0.1:500{i}"),
                lock_failure_chance: config.redis.lock_failure_chance,
                relase_lock_failure_chance: config.redis.relase_lock_failure_chance,
            }));
            redis
        })
        .collect();

    // Client several independent clients that communicate with the same set of redis servers.
    let clients: Vec<RedLock<r#async::Async>> = (0..config.clients.num_clients)
        .map(|i| {
            RedLock::new(
                crate::Config {
                    node_id: i.to_string(),
                    max_lock_retry_interval: Duration::from_secs(15),
                    acquire_lock_timeout: Duration::from_millis(50),
                    release_lock_timeout: Duration::from_millis(250),
                    lock_ttl: Duration::from_secs(30),
                    clock_drift: Duration::from_millis(500),
                    min_lock_validity: Duration::from_secs(5),
                },
                Arc::new(ChaCha8RandomGenerator::from_seed(seed)),
                redis_servers.clone(),
                Arc::new(clock::Clock::new()),
                Arc::new(r#async::Async::new(Arc::clone(&async_runtime))),
            )
        })
        .collect();

    let futures: Vec<_> = clients
        .iter()
        .map(|client| Stepper::new(Box::pin(client.retry_until_locked("key".to_string()))))
        .collect();

    let mut iterator =
        ShuffleIterator::new(Arc::new(ChaCha8RandomGenerator::from_seed(seed)), futures);

    for _ in 0..config.simulation.steps {
        let mut stepper = iterator.next_mut().unwrap();
        if stepper.completed {
            println!("aaaaaa skipping, stepper.completed {:?}", stepper.completed);
            continue;
        }
        // TODO: should not poll again when a future completes.
        let result = stepper.next().await;

        dbg!(&result);

        async_runtime.step_all_tasks().await;

        // tokio::task::yield_now().await;
        // Assert invariants hold.
        assertion_fn(&redis_servers, &clients);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn simulate() {
        let config = Config {
            simulation: SimulationConfig { steps: 10 },
            redis: RedisConfig {
                num_servers: 5,
                lock_failure_chance: 5,
                relase_lock_failure_chance: 5,
            },
            clients: ClientsConfig { num_clients: 1 },
        };

        run(config, |_redis_servers, _clients| {}).await;
    }
}
