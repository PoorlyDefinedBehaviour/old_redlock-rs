use std::{sync::Arc, time::Duration};

use rand::Rng;

use crate::{clock::Clock, random::ChaCha8RandomGenerator, redis::Redis, RedLock};

use self::{client::Client, r#async::AsyncRuntime, shuffle_iterator::ShuffleIterator};
mod r#async;
mod client;
mod clock;
mod redis;
mod shuffle_iterator;

pub(crate) struct Config {
    pub(crate) simulation: SimulationConfig,
    pub(crate) redis: RedisConfig,
    pub(crate) clients: ClientsConfig,
    pub(crate) clock: ClockConfig,
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

pub(crate) struct ClockConfig {}

pub(crate) struct AssertionInput<'a> {
    redis_servers: &'a [Arc<dyn Redis>],
    clients: &'a [Arc<Client<'a>>],
}

pub(crate) async fn run<F>(config: Config, assertion_fn: F)
where
    F: for<'a> Fn(AssertionInput<'a>),
{
    let seed: u64 = rand::thread_rng().gen();

    let async_runtime = Arc::new(AsyncRuntime::new());
    let clock = Arc::new(clock::Clock::new());

    // Create several independent redis servers.
    let redis_servers: Vec<_> = (0..config.redis.num_servers)
        .map(|i| {
            let redis: Arc<dyn crate::Redis> = Arc::new(redis::Redis::new(
                redis::Config {
                    address: format!("127.0.0.1:500{i}"),
                    lock_failure_chance: config.redis.lock_failure_chance,
                    relase_lock_failure_chance: config.redis.relase_lock_failure_chance,
                },
                Arc::clone(&clock) as Arc<dyn Clock>,
            ));
            redis
        })
        .collect();

    // Client several independent clients that communicate with the same set of redis servers.
    let clients: Vec<_> = (0..config.clients.num_clients)
        .map(|i| {
            Arc::new(Client::new(RedLock::new(
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
                Arc::clone(&(clock.clone() as Arc<dyn Clock>)),
                Arc::new(r#async::Async::new(Arc::clone(&async_runtime))),
            )))
        })
        .collect();

    let mut iterator =
        ShuffleIterator::new(Arc::new(ChaCha8RandomGenerator::from_seed(seed)), clients);

    assertion_fn(AssertionInput {
        redis_servers: &redis_servers,
        clients: &iterator.items,
    });
    for _ in 0..config.simulation.steps {
        // let client = iterator.next_cloned().unwrap();

        // client.step("key".to_string()).await;

        async_runtime.step_all_tasks().await;

        clock.step();

        // tokio::task::yield_now().await;
        // Assert invariants hold.
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
            clients: ClientsConfig { num_clients: 3 },
            clock: ClockConfig {},
        };

        run(config, assert_mutual_exclusion).await;
    }

    /// Asserts that the lock is not held by more than one client.
    fn assert_mutual_exclusion(input: AssertionInput) {
        dbg!(&input.redis_servers);
        // let clients_with_locks_acquired = futures::future::join_all(
        //     input
        //         .clients
        //         .iter()
        //         .map(|client| client.has_acquired_lock()),
        // )
        // .await
        // .into_iter()
        // .filter(|acquired| *acquired)
        // .count();

        // assert!(clients_with_locks_acquired <= 1);
    }
}
