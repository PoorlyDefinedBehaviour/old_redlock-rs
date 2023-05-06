use std::{sync::Arc, time::Duration};

use rand::Rng;

use crate::{
    clock::Clock,
    random::{ChaCha8RandomGenerator, RandomGenerator},
    redis::Redis,
    RedLock,
};

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
    pub(crate) lock_failure_chance: f64,
    /// The chance of a call to `Redis::release_lock` failing.
    pub(crate) try_relase_lock_failure_chance: f64,
    /// The chance of an ack being lost.
    pub(crate) message_loss_chance: f64,
    /// The chance of the message saying that a call to .lock() succeed being lost.
    pub(crate) lock_response_message_loss_chance: f64,
    /// The minimum amount of time in ms that a call to lock() will take.
    pub(crate) min_lock_delay: u64,
    /// The maximum amount of time in ms that a call to lock() will take.
    pub(crate) max_lock_delay: u64,
    /// The minimum amount of time in ms that a call to release_lock() will take.
    pub(crate) min_release_lock_delay: u64,
    /// The maximum amount of time in ms that a call to release_lock() will take.
    pub(crate) max_release_lock_delay: u64,
}

pub(crate) struct ClientsConfig {
    // TODO: delays, message loss, etc
    /// The number of clients that will try to acquire locks in the redis cluster.
    pub(crate) num_clients: usize,
    /// The chance of a client trying to release a lock after acquiring it.
    pub(crate) lock_release_chance: f64,
}

pub(crate) struct ClockConfig {}

#[derive(Clone, Copy)]
pub(crate) struct AssertionInput<'a, 'b> {
    clocks: &'a [Arc<clock::Clock>],
    redis_servers: &'a [Arc<redis::Redis>],
    clients: &'a [Arc<Client<'b>>],
}

impl<'a, 'b> AssertionInput<'a, 'b> {
    /// Returns the clients that are holding locks at the moment.
    /// Clients with expired locks are excluded.
    fn clients_holding_locks(&self) -> Vec<Arc<Client<'b>>> {
        self.clients
            .iter()
            .filter(|client| client.has_acquired_lock())
            .map(Arc::clone)
            .collect()
    }
}

pub(crate) async fn run<F>(config: Config, mut assertion_fn: F)
where
    F: FnMut(AssertionInput),
{
    let seed: u64 = rand::thread_rng().gen();

    let random: Arc<dyn RandomGenerator> = Arc::new(ChaCha8RandomGenerator::from_seed(seed));

    let async_runtime = Arc::new(AsyncRuntime::new(Arc::clone(&random)));

    let mut clocks = vec![Arc::new(clock::Clock::new())];

    let r#async = Arc::new(r#async::Async::new(
        Arc::clone(&clocks[0]),
        Arc::clone(&async_runtime),
    ));

    // Create several independent redis servers.
    let mut redis_servers = Vec::new();
    for i in 0..config.redis.num_servers {
        let redis = redis::Redis::new(
            redis::Config {
                id: i,
                address: format!("127.0.0.1:500{i}"),
                lock_failure_chance: config.redis.lock_failure_chance,
                try_relase_lock_failure_chance: config.redis.try_relase_lock_failure_chance,
                message_loss_chance: config.redis.message_loss_chance,
                lock_response_message_loss_chance: config.redis.lock_response_message_loss_chance,
                min_lock_delay: config.redis.min_lock_delay,
                max_lock_delay: config.redis.max_lock_delay,
                min_release_lock_delay: config.redis.min_release_lock_delay,
                max_release_lock_delay: config.redis.max_release_lock_delay,
            },
            {
                let clock = Arc::new(clock::Clock::new());
                clocks.push(Arc::clone(&clock));
                clock
            },
            Arc::clone(&r#async),
            Arc::clone(&random),
        )
        .await;
        redis_servers.push(redis);
    }

    // Client several independent clients that communicate with the same set of redis servers.
    let mut clients: Vec<_> = (0..config.clients.num_clients)
        .map(|i| {
            Arc::new(Client::new(
                client::Config {
                    id: i,
                    lock_release_chance: config.clients.lock_release_chance,
                },
                {
                    let clock = Arc::new(clock::Clock::new());
                    clocks.push(Arc::clone(&clock));
                    clock
                },
                RedLock::new(
                    crate::Config {
                        node_id: i.to_string(),
                        max_lock_retry_interval: Duration::from_secs(15),
                        acquire_lock_timeout: Duration::from_millis(50),
                        release_lock_timeout: Duration::from_millis(250),
                        lock_ttl: Duration::from_secs(30),
                        min_lock_validity: Duration::from_secs(5),
                        clock_drift: Duration::from_millis(500),
                    },
                    Arc::clone(&random),
                    redis_servers
                        .clone()
                        .into_iter()
                        .map(|redis| redis as Arc<dyn Redis>)
                        .collect(),
                    {
                        let clock = Arc::new(clock::Clock::new());
                        clocks.push(Arc::clone(&clock));
                        clock
                    },
                    Arc::clone(&r#async),
                ),
                Arc::clone(&random),
            ))
        })
        .collect();

    let mut iterator = ShuffleIterator::new(Arc::clone(&random), &mut clients);

    for _ in 0..config.simulation.steps {
        let client = iterator.next_mut().cloned().unwrap();

        client.step("key".to_string()).await;

        async_runtime.step_all_tasks().await;

        for clock in clocks.iter() {
            clock.step();
        }

        // Assert invariants hold.
        assertion_fn(AssertionInput {
            clocks: &clocks,
            redis_servers: &redis_servers,
            clients: iterator.items,
        });
    }
}

#[cfg(test)]
mod tests {
    use crate::Lock;

    use super::*;

    /// Ensures that a lock is acquired by at least client eventually.
    struct LivelockCheck {
        /// The amount of times it checked if any locks have been acquired by one of the clients.
        checks: usize,
        /// The locks that have been acquired over time.
        locks: Vec<Lock>,
    }

    impl LivelockCheck {
        fn new() -> Self {
            Self {
                checks: 0,
                locks: Vec::new(),
            }
        }

        /// Returns true when no clients have been able to acquire a lock.
        fn is_livelocked(&mut self, input: AssertionInput) -> bool {
            self.checks += 1;

            for client in input.clients_holding_locks() {
                self.locks.push(client.get_lock().unwrap());
            }

            if self.checks % 10_000 == 0 {
                return self
                    .locks
                    .last()
                    .map(|lock| input.clocks[0].now() - lock.acquired_at > 100_000)
                    .unwrap_or(false);
            }

            false
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn simulate() {
        let config = Config {
            simulation: SimulationConfig { steps: 100_000 },
            redis: RedisConfig {
                num_servers: 3,
                lock_failure_chance: 0.1,
                try_relase_lock_failure_chance: 0.1,
                message_loss_chance: 0.1,
                lock_response_message_loss_chance: 0.05,
                min_lock_delay: 0,
                max_lock_delay: 1000,
                min_release_lock_delay: 0,
                max_release_lock_delay: 1000,
            },
            clients: ClientsConfig {
                num_clients: 3,
                lock_release_chance: 0.1,
            },
            clock: ClockConfig {},
        };

        let mut livelock_check = LivelockCheck::new();

        run(config, |input| {
            assert_mutual_exclusion(input);

            let _ = livelock_check.is_livelocked(input);
        })
        .await;

        dbg!(&livelock_check.locks.len());
    }

    /// Asserts that the lock is not held by more than one client.
    fn assert_mutual_exclusion(input: AssertionInput) {
        assert!(input.clients_holding_locks().len() <= 1);
    }
}
