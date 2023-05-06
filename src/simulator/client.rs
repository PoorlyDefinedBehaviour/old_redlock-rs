use std::{pin::Pin, sync::Arc};

use futures::{Future, StreamExt};
use std::sync::Mutex;

use crate::{clock::Clock, random::RandomGenerator, Lock, RedLock};

use super::{
    clock,
    r#async::{Async, Stepper, StepperOutput},
};

#[derive(Debug)]
pub(crate) struct Config {
    /// The client id.
    pub(crate) id: usize,
    pub(crate) lock_release_chance: f64,
}

/// Calls `RedLock` methods. It exists to aid in simulations.
#[derive(Debug)]
pub(crate) struct Client<'a> {
    pub(crate) config: Config,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) redlock: RedLock<Async>,
    pub(crate) random: Arc<dyn RandomGenerator>,
    pub(crate) state: Mutex<State<'a>>,
}

pub(crate) enum State<'a> {
    /// Client has just been instantiated.
    Initial,
    /// Client is trying to acquire a lock.
    AcquireLock {
        /// Stepper for the `retry_until_locked` future.
        stepper: Stepper<Pin<Box<dyn Future<Output = Lock> + 'a>>>,
    },
    /// Client has acquired a lock.
    LockAcquired {
        /// The lock this client has just acquired.
        lock: Lock,
    },
}

impl<'a> std::fmt::Debug for State<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Initial => write!(f, "Initial"),
            Self::AcquireLock { .. } => f
                .debug_struct("AcquireLock")
                .field("stepper", &"Stepper Future")
                .finish(),
            Self::LockAcquired { lock } => {
                f.debug_struct("LockAcquired").field("lock", lock).finish()
            }
        }
    }
}

impl<'a> Client<'a> {
    pub(crate) fn new(
        config: Config,
        clock: Arc<clock::Clock>,
        redlock: RedLock<Async>,
        random: Arc<dyn RandomGenerator>,
    ) -> Self {
        Self {
            config,
            clock,
            redlock,
            random,
            state: Mutex::new(State::Initial),
        }
    }

    /// Returns true when the client holds a non expired lock.
    pub(crate) fn has_acquired_lock(&self) -> bool {
        let state = self.state.lock().unwrap();

        match &*state {
            State::Initial | State::AcquireLock { .. } => false,
            State::LockAcquired { lock } => !lock.is_expired(self.clock.now()),
        }
    }

    /// Returns the lock being held by this client if there's one.
    pub(crate) fn get_lock(&self) -> Option<Lock> {
        let state = self.state.lock().unwrap();
        match &*state {
            State::Initial | State::AcquireLock { .. } => None,
            State::LockAcquired { lock } => Some(lock.clone()),
        }
    }

    pub(crate) async fn step(self: Arc<Self>, key: String) {
        let mut state = self.state.lock().unwrap();

        // We know that the client will exist during the whole simulation
        // and we need to store a future that borrows itself.
        let s = self.as_ref() as *const Self as *mut Self;

        match &mut *state {
            State::Initial => {
                let future: Pin<Box<dyn Future<Output = Lock> + 'a>> =
                    Box::pin(unsafe { (*s).redlock.retry_until_locked(key) });

                *state = State::AcquireLock {
                    stepper: Stepper::new(future),
                }
            }
            State::AcquireLock { ref mut stepper } => {
                if let Some(StepperOutput::Ready(lock)) = stepper.next().await {
                    *state = State::LockAcquired { lock };
                }
            }
            State::LockAcquired { lock } => {
                if self.random.gen_bool(self.config.lock_release_chance) {
                    if self.redlock.release(lock).await {
                        *state = State::Initial;
                    }
                }
            }
        }
    }
}
