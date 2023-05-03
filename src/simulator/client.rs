use std::{pin::Pin, sync::Arc};

use futures::{Future, StreamExt};
use tokio::sync::Mutex;

use crate::{Lock, RedLock};

use super::r#async::{Async, Stepper, StepperOutput};

/// Calls `RedLock` methods. It exists to aid in simulations.

pub(crate) struct Client<'a> {
    redlock: RedLock<Async>,
    state: Mutex<State<'a>>,
}

enum State<'a> {
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

impl<'a> Client<'a> {
    pub(crate) fn new(redlock: RedLock<Async>) -> Self {
        Self {
            redlock,
            state: Mutex::new(State::Initial),
        }
    }

    pub(crate) async fn has_acquired_lock(&self) -> bool {
        let state = self.state.lock().await;
        // TODO: check if lock is expired.
        matches!(*state, State::LockAcquired { .. })
    }

    pub(crate) async fn step(self: Arc<Self>, key: String) {
        let mut state = self.state.lock().await;

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
                // TODO: chance of unlocking
            }
        }
    }
}
