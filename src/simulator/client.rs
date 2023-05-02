use std::pin::Pin;

use futures::{Future, StreamExt};

use crate::{Lock, RedLock};

use super::r#async::{Async, Stepper, StepperOutput};

/// Calls `RedLock` methods. It exists to aid in simulations.

pub(crate) struct Client<'a> {
    redlock: RedLock<Async>,
    state: State<'a>,
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
            state: State::Initial,
        }
    }

    pub(crate) async fn step(&'a mut self, key: String) {
        match &mut self.state {
            State::Initial => {
                let future: Pin<Box<dyn Future<Output = Lock> + 'a>> =
                    Box::pin(self.redlock.retry_until_locked(key));

                self.state = State::AcquireLock {
                    stepper: Stepper::new(future),
                }
            }
            State::AcquireLock { ref mut stepper } => {
                if let Some(StepperOutput::Ready(lock)) = stepper.next().await {
                    self.state = State::LockAcquired { lock };
                }
            }
            State::LockAcquired { lock } => {
                // TODO: chance of unlocking
            }
        }
    }
}
