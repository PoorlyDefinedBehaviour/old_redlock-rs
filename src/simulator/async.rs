use async_trait::async_trait;

use futures::{Stream, StreamExt};
use std::cmp::Ordering;
use std::future::Future;
use std::pin::Pin;

use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tokio::sync::Mutex;

use crate::clock::Clock;
use crate::random::RandomGenerator;

use super::clock;
use super::shuffle_iterator::ShuffleIterator;

pub(crate) struct AsyncRuntime {
    random: Arc<dyn RandomGenerator>,
    /// Futures that the runtime needs to execute.
    futures: Mutex<Vec<Stepper<Pin<Box<dyn Future<Output = ()> + Send>>>>>,
}

impl std::fmt::Debug for AsyncRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncRuntime")
            .field("futures", &"List of futures")
            .finish()
    }
}

impl AsyncRuntime {
    pub(crate) fn new(random: Arc<dyn RandomGenerator>) -> Self {
        Self {
            random,
            futures: Mutex::new(Vec::new()),
        }
    }

    async fn add_future<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut futures = self.futures.lock().await;
        futures.push(Stepper::new(Box::pin(future)));
    }

    /// Polls every spawned future once.
    pub(crate) async fn step_all_tasks(&self) {
        let mut futures = self.futures.lock().await;
        let num_futures = futures.len();

        let mut iterator = ShuffleIterator::new(Arc::clone(&self.random), &mut futures);

        let mut completed_futures = Vec::new();

        for i in 0..num_futures {
            let stepper = iterator.next_mut().unwrap();
            // TODO: don't poll completed futures;
            let stepper_output = stepper.next().await;
            if let Some(StepperOutput::Ready(_value)) = stepper_output {
                completed_futures.push(i);
            }
        }

        // This is slow but it is fine for testing.
        // Order from greatest to smallest.
        completed_futures.sort_by(|a, b| {
            if a > b {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        });
        for i in completed_futures {
            futures.remove(i);
        }
    }
}

/// Provides async utilities that can be used during the simulation.
#[derive(Debug)]
pub(crate) struct Async {
    clock: Arc<clock::Clock>,
    runtime: Arc<AsyncRuntime>,
}

impl Async {
    pub(crate) fn new(clock: Arc<clock::Clock>, runtime: Arc<AsyncRuntime>) -> Self {
        Self { clock, runtime }
    }
}

pub(crate) struct Sleep {
    /// When this Sleep was created.
    created_at: u64,
    /// How long to sleep for.
    duration: Duration,
    /// Can be used to get the time.
    clock: Arc<clock::Clock>,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        if self.clock.now() - self.created_at > self.duration.as_millis() as u64 {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

#[async_trait]
impl crate::r#async::Async for Async {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(Sleep {
            clock: Arc::clone(&self.clock),
            created_at: self.clock.now(),
            duration,
        })
    }

    async fn timeout<F>(
        self: Arc<Self>,
        _duration: Duration,
        future: F,
    ) -> Result<F::Output, tokio::time::error::Elapsed>
    where
        F: Future + Send,
    {
        // TODO: timeout chance
        Ok(future.await)
    }

    /// Spawns a future that will be executed by the runtime.
    async fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.runtime.add_future(future).await;
    }
}

pub(crate) struct Stepper<F>
where
    F: Future,
{
    pub(crate) completed: bool,
    pub(crate) future: F,
}

impl<F> Stepper<Pin<Box<F>>>
where
    F: Future + ?Sized,
{
    pub(crate) fn new(future: Pin<Box<F>>) -> Self {
        Self {
            completed: false,
            future,
        }
    }
}

#[derive(Debug)]
pub(crate) enum StepperOutput<T> {
    Ready(T),
    NotReady,
}

impl<F> Stream for Stepper<F>
where
    F: Future + Unpin,
{
    type Item = StepperOutput<F::Output>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.completed {
            return Poll::Ready(Some(StepperOutput::NotReady));
        }

        match Pin::new(&mut self.future).poll(cx) {
            Poll::Pending => Poll::Ready(Some(StepperOutput::NotReady)),
            Poll::Ready(v) => {
                self.completed = true;
                Poll::Ready(Some(StepperOutput::Ready(v)))
            }
        }
    }
}
