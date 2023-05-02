use async_trait::async_trait;

use futures::{Stream, StreamExt};
use std::future::Future;
use std::pin::Pin;

use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tokio::sync::Mutex;

pub(crate) struct AsyncRuntime {
    /// Futures that the runtime needs to execute.
    futures: Mutex<Vec<Stepper<Pin<Box<dyn Future<Output = ()> + Send>>>>>,
}

impl AsyncRuntime {
    pub(crate) fn new() -> Self {
        Self {
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
        println!("aaaaaa will poll {} futures", futures.len());

        let mut completed_futures = Vec::new();

        for (i, mut stepper) in futures.iter_mut().enumerate() {
            // TODO: don't poll completed futures;
            let stepper_output = stepper.next().await;
            if let Some(StepperOutput::Ready(_value)) = stepper_output {
                completed_futures.push(i);
            }
        }

        // This is slow but it is fine for testing.
        for i in completed_futures {
            futures.remove(i);
        }
    }
}

/// Provides async utilities that can be used during the simulation.
pub(crate) struct Async {
    runtime: Arc<AsyncRuntime>,
}

impl Async {
    pub(crate) fn new(runtime: Arc<AsyncRuntime>) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl crate::r#async::Async for Async {
    async fn sleep(&self, _duration: Duration) {
        // Resolve immediately.
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
        println!("aaaaaa simulator spawn called",);
        self.runtime.add_future(future).await;
    }
}

pub(crate) struct Stepper<F>
where
    F: Future + Unpin,
{
    pub(crate) completed: bool,
    pub(crate) future: F,
}

impl<F> Stepper<F>
where
    F: Future + Unpin,
{
    pub(crate) fn new(future: F) -> Self {
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

impl<F> Stream for &mut Stepper<F>
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
