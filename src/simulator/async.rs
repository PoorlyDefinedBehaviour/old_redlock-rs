use async_trait::async_trait;
use futures::Stream;
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use crate::Lock;

/// Provides async utilities that can be used during the simulation.
#[derive(Debug, Clone)]
pub(crate) struct Async {}

impl Async {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl crate::r#async::Async for Async {
    async fn sleep(&self, _duration: Duration) {
        // Resolve immediately.
    }

    async fn timeout<F>(
        self,
        _duration: Duration,
        future: F,
    ) -> Result<F::Output, tokio::time::error::Elapsed>
    where
        F: Future + Send,
    {
        // TODO: timeout chance
        Ok(future.await)
    }
}

pub(crate) struct Stepper<F>
where
    F: Future<Output = Lock> + Unpin,
{
    pub(crate) completed: bool,
    pub(crate) future: F,
}

impl<F> Stepper<F>
where
    F: Future<Output = Lock> + Unpin,
{
    pub(crate) fn new(future: F) -> Self {
        Self {
            completed: false,
            future,
        }
    }
}

#[derive(Debug)]
pub(crate) enum StepperOutput {
    Ready(Lock),
    NotReady,
}

impl<F> Stream for &mut Stepper<F>
where
    F: Future<Output = Lock> + Unpin,
{
    type Item = StepperOutput;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        println!(
            "aaaaaa Stepper.poll_next: self.completed {:?}",
            self.completed
        );
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
