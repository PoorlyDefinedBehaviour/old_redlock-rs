//! Contains functions to run code async.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::Future;

#[async_trait]
pub trait Async: Send + Sync {
    /// Returns a future that will resolve after `duration`.
    async fn sleep(&self, duration: Duration);

    /// Returns an error if the future does not complete before `t`.
    async fn timeout<F>(
        self: Arc<Self>,
        duration: Duration,
        future: F,
    ) -> Result<F::Output, tokio::time::error::Elapsed>
    where
        F: Future + Send;

    /// Spawns a future that will be executed by the runtime.
    async fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static;
}
