//! Contains functions to run code async.

use std::time::Duration;

use async_trait::async_trait;
use futures::Future;

#[async_trait]
pub trait Async: std::fmt::Debug + Send + Clone {
    /// Returns a future that will resolve after `duration`.
    async fn sleep(&self, duration: Duration);

    /// Returns an error if the future does not complete before `t`.
    async fn timeout<F>(
        self,
        duration: Duration,
        future: F,
    ) -> Result<F::Output, tokio::time::error::Elapsed>
    where
        F: Future + Send;
}
