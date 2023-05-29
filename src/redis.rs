use anyhow::Result;
use async_trait::async_trait;
use std::{net::SocketAddr, time::Duration};

use crate::Lock;

#[async_trait]
pub trait Redis: std::fmt::Debug + Send + Sync {
    /// Returns the address of this redis server.
    fn address(&self) -> SocketAddr;

    /// Acquires a lock if a lock does not already exist for the resource.
    async fn lock(&self, lock: &Lock, ttl: Duration) -> Result<()>;

    /// Releases the lock if the resource value matches `value`.
    async fn release_lock(&self, lock: &Lock) -> Result<()>;
}
