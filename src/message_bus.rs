use std::net::SocketAddr;

use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait MessageBus: Send + Sync + std::fmt::Debug {
    async fn send_message(&self, redis_server_addr: SocketAddr) -> Result<()>;
}
