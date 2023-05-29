use std::{net::SocketAddr, pin::Pin, task::Poll};

use crate::message_bus::MessageBus;
use anyhow::Result;
use async_trait::async_trait;
use futures::Future;

#[derive(Debug)]
pub(crate) struct SimulatorMessageBus {}

#[async_trait]
impl MessageBus for SimulatorMessageBus {
    async fn send_message(
        &self,
        redis_server_addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(SendMessage {})
    }
}

struct SendMessage {}

impl Future for SendMessage {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        todo!()
    }
}
