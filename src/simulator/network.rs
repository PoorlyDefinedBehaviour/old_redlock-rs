use std::{net::SocketAddr, sync::Arc};

use super::clock::SimulatorClock;

pub(crate) struct SimulatorNetwork {
    clock: Arc<SimulatorClock>,
}

impl SimulatorNetwork {
    pub fn new(clock: Arc<SimulatorClock>) -> Self {
        Self { clock }
    }

    pub fn send_message(&mut self, from: SocketAddr, to: SocketAddr) -> Result<()> {}
}
