use std::sync::atomic::{AtomicU64, Ordering};

/// A fake clock that can be used in simulations.
#[derive(Debug)]
pub(crate) struct SimulatorClock {
    time: AtomicU64,
}

impl SimulatorClock {
    pub(crate) fn new() -> Self {
        Self {
            time: AtomicU64::new(0),
        }
    }

    /// Advance time by 1 unit.
    pub(crate) fn step(&self) {
        self.time.fetch_add(1, Ordering::SeqCst);
    }
}

impl crate::Clock for SimulatorClock {
    fn now(&self) -> u64 {
        self.time.load(Ordering::SeqCst)
    }
}
