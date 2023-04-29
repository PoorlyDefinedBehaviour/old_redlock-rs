use std::time::SystemTime;

/// A fake clock that can be used in simulations.
#[derive(Debug)]
pub(crate) struct Clock {}

impl Clock {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl crate::Clock for Clock {
    fn now(&self) -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}
