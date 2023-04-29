pub trait Clock: std::fmt::Debug + Send + Sync {
    /// Returns the current time in milliseconds.
    fn now(&self) -> u64;
}
