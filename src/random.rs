use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::sync::Mutex;

pub trait RandomGenerator: std::fmt::Debug + Send + Sync {
    // Generates a number between `start` and `end`
    fn gen_in_range(&self, start: u64, end: u64) -> u64;

    /// Generates a bool with `probability` of it being true.
    fn gen_bool(&self, probability: f64) -> bool;

    // Generates a u64.
    fn gen_u64(&self) -> u64;
}

#[derive(Debug)]
pub struct ChaCha8RandomGenerator {
    rng: Mutex<ChaCha8Rng>,
}

impl ChaCha8RandomGenerator {
    pub fn from_seed(seed: u64) -> Self {
        Self {
            rng: Mutex::new(ChaCha8Rng::seed_from_u64(seed)),
        }
    }
}

impl RandomGenerator for ChaCha8RandomGenerator {
    fn gen_in_range(&self, start: u64, end: u64) -> u64 {
        let mut rng = self.rng.lock().unwrap();
        rng.gen_range(start..end)
    }
    fn gen_u64(&self) -> u64 {
        let mut rng = self.rng.lock().unwrap();
        rng.gen()
    }

    fn gen_bool(&self, probability: f64) -> bool {
        let mut rng = self.rng.lock().unwrap();
        rng.gen_bool(probability)
    }
}
