use std::sync::Mutex;

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

pub trait RandomGenerator: std::fmt::Debug + Send + Sync {
    fn gen_u64(&self) -> u64;
}

#[derive(Debug)]
pub struct ChaCha8RandomGenerator {
    rng: Mutex<ChaCha8Rng>,
}

impl ChaCha8RandomGenerator {
    pub fn new() -> Self {
        Self::from_seed(rand::thread_rng().gen())
    }

    pub fn from_seed(seed: u64) -> Self {
        Self {
            rng: Mutex::new(ChaCha8Rng::seed_from_u64(seed)),
        }
    }
}

impl RandomGenerator for ChaCha8RandomGenerator {
    fn gen_u64(&self) -> u64 {
        let mut rng = self.rng.lock().unwrap();
        rng.gen()
    }
}
