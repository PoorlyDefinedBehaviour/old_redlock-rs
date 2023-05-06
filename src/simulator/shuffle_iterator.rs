use std::sync::Arc;

use crate::random::RandomGenerator;

/// A circular iterator that shuffles the elements at the beginning of each iteration.
pub(crate) struct ShuffleIterator<'a, T> {
    pub(crate) index: usize,
    pub(crate) items: &'a mut [T],
    random: Arc<dyn RandomGenerator>,
}

impl<'a, T> ShuffleIterator<'a, T> {
    pub(crate) fn new(random: Arc<dyn RandomGenerator>, items: &'a mut [T]) -> Self {
        Self {
            index: 0,
            items,
            random,
        }
    }

    pub(crate) fn next_mut(&mut self) -> Option<&mut T> {
        let index = self.index;

        self.index = (1 + self.index) % self.items.len();

        if self.index == 0 {
            shuffle(self.random.as_ref(), self.items);
        }

        self.items.get_mut(index)
    }
}

fn shuffle<T>(random: &dyn RandomGenerator, items: &mut [T]) {
    for i in 0..items.len() {
        let j = random.gen_u64() as usize % items.len();
        items.swap(i, j);
    }
}
