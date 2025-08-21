use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Debug, Clone)]
pub struct IdGenerator {
    counter: Arc<AtomicU64>,
}

impl IdGenerator {
    pub fn new() -> Self {
        Self {
            counter: Arc::new(AtomicU64::new(0u64)),
        }
    }

    pub fn next_id(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::SeqCst)
    }
}
