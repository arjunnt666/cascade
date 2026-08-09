use std::collections::BinaryHeap;
use std::cmp::Ordering;

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use cascade_core::{Result, TimerId};

use crate::{TimerEntry, TimerService};

#[derive(Debug)]
struct HeapEntry {
    fire_at: DateTime<Utc>,
    entry: TimerEntry,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.fire_at == other.fire_at
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other.fire_at.cmp(&self.fire_at)
    }
}

pub struct TimerWheel {
    heap: Mutex<BinaryHeap<HeapEntry>>,
}

impl TimerWheel {
    pub fn new() -> Self {
        Self {
            heap: Mutex::new(BinaryHeap::new()),
        }
    }
}

impl Default for TimerWheel {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl TimerService for TimerWheel {
    async fn schedule(&self, entry: TimerEntry) -> Result<()> {
        let mut h = self.heap.lock();
        h.push(HeapEntry {
            fire_at: entry.fire_at,
            entry,
        });
        Ok(())
    }

    async fn cancel(&self, id: &TimerId) -> Result<()> {
        let _ = id;
        Ok(())
    }

    async fn poll_due(&self, now: DateTime<Utc>) -> Result<Vec<TimerEntry>> {
        let mut h = self.heap.lock();
        let mut due = Vec::new();
        while let Some(top) = h.peek() {
            if top.fire_at <= now {
                if let Some(e) = h.pop() {
                    due.push(e.entry);
                }
            } else {
                break;
            }
        }
        Ok(due)
    }
}
