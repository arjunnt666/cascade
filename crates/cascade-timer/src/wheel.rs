use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};

use cascade_core::{Result, TimerId};
use chrono::{DateTime, Utc};
use parking_lot::Mutex;

use crate::{TimerEntry, TimerService};

/// Min-heap of timers keyed on fire time. Named "wheel" for historical
/// reasons; it's a binary heap, O(log n) schedule and pop.
///
/// Cancel is lazy. Each schedule bumps a generation for the id, and heap
/// entries whose generation is no longer current are dropped when they
/// reach the top.
#[derive(Default)]
pub struct TimerWheel {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    heap: BinaryHeap<Reverse<Slot>>,
    /// Current generation per live timer.
    live: HashMap<TimerId, u64>,
    next_gen: u64,
}

#[derive(Debug)]
struct Slot(TimerEntry, u64);

impl PartialEq for Slot {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Slot {}
impl PartialOrd for Slot {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Slot {
    // Tie-break on id so equal fire times pop in a stable order.
    fn cmp(&self, other: &Self) -> Ordering {
        (self.0.fire_at, self.0.id).cmp(&(other.0.fire_at, other.0.id))
    }
}

impl TimerWheel {
    pub fn new() -> Self {
        Self::default()
    }

    /// Timers scheduled and not yet fired or cancelled.
    pub fn len(&self) -> usize {
        self.inner.lock().live.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Fire time of the next live timer, for sleeping until then.
    pub fn next_deadline(&self) -> Option<DateTime<Utc>> {
        let mut g = self.inner.lock();
        g.skip_cancelled();
        g.heap.peek().map(|Reverse(s)| s.0.fire_at)
    }
}

impl Inner {
    fn skip_cancelled(&mut self) {
        while let Some(Reverse(top)) = self.heap.peek() {
            if self.live.get(&top.0.id) == Some(&top.1) {
                break;
            }
            self.heap.pop();
        }
    }
}

#[async_trait::async_trait]
impl TimerService for TimerWheel {
    async fn schedule(&self, entry: TimerEntry) -> Result<()> {
        let mut g = self.inner.lock();
        // Rescheduling the same id replaces the old one.
        g.next_gen += 1;
        let gen = g.next_gen;
        g.live.insert(entry.id, gen);
        g.heap.push(Reverse(Slot(entry, gen)));
        Ok(())
    }

    async fn cancel(&self, id: &TimerId) -> Result<()> {
        let mut g = self.inner.lock();
        g.live.remove(id);
        Ok(())
    }

    async fn poll_due(&self, now: DateTime<Utc>) -> Result<Vec<TimerEntry>> {
        let mut g = self.inner.lock();
        let mut due = Vec::new();
        loop {
            g.skip_cancelled();
            match g.heap.peek() {
                Some(Reverse(top)) if top.0.fire_at <= now => {}
                _ => break,
            }
            let Reverse(Slot(entry, _)) = g.heap.pop().expect("peeked");
            g.live.remove(&entry.id);
            due.push(entry);
        }
        Ok(due)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cascade_core::RunId;
    use chrono::Duration;

    fn entry(at: DateTime<Utc>) -> TimerEntry {
        TimerEntry {
            id: TimerId::new(),
            run_id: RunId::new(),
            fire_at: at,
        }
    }

    #[tokio::test]
    async fn pops_in_fire_order_and_only_when_due() {
        let w = TimerWheel::new();
        let t0 = Utc::now();
        let (a, b, c) = (
            entry(t0 + Duration::seconds(2)),
            entry(t0),
            entry(t0 + Duration::seconds(1)),
        );
        for e in [&a, &b, &c] {
            w.schedule(e.clone()).await.unwrap();
        }
        let due = w.poll_due(t0 + Duration::seconds(1)).await.unwrap();
        assert_eq!(due.iter().map(|e| e.id).collect::<Vec<_>>(), [b.id, c.id]);
        assert_eq!(w.len(), 1);
        assert_eq!(w.next_deadline(), Some(a.fire_at));
    }

    #[tokio::test]
    async fn cancelled_timers_never_fire() {
        let w = TimerWheel::new();
        let t0 = Utc::now();
        let (a, b) = (entry(t0), entry(t0 + Duration::seconds(1)));
        w.schedule(a.clone()).await.unwrap();
        w.schedule(b.clone()).await.unwrap();
        w.cancel(&a.id).await.unwrap();
        assert_eq!(w.next_deadline(), Some(b.fire_at));
        let due = w.poll_due(t0 + Duration::seconds(5)).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].id, b.id);
        assert!(w.is_empty());
    }

    #[tokio::test]
    async fn reschedule_replaces() {
        let w = TimerWheel::new();
        let t0 = Utc::now();
        let mut e = entry(t0);
        w.schedule(e.clone()).await.unwrap();
        e.fire_at = t0 + Duration::seconds(10);
        w.schedule(e.clone()).await.unwrap();
        assert!(w
            .poll_due(t0 + Duration::seconds(1))
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            w.poll_due(t0 + Duration::seconds(10)).await.unwrap().len(),
            1
        );

        // and moving it earlier works too
        e.fire_at = t0 + Duration::seconds(10);
        w.schedule(e.clone()).await.unwrap();
        e.fire_at = t0;
        w.schedule(e.clone()).await.unwrap();
        assert_eq!(w.poll_due(t0).await.unwrap().len(), 1);
        assert!(w
            .poll_due(t0 + Duration::seconds(20))
            .await
            .unwrap()
            .is_empty());
    }
}
