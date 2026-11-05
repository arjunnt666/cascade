//! One JSON-lines file per run under a directory.
//!
//! Appends are a single `write_all` followed by `sync_data`. If the process
//! dies mid-write the last line can be partial; on load that line is
//! dropped and the file truncated back to the last full event, the same way
//! a WAL handles a torn tail. Loaded logs are cached, so a run's file is
//! read once per process.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use cascade_core::{CascadeError, HistoryEvent, Result, RunId};
use parking_lot::Mutex;
use tracing::warn;

use crate::{check_sequence, AppendResult, HistoryBackend, Snapshot};

pub struct FileHistoryStore {
    dir: PathBuf,
    cache: Mutex<HashMap<RunId, Vec<HistoryEvent>>>,
}

impl FileHistoryStore {
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            cache: Mutex::new(HashMap::new()),
        })
    }

    fn log_path(&self, run: &RunId) -> PathBuf {
        self.dir.join(format!("{run}.jsonl"))
    }

    fn snapshot_path(&self, run: &RunId) -> PathBuf {
        self.dir.join(format!("{run}.snapshot.json"))
    }

    /// Make sure `run` is in the cache. Returns false if it has no file.
    fn load_into(
        &self,
        cache: &mut HashMap<RunId, Vec<HistoryEvent>>,
        run: &RunId,
    ) -> Result<bool> {
        if cache.contains_key(run) {
            return Ok(true);
        }
        let path = self.log_path(run);
        if !path.exists() {
            return Ok(false);
        }
        cache.insert(*run, read_log(&path)?);
        Ok(true)
    }
}

fn read_log(path: &Path) -> Result<Vec<HistoryEvent>> {
    let data = std::fs::read(path)?;
    let mut events = Vec::new();
    let mut good = 0usize;
    for line in data.split_inclusive(|&b| b == b'\n') {
        if !line.ends_with(b"\n") {
            break;
        }
        match serde_json::from_slice::<HistoryEvent>(line) {
            Ok(ev) if ev.sequence == events.len() as u64 => {
                events.push(ev);
                good += line.len();
            }
            _ => break,
        }
    }
    if good < data.len() {
        warn!(
            log = %path.display(),
            dropped_bytes = data.len() - good,
            "partial history tail, truncating"
        );
        let f = OpenOptions::new().write(true).open(path)?;
        f.set_len(good as u64)?;
        f.sync_all()?;
    }
    Ok(events)
}

#[async_trait::async_trait]
impl HistoryBackend for FileHistoryStore {
    async fn append(&self, run_id: &RunId, events: Vec<HistoryEvent>) -> Result<AppendResult> {
        let mut cache = self.cache.lock();
        self.load_into(&mut cache, run_id)?;
        let log = cache.entry(*run_id).or_default();
        let first = log.len() as u64;
        check_sequence(first, &events)?;

        let mut buf = Vec::new();
        for e in &events {
            serde_json::to_writer(&mut buf, e)?;
            buf.push(b'\n');
        }
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.log_path(run_id))?;
        f.write_all(&buf)?;
        f.sync_data()?;

        let count = events.len();
        log.extend(events);
        Ok(AppendResult::new(first, count))
    }

    async fn get_events(&self, run_id: &RunId, from_seq: u64) -> Result<Vec<HistoryEvent>> {
        let mut cache = self.cache.lock();
        if !self.load_into(&mut cache, run_id)? {
            return Err(CascadeError::RunNotFound(run_id.to_string()));
        }
        Ok(cache[run_id]
            .iter()
            .skip(from_seq as usize)
            .cloned()
            .collect())
    }

    async fn list_runs(&self) -> Result<Vec<RunId>> {
        let mut runs = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let name = entry?.file_name();
            let name = name.to_string_lossy();
            if let Some(id) = name.strip_suffix(".jsonl").and_then(|s| s.parse().ok()) {
                runs.push(id);
            }
        }
        runs.sort();
        Ok(runs)
    }

    async fn get_latest_snapshot(&self, run_id: &RunId) -> Result<Option<Snapshot>> {
        match std::fs::read(self.snapshot_path(run_id)) {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn save_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        let path = self.snapshot_path(&snapshot.run_id);
        let tmp = path.with_extension("json.tmp");
        {
            let mut f = File::create(&tmp)?;
            serde_json::to_writer(&mut f, &snapshot)?;
            f.sync_all()?;
        }
        std::fs::rename(tmp, path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{marker, started};
    use cascade_core::WorkflowId;

    #[tokio::test]
    async fn survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let (run, wf) = (RunId::new(), WorkflowId::new());
        {
            let s = FileHistoryStore::open(dir.path()).unwrap();
            s.append(&run, vec![started(run, wf, 0)]).await.unwrap();
            s.append(&run, vec![marker(run, wf, 1), marker(run, wf, 2)])
                .await
                .unwrap();
        }
        let s = FileHistoryStore::open(dir.path()).unwrap();
        assert_eq!(s.list_runs().await.unwrap(), [run]);
        assert_eq!(s.get_events(&run, 0).await.unwrap().len(), 3);
        assert_eq!(s.get_events(&run, 2).await.unwrap()[0].sequence, 2);
    }

    #[tokio::test]
    async fn torn_tail_is_truncated_and_appends_continue() {
        let dir = tempfile::tempdir().unwrap();
        let (run, wf) = (RunId::new(), WorkflowId::new());
        let path = {
            let s = FileHistoryStore::open(dir.path()).unwrap();
            s.append(&run, vec![started(run, wf, 0)]).await.unwrap();
            s.log_path(&run)
        };
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(br#"{"id":"half a line"#).unwrap();
        drop(f);

        let s = FileHistoryStore::open(dir.path()).unwrap();
        assert_eq!(s.get_events(&run, 0).await.unwrap().len(), 1);
        s.append(&run, vec![marker(run, wf, 1)]).await.unwrap();
        drop(s);
        let s = FileHistoryStore::open(dir.path()).unwrap();
        assert_eq!(s.get_events(&run, 0).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn snapshot_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let s = FileHistoryStore::open(dir.path()).unwrap();
        let run = RunId::new();
        assert!(s.get_latest_snapshot(&run).await.unwrap().is_none());
        let snap = Snapshot::new(
            run,
            WorkflowId::new(),
            4,
            cascade_core::WorkflowStatus::Running,
            cascade_core::Payload::empty(),
        );
        s.save_snapshot(snap).await.unwrap();
        let back = s.get_latest_snapshot(&run).await.unwrap().unwrap();
        assert_eq!(back.last_event_sequence, 4);
    }
}
