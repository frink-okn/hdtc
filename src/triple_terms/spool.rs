//! The proposition spool: every triple term the parser meets, as it meets it.
//!
//! Parsing cannot tell an asserted triple term from an unasserted one — the
//! triple may be asserted in another file, or later in this one — so the parser
//! only records each occurrence, and the decision is made once the HDT exists
//! ([`super::finish`]). Records are appended by every parser worker through one
//! shared [`PropositionSink`], zstd-compressed, and read back in arbitrary
//! order; duplicates are expected (a triple term reified twice is recorded
//! twice) and removed by the sort that follows.
//!
//! An input without triple terms never touches the sink, so nothing is created.
//!
//! A spool's file is removed when the spool, or the writer still filling it,
//! is dropped: a build that fails part way leaves none behind.

use super::PropositionRecord;
use anyhow::{Context, Result, ensure};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use tempfile::TempPath;

/// Bytes a parser worker buffers before handing its records to the sink.
pub const WORKER_FLUSH_BYTES: usize = 1 << 20;

/// A finished spool on disk, removed when dropped.
#[derive(Debug)]
pub struct PropositionSpool {
    path: TempPath,
    pub records: u64,
}

impl PropositionSpool {
    pub fn reader(&self) -> Result<SpoolReader> {
        SpoolReader::open(&self.path)
    }

    /// Remove the spool now rather than when it is dropped.
    pub fn remove(self) {
        if let Err(error) = self.path.close() {
            tracing::debug!("Failed to remove proposition spool: {error}");
        }
    }
}

/// A spool being written by one owner. Its file is removed if the writer is
/// dropped unfinished.
pub struct SpoolWriter {
    path: TempPath,
    encoder: zstd::Encoder<'static, BufWriter<File>>,
    records: u64,
    scratch: Vec<u8>,
}

impl SpoolWriter {
    pub fn create(path: PathBuf) -> Result<Self> {
        let file =
            File::create(&path).with_context(|| format!("Failed to create {}", path.display()))?;
        // Owned from here, so an error below removes the file as well.
        let path = TempPath::from_path(path);
        let encoder = zstd::Encoder::new(BufWriter::with_capacity(1 << 20, file), 1)?;
        Ok(Self {
            path,
            encoder,
            records: 0,
            scratch: Vec::new(),
        })
    }

    /// Append records already encoded with [`encode_record`].
    pub fn append_encoded(&mut self, encoded: &[u8], records: u64) -> Result<()> {
        self.encoder.write_all(encoded)?;
        self.records += records;
        Ok(())
    }

    pub fn push(&mut self, record: &PropositionRecord) -> Result<()> {
        self.scratch.clear();
        encode_record(&mut self.scratch, record);
        self.encoder.write_all(&self.scratch)?;
        self.records += 1;
        Ok(())
    }

    pub fn finish(self) -> Result<PropositionSpool> {
        let mut inner = self.encoder.finish()?;
        inner.flush()?;
        Ok(PropositionSpool {
            path: self.path,
            records: self.records,
        })
    }
}

/// The shared, lazily created writer the parser workers append to.
pub struct PropositionSink {
    temp_dir: PathBuf,
    name: &'static str,
    state: Mutex<Option<SpoolWriter>>,
}

impl PropositionSink {
    pub fn new(temp_dir: &Path, name: &'static str) -> Self {
        Self {
            temp_dir: temp_dir.to_path_buf(),
            name,
            state: Mutex::new(None),
        }
    }

    /// Append a buffer of encoded records ([`encode_record`]).
    pub fn append(&self, encoded: &[u8], records: u64) -> Result<()> {
        if records == 0 {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("proposition sink mutex poisoned"))?;
        if state.is_none() {
            *state = Some(SpoolWriter::create(self.temp_dir.join(self.name))?);
        }
        state
            .as_mut()
            .expect("initialized above")
            .append_encoded(encoded, records)
    }

    /// Whether any record has been appended.
    pub fn has_records(&self) -> Result<bool> {
        Ok(self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("proposition sink mutex poisoned"))?
            .is_some())
    }

    /// Close the spool, returning it if anything was written.
    pub fn finish(&self) -> Result<Option<PropositionSpool>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("proposition sink mutex poisoned"))?;
        state.take().map(SpoolWriter::finish).transpose()
    }
}

fn put(buf: &mut Vec<u8>, field: &[u8]) {
    buf.extend_from_slice(&(field.len() as u32).to_le_bytes());
    buf.extend_from_slice(field);
}

/// Append one record's encoding: four length-prefixed fields.
pub fn encode_record(buf: &mut Vec<u8>, record: &PropositionRecord) {
    put(buf, record.iri.as_bytes());
    put(buf, record.subject.as_bytes());
    put(buf, record.predicate.as_bytes());
    put(buf, record.object.as_bytes());
}

/// Streams the records of a spool back in write order.
pub struct SpoolReader {
    decoder: zstd::Decoder<'static, BufReader<File>>,
}

impl SpoolReader {
    fn open(path: &Path) -> Result<Self> {
        let file =
            File::open(path).with_context(|| format!("Failed to open {}", path.display()))?;
        Ok(Self {
            decoder: zstd::Decoder::new(file)?,
        })
    }

    fn field(&mut self, first: bool) -> Result<Option<String>> {
        let mut len = [0u8; 4];
        if first {
            let mut read = 0;
            while read < 4 {
                let n = self.decoder.read(&mut len[read..])?;
                if n == 0 {
                    ensure!(read == 0, "truncated proposition spool");
                    return Ok(None);
                }
                read += n;
            }
        } else {
            self.decoder
                .read_exact(&mut len)
                .context("truncated proposition spool")?;
        }
        let mut bytes = vec![0u8; u32::from_le_bytes(len) as usize];
        self.decoder
            .read_exact(&mut bytes)
            .context("truncated proposition spool")?;
        Ok(Some(
            String::from_utf8(bytes).context("non-UTF-8 term in proposition spool")?,
        ))
    }

    pub fn next_record(&mut self) -> Result<Option<PropositionRecord>> {
        let Some(iri) = self.field(true)? else {
            return Ok(None);
        };
        let subject = self.field(false)?.context("truncated proposition spool")?;
        let predicate = self.field(false)?.context("truncated proposition spool")?;
        let object = self.field(false)?.context("truncated proposition spool")?;
        Ok(Some(PropositionRecord {
            iri,
            subject,
            predicate,
            object,
        }))
    }
}

impl Iterator for SpoolReader {
    type Item = Result<PropositionRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_record().transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_round_trip_through_a_spool() {
        let dir = tempfile::tempdir().unwrap();
        let sink = PropositionSink::new(dir.path(), "props.zst");
        assert!(
            sink.finish().unwrap().is_none(),
            "nothing written, nothing created"
        );
        let records: Vec<PropositionRecord> = (0..1000)
            .map(|i| PropositionRecord {
                iri: format!("urn:x-kgf:tt:g:{i:032}"),
                subject: format!("http://ex/s{i}"),
                predicate: "http://ex/p".to_owned(),
                object: format!("\"{i}\"@en"),
            })
            .collect();
        let mut buf = Vec::new();
        for record in &records {
            encode_record(&mut buf, record);
        }
        sink.append(&buf, records.len() as u64).unwrap();
        let spool = sink.finish().unwrap().unwrap();
        assert_eq!(spool.records, 1000);
        let back: Vec<_> = spool.reader().unwrap().collect::<Result<_>>().unwrap();
        assert_eq!(back, records);
    }

    /// A spool's file goes with it: dropped finished or unfinished, as a
    /// failed build drops it, nothing is left in the temp directory.
    #[test]
    fn a_dropped_spool_leaves_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let record = PropositionRecord {
            iri: "urn:x-kgf:tt:g:x".to_owned(),
            subject: "http://ex/s".to_owned(),
            predicate: "http://ex/p".to_owned(),
            object: "http://ex/o".to_owned(),
        };
        let path = dir.path().join("unfinished.zst");
        let mut writer = SpoolWriter::create(path.clone()).unwrap();
        writer.push(&record).unwrap();
        assert!(path.exists());
        drop(writer);
        assert!(!path.exists(), "an unfinished spool was left behind");

        let path = dir.path().join("finished.zst");
        let mut writer = SpoolWriter::create(path.clone()).unwrap();
        writer.push(&record).unwrap();
        let spool = writer.finish().unwrap();
        assert!(path.exists());
        drop(spool);
        assert!(!path.exists(), "a finished spool was left behind");

        let sink = PropositionSink::new(dir.path(), "sink.zst");
        let mut encoded = Vec::new();
        encode_record(&mut encoded, &record);
        sink.append(&encoded, 1).unwrap();
        assert!(dir.path().join("sink.zst").exists());
        drop(sink);
        assert!(!dir.path().join("sink.zst").exists());
    }
}
