//! A Tantivy [`Directory`] that reads a published index and never writes to it.
//!
//! Tantivy's reader takes the index meta lock while it opens segment readers,
//! so that a concurrent writer's garbage collection cannot delete the segments
//! mid-open. A published hdtc text index has no writer — it is built once and
//! then immutable by contract — so that lock guards nothing, and taking it is
//! the *only* write a reader performs: [`MmapDirectory::acquire_lock`] opens
//! `.tantivy-meta.lock` with `write(true).create(true)`, which fails with
//! `EROFS` on a read-only mount and `EACCES` on a directory whose mode bits
//! forbid writing.
//!
//! That single write is enough to make a text index unservable from the places
//! an immutable artifact belongs: a `readOnly` volume, a bucket mirror, a
//! snapshot. So the read path opens the index through this wrapper, which
//! answers the lock itself and refuses every write. Refusing rather than
//! delegating is deliberate: a future code path that tries to write through a
//! reader's directory fails loudly here instead of quietly mutating a
//! published artifact.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tantivy::Directory;
use tantivy::directory::error::{
    DeleteError, LockError, OpenDirectoryError, OpenReadError, OpenWriteError,
};
use tantivy::directory::{
    DirectoryLock, FileHandle, FileSlice, Lock, MmapDirectory, WatchCallback, WatchHandle, WritePtr,
};

/// An [`MmapDirectory`] that reads, and fails every attempt to write.
#[derive(Clone, Debug)]
pub(crate) struct ReadOnlyDirectory {
    inner: MmapDirectory,
    root: PathBuf,
}

impl ReadOnlyDirectory {
    /// Open the index directory at `path` for reading.
    ///
    /// Opening memory-maps nothing yet and requires no write permission on
    /// `path` or anything in it.
    pub(crate) fn open(path: &Path) -> Result<Self, OpenDirectoryError> {
        Ok(Self {
            inner: MmapDirectory::open(path)?,
            root: path.to_path_buf(),
        })
    }

    /// The error every write path returns, naming the file that was refused.
    fn refusal(&self, path: &Path) -> io::Error {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "refusing to write {} in the text index {}, which is opened read-only",
                path.display(),
                self.root.display()
            ),
        )
    }
}

impl Directory for ReadOnlyDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        self.inner.get_file_handle(path)
    }

    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        self.inner.open_read(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.inner.exists(path)
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.inner.atomic_read(path)
    }

    fn watch(&self, callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.inner.watch(callback)
    }

    /// The point of the type: no lock file, ever.
    ///
    /// A lock is only meaningful against a concurrent writer, and this
    /// directory cannot be written through. The returned guard owns nothing
    /// and deletes nothing when it drops.
    fn acquire_lock(&self, _lock: &Lock) -> Result<DirectoryLock, LockError> {
        Ok(DirectoryLock::from(Box::new(())))
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        Err(OpenWriteError::wrap_io_error(
            self.refusal(path),
            path.to_path_buf(),
        ))
    }

    fn atomic_write(&self, path: &Path, _data: &[u8]) -> io::Result<()> {
        Err(self.refusal(path))
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        Err(DeleteError::IoError {
            io_error: Arc::new(self.refusal(path)),
            filepath: path.to_path_buf(),
        })
    }

    /// Nothing was written, so there is nothing to make durable.
    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::text::schema::{
        FIELD_LANG, FIELD_OBJECT, FIELD_TEXT, register_tokenizer, text_schema,
    };
    use tantivy::directory::META_LOCK;
    use tantivy::{Index, IndexWriter, ReloadPolicy, TantivyDocument};

    /// Build a one-document index in `dir`, the shape a published one has.
    fn build_index(dir: &Path) {
        let index = Index::create_in_dir(dir, text_schema()).expect("create index");
        register_tokenizer(&index);
        let schema = index.schema();
        let mut writer: IndexWriter<TantivyDocument> = index
            .writer_with_num_threads(1, 15 * 1024 * 1024)
            .expect("index writer");
        let mut document = TantivyDocument::new();
        document.add_text(schema.get_field(FIELD_TEXT).unwrap(), "atrazine");
        document.add_u64(schema.get_field(FIELD_OBJECT).unwrap(), 7);
        document.add_text(schema.get_field(FIELD_LANG).unwrap(), "en");
        writer.add_document(document).expect("add document");
        writer.commit().expect("commit");
        drop(writer);
        drop(index);
        // The build leaves its own lock files behind; clear them so a later
        // assertion about lock files is about the read path's behaviour.
        for entry in std::fs::read_dir(dir).expect("read index directory") {
            let entry = entry.expect("read index entry");
            if entry.file_name().to_string_lossy().ends_with(".lock") {
                std::fs::remove_file(entry.path()).expect("remove build lock file");
            }
        }
    }

    /// Drop write permission from `dir` and everything in it, and report
    /// whether the mode bits actually bite. Root ignores them, and a test that
    /// assumed otherwise would pass without testing anything.
    fn make_read_only(dir: &Path) -> bool {
        set_mode(dir, 0o444, 0o555);
        let probe = dir.join(".write-probe");
        match std::fs::File::create(&probe) {
            Ok(_) => {
                let _ = std::fs::remove_file(&probe);
                false
            }
            Err(_) => true,
        }
    }

    fn restore_write(dir: &Path) {
        set_mode(dir, 0o644, 0o755);
    }

    #[cfg(unix)]
    fn set_mode(dir: &Path, file_mode: u32, dir_mode: u32) {
        use std::os::unix::fs::PermissionsExt;
        for entry in std::fs::read_dir(dir).expect("read index directory") {
            let entry = entry.expect("read index entry");
            std::fs::set_permissions(entry.path(), PermissionsExt::from_mode(file_mode))
                .expect("set file permissions");
        }
        std::fs::set_permissions(dir, PermissionsExt::from_mode(dir_mode))
            .expect("set directory permissions");
    }

    #[cfg(not(unix))]
    fn set_mode(dir: &Path, _file_mode: u32, _dir_mode: u32) {
        let read_only = _file_mode & 0o200 == 0;
        for entry in std::fs::read_dir(dir).expect("read index directory") {
            let entry = entry.expect("read index entry");
            let mut permissions = entry.metadata().expect("read metadata").permissions();
            permissions.set_readonly(read_only);
            std::fs::set_permissions(entry.path(), permissions).expect("set file permissions");
        }
    }

    /// The type's contract, independent of Tantivy's reader: the lock is
    /// answered without a file, reads work, and every write is refused.
    #[test]
    fn a_read_only_directory_reads_and_refuses_every_write() {
        let temp = tempfile::tempdir().unwrap();
        build_index(temp.path());
        if !make_read_only(temp.path()) {
            restore_write(temp.path());
            eprintln!("skipped: mode bits do not restrict this user");
            return;
        }

        let directory = ReadOnlyDirectory::open(temp.path()).expect("open read-only directory");
        let meta = Path::new("meta.json");
        assert!(directory.exists(meta).expect("meta.json exists"));
        assert!(
            !directory
                .atomic_read(meta)
                .expect("read meta.json")
                .is_empty()
        );

        // The lock leaves no file behind, which is what read-only needs.
        let lock = directory
            .acquire_lock(&META_LOCK)
            .expect("acquire the meta lock");
        assert!(!temp.path().join(&META_LOCK.filepath).exists());
        drop(lock);

        let scratch = Path::new("scratch");
        assert!(directory.open_write(scratch).is_err());
        assert!(directory.atomic_write(scratch, b"x").is_err());
        assert!(directory.delete(meta).is_err());
        directory.sync_directory().expect("nothing to sync");

        // Refused, not merely failed by the filesystem: the message names the
        // file and the index, so a stray write is diagnosable.
        let message = directory
            .atomic_write(scratch, b"x")
            .unwrap_err()
            .to_string();
        assert!(message.contains("scratch"), "{message}");
        assert!(message.contains("read-only"), "{message}");

        restore_write(temp.path());
    }

    /// The defect itself: constructing a reader takes the meta lock, which on a
    /// read-only directory used to fail before a single query ran.
    #[test]
    fn an_index_opens_and_answers_from_a_read_only_directory() {
        let temp = tempfile::tempdir().unwrap();
        build_index(temp.path());
        if !make_read_only(temp.path()) {
            restore_write(temp.path());
            eprintln!("skipped: mode bits do not restrict this user");
            return;
        }

        let index = Index::open(ReadOnlyDirectory::open(temp.path()).expect("open directory"))
            .expect("open index read-only");
        register_tokenizer(&index);
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .expect("open reader read-only");
        let reader: tantivy::IndexReader = reader;
        assert_eq!(reader.searcher().num_docs(), 1);
        assert!(!temp.path().join(&META_LOCK.filepath).exists());

        restore_write(temp.path());
    }
}
