use std::{
    collections::HashMap,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use common::file_slice::FileSlice;

use super::Directory;

// RoRamDirectory is a read only directory that stores data in RAM.
#[derive(Clone)]
struct RoRamDirectory {
    inner: Arc<RwLock<RoRamDirectoryInner>>,
}

impl RoRamDirectory {
    fn new(dir: &Path) -> Result<RoRamDirectory, std::io::Error> {
        Ok(RoRamDirectory {
            inner: Arc::new(RwLock::new(RoRamDirectoryInner::new(dir)?)),
        })
    }
}

impl std::fmt::Debug for RoRamDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoRamDirectory").finish()
    }
}

impl Directory for RoRamDirectory {
    fn get_file_handle(
        &self,
        path: &std::path::Path,
    ) -> Result<std::sync::Arc<dyn common::file_slice::FileHandle>, super::error::OpenReadError>
    {
        let file_slice = self.open_read(path)?;
        Ok(Arc::new(file_slice))
    }

    fn open_read(&self, path: &Path) -> Result<FileSlice, super::error::OpenReadError> {
        self.inner.read().unwrap().open_read(path)
    }

    fn delete(&self, path: &std::path::Path) -> Result<(), super::error::DeleteError> {
        unimplemented!("RoRamDirectory is read-only")
    }

    fn exists(&self, path: &std::path::Path) -> Result<bool, super::error::OpenReadError> {
        Ok(self.inner.read().unwrap().exists(path))
    }

    fn open_write(
        &self,
        path: &std::path::Path,
    ) -> Result<super::WritePtr, super::error::OpenWriteError> {
        unimplemented!("RoRamDirectory is read-only")
    }

    fn atomic_read(&self, path: &std::path::Path) -> Result<Vec<u8>, super::error::OpenReadError> {
        let bytes = self
            .inner
            .read()
            .unwrap()
            .open_read(path)?
            .read_bytes()
            .map_err(|io_error| super::error::OpenReadError::IoError {
                io_error: Arc::new(io_error),
                filepath: path.to_path_buf(),
            })?;
        Ok(bytes.as_slice().to_owned())
    }

    fn atomic_write(&self, path: &std::path::Path, data: &[u8]) -> std::io::Result<()> {
        unimplemented!("RoRamDirectory is read-only")
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        todo!()
    }

    fn watch(&self, watch_callback: super::WatchCallback) -> crate::Result<super::WatchHandle> {
        todo!()
    }
}

struct RoRamDirectoryInner {
    files: HashMap<PathBuf, FileSlice>,
}

impl RoRamDirectoryInner {
    fn new(dir: &Path) -> Result<RoRamDirectoryInner, std::io::Error> {
        // read all files in the directory
        let mut files: HashMap<PathBuf, FileSlice> = HashMap::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if !entry.file_type()?.is_file() {
                warn!("Skipping non-file {:?}", path);
                continue;
            }
            let mut file = File::open(&path)?;
            let mut data = Vec::new();
            file.read_to_end(&mut data)?;
            let file_slice = FileSlice::from(data);
            files.insert(entry.file_name().into(), file_slice);
        }
        Ok(RoRamDirectoryInner { files })
    }

    fn open_read(&self, path: &Path) -> Result<FileSlice, super::error::OpenReadError> {
        self.files
            .get(path)
            .cloned()
            .ok_or(super::error::OpenReadError::FileDoesNotExist(
                path.to_path_buf(),
            ))
    }

    fn exists(&self, path: &Path) -> bool {
        self.files.contains_key(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_ro_ram_directory() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        let file_name = Path::new("test.txt");
        let file_path = dir_path.join(file_name);
        let mut file = File::create(&file_path).unwrap();
        file.write_all(b"hello world").unwrap();

        let ram_dir = RoRamDirectory::new(dir_path).unwrap();

        let file_slice = ram_dir.open_read(&file_name).unwrap();
        assert_eq!(file_slice.read_bytes().unwrap().as_slice(), b"hello world");

        assert!(ram_dir.exists(&file_name).unwrap());
    }
}
