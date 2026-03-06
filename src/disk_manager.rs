
//! Disk manager with per-table segment files.
//!
//! Each table has its own subdirectory under the database directory, with data
//! stored in 1 GB segment files (`0.seg`, `1.seg`, ...). Page IDs are composite:
//! high 32 bits = table_id, low 32 bits = local page number within the table.
use super::api::{local_page_of, make_page_id, table_id_of, PageId, PAGE_SIZE, PAGES_PER_SEGMENT};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

/// Trait for disk manager operations to enable testing and mocking.
pub trait DiskManagerTrait: Send + Sync + std::fmt::Debug {
    /// Reads a page from disk into the provided buffer.
    fn read_page(&self, page_id: PageId, data: &mut [u8]) -> io::Result<()>;

    /// Writes a page from the buffer to disk.
    fn write_page(&self, page_id: PageId, data: &[u8]) -> io::Result<()>;

    /// Allocates a new page ID for the given table.
    fn allocate_page(&self, table_id: u32) -> PageId;
}

/// Per-table file state: open segment handles and page allocation counter.
#[derive(Debug)]
struct TableFiles {
    dir: PathBuf,
    segments: Mutex<HashMap<usize, Arc<File>>>,
    next_local_page: Mutex<u32>,
}

/// Manages reading and writing pages across per-table segment files.
///
/// Each table's pages live in `<db_dir>/<table_name>/<n>.seg`. Page IDs encode
/// the table owner in the high 32 bits, so a single global buffer pool can
/// unambiguously cache pages from any table.
#[derive(Debug)]
pub struct DiskManager {
    db_dir: PathBuf,
    tables: RwLock<HashMap<u32, TableFiles>>,
    direct_io: bool,
}

impl DiskManager {
    /// Creates a new DiskManager for the given database directory.
    pub fn new(db_dir: &Path, direct_io: bool) -> io::Result<Self> {
        std::fs::create_dir_all(db_dir)?;
        Ok(Self {
            db_dir: db_dir.to_path_buf(),
            tables: RwLock::new(HashMap::new()),
            direct_io,
        })
    }

    /// Registers a table so pages can be allocated and routed for it.
    ///
    /// Creates the table directory if it doesn't exist, then scans existing
    /// segment files to determine the next available local page number.
    pub fn register_table(&self, table_id: u32, table_name: &str) -> io::Result<()> {
        let dir = self.db_dir.join(table_name);
        std::fs::create_dir_all(&dir)?;

        // Count pages across all existing segments to resume allocation correctly.
        let mut total_pages = 0u32;
        let mut seg_idx = 0usize;
        loop {
            let seg_path = dir.join(format!("{}.seg", seg_idx));
            if !seg_path.exists() {
                break;
            }
            let pages = (std::fs::metadata(&seg_path)?.len() / PAGE_SIZE as u64) as u32;
            total_pages += pages;
            seg_idx += 1;
        }
        // Local page 0 is reserved; allocation always starts at 1.
        let next_local_page = total_pages.max(1);

        self.tables.write().unwrap().insert(
            table_id,
            TableFiles {
                dir,
                segments: Mutex::new(HashMap::new()),
                next_local_page: Mutex::new(next_local_page),
            },
        );
        Ok(())
    }

    /// Removes a table's registration and deletes its data directory.
    pub fn drop_table(&self, table_id: u32) -> io::Result<()> {
        let mut tables = self.tables.write().unwrap();
        if let Some(table_files) = tables.remove(&table_id) {
            std::fs::remove_dir_all(&table_files.dir)?;
        }
        Ok(())
    }

    /// Opens (or returns a cached handle to) the segment file at `seg_idx`.
    fn open_segment(table_files: &TableFiles, seg_idx: usize, direct_io: bool) -> io::Result<Arc<File>> {
        let mut segments = table_files.segments.lock().unwrap();
        if let Some(file) = segments.get(&seg_idx) {
            return Ok(file.clone());
        }

        let path = table_files.dir.join(format!("{}.seg", seg_idx));
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);

        if direct_io {
            #[cfg(target_os = "linux")]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.custom_flags(libc::O_DIRECT);
            }
            #[cfg(target_os = "windows")]
            {
                use std::os::windows::fs::OpenOptionsExt;
                options.flags(0x20000000);
            }
        }

        let file = options.open(&path)?;

        if direct_io {
            #[cfg(target_os = "macos")]
            {
                set_file_nocache(&file)?;
            }
        }

        let file = Arc::new(file);
        segments.insert(seg_idx, file.clone());
        Ok(file)
    }
}

impl DiskManagerTrait for DiskManager {
    fn read_page(&self, page_id: PageId, data: &mut [u8]) -> io::Result<()> {
        let (file, offset) = {
            let table_id = table_id_of(page_id);
            let local_page = local_page_of(page_id) as usize;
            let seg_idx = local_page / PAGES_PER_SEGMENT;
            let local_offset = ((local_page % PAGES_PER_SEGMENT) * PAGE_SIZE) as u64;

            let tables = self.tables.read().unwrap();
            let table_files = tables.get(&table_id).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("table_id {} not registered", table_id),
                )
            })?;
            let file = Self::open_segment(table_files, seg_idx, self.direct_io)?;
            (file, local_offset)
        };
        file.read_exact_at(data, offset)
    }

    fn write_page(&self, page_id: PageId, data: &[u8]) -> io::Result<()> {
        let (file, offset) = {
            let table_id = table_id_of(page_id);
            let local_page = local_page_of(page_id) as usize;
            let seg_idx = local_page / PAGES_PER_SEGMENT;
            let local_offset = ((local_page % PAGES_PER_SEGMENT) * PAGE_SIZE) as u64;

            let tables = self.tables.read().unwrap();
            let table_files = tables.get(&table_id).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("table_id {} not registered", table_id),
                )
            })?;
            let file = Self::open_segment(table_files, seg_idx, self.direct_io)?;
            (file, local_offset)
        };
        file.write_all_at(data, offset)
    }

    fn allocate_page(&self, table_id: u32) -> PageId {
        let local_page = {
            let tables = self.tables.read().unwrap();
            let table_files = tables
                .get(&table_id)
                .unwrap_or_else(|| panic!("table_id {} not registered", table_id));
            let mut next = table_files.next_local_page.lock().unwrap();
            let lp = *next;
            *next += 1;
            lp
        };
        make_page_id(table_id, local_page)
    }
}

#[cfg(target_os = "macos")]
fn set_file_nocache(file: &File) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    let fd = file.as_raw_fd();
    // SAFETY: valid fd from a live File, F_NOCACHE is a valid macOS fcntl command.
    unsafe {
        if libc::fcntl(fd, libc::F_NOCACHE, 1) == -1 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}
