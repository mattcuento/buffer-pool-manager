
//! A placeholder for a real disk manager.
use super::api::{PageId, PAGE_SIZE};
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt; // Using positioned I/O for better concurrency
use std::sync::Mutex;

/// Manages reading and writing pages to a file on disk.
/// This implementation uses positioned I/O (`read_at`, `write_at`) to allow
/// multiple concurrent_buffer_pool_manager reads and writes without a global lock on the file.
#[derive(Debug)]
pub struct DiskManager {
    db_file: File, // No Mutex needed for I/O, only for allocating new pages
    next_page_id: Mutex<PageId>,
}

impl DiskManager {
    /// Creates a new DiskManager for a given database file.
    pub fn new(db_file_path: &str, direct_io: bool) -> io::Result<Self> {
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
                // FILE_FLAG_NO_BUFFERING
                options.flags(0x20000000);
            }
        }

        let file = options.open(db_file_path)?;

        if direct_io {
            #[cfg(target_os = "macos")]
            {
                use std::os::unix::io::AsRawFd;
                let fd = file.as_raw_fd();
                unsafe {
                    if libc::fcntl(fd, libc::F_NOCACHE, 1) == -1 {
                        return Err(io::Error::last_os_error());
                    }
                }
            }
        }

        let metadata = file.metadata()?;
        let next_page_id = (metadata.len() / PAGE_SIZE as u64) as PageId;

        Ok(Self {
            db_file: file,
            next_page_id: Mutex::new(next_page_id),
        })
    }

    /// Reads a page from the database file into the provided buffer using positioned I/O.
    pub fn read_page(&self, page_id: PageId, data: &mut [u8]) -> io::Result<()> {
        let offset = (page_id * PAGE_SIZE) as u64;
        self.db_file.read_exact_at(data, offset)
    }

    /// Writes a page from the buffer into the database file using positioned I/O.
    pub fn write_page(&self, page_id: PageId, data: &[u8]) -> io::Result<()> {
        let offset = (page_id * PAGE_SIZE) as u64;
        self.db_file.write_all_at(data, offset)
    }

    /// Allocates a new page ID.
    pub fn allocate_page(&self) -> PageId {
        let mut next_page_id = self.next_page_id.lock().unwrap();
        let page_id = *next_page_id;
        *next_page_id += 1;
        page_id
    }
}
