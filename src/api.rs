
//! Defines the common API for all buffer pool manager implementations.
use std::ops::{Deref, DerefMut};
use std::fmt::Debug;


/// A unique identifier for a page in the database.
///
/// Composite encoding: high 32 bits = table_id, low 32 bits = local page number.
/// `INVALID_PAGE_ID = 0` is always invalid since valid table IDs start at 1.
pub type PageId = usize;

/// A constant to represent an invalid page ID.
pub const INVALID_PAGE_ID: PageId = 0;

/// The size of a single page in bytes.
pub const PAGE_SIZE: usize = 4096;

/// Number of pages per segment file (1 GB / 4 KB).
pub const PAGES_PER_SEGMENT: usize = 262_144;

/// Maximum bytes per segment file.
pub const MAX_SEGMENT_BYTES: u64 = PAGES_PER_SEGMENT as u64 * PAGE_SIZE as u64;

/// Creates a composite page ID from a table ID and a local page number.
///
/// High 32 bits = table_id, low 32 bits = local_page. table_id must be > 0.
pub fn make_page_id(table_id: u32, local_page: u32) -> PageId {
    ((table_id as usize) << 32) | (local_page as usize)
}

/// Extracts the table ID from a composite page ID.
pub fn table_id_of(page_id: PageId) -> u32 {
    (page_id >> 32) as u32
}

/// Extracts the local page number from a composite page ID.
pub fn local_page_of(page_id: PageId) -> u32 {
    (page_id & 0xFFFF_FFFF) as u32
}

/// A specialized error type for buffer pool manager operations.
#[derive(Debug)]
pub enum BpmError {
    /// Returned when the pool is full and no pages can be evicted.
    NoFreeFrames,
    /// Represents an I/O error from the disk manager.
    IoError(std::io::Error),
}

/// A smart pointer representing a pinned page.
///
/// This guard provides mutable access to the page's byte data. When the guard
/// is dropped, it automatically informs the buffer pool manager to unpin the page,
/// allowing it to be considered for eviction.
pub trait PageGuard: Deref<Target = [u8]> + DerefMut + Debug {
    /// Returns the ID of the page being held.
    fn page_id(&self) -> PageId;
}

/// The main trait defining the behavior of a Buffer Pool Manager.
///
/// This trait is designed to be object-safe, so it can be used with
/// trait objects (`Box<dyn BufferPoolManager>`).
pub trait BufferPoolManager: Send + Sync {
    /// Fetches a page from the buffer pool, reading from disk if necessary.
    ///
    /// This method pins the page and returns a `PageGuard`. The page remains
    /// pinned until the `PageGuard` is dropped.
    ///
    /// # Arguments
    /// * `page_id` - The ID of the page to fetch.
    fn fetch_page(&self, page_id: PageId) -> Result<Box<dyn PageGuard + '_>, BpmError>;

    /// Creates a new page in the buffer pool for the given table.
    ///
    /// Finds an available frame, allocates a new page ID, and returns the
    /// pinned page as a `PageGuard`.
    ///
    /// # Arguments
    /// * `table_id` - The ID of the table that owns the new page.
    fn new_page(&self, table_id: u32) -> Result<Box<dyn PageGuard + '_>, BpmError>;

    /// Unpins a page from the buffer pool.
    ///
    /// This is typically called by the `PageGuard`'s drop implementation.
    ///
    /// # Arguments
    /// * `page_id` - The ID of the page to unpin.
    fn unpin_page(&self, page_id: PageId) -> Result<(), BpmError>;

    /// Flushes a specific page to disk if it is dirty.
    ///
    /// # Arguments
    /// * `page_id` - The ID of the page to flush.
    fn flush_page(&self, page_id: PageId) -> Result<(), BpmError>;

    /// Flushes all dirty pages in the buffer pool to disk.
    fn flush_all_pages(&self) -> Result<(), BpmError>;
}
