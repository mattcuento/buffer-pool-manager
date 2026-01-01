
//! Defines the common API for all buffer pool manager implementations.
use std::ops::{Deref, DerefMut};
use std::fmt::Debug;


/// A unique identifier for a page in the database.
pub type PageId = usize;

/// A constant to represent an invalid page ID.
pub const INVALID_PAGE_ID: PageId = 0;

/// The size of a single page in bytes.
pub const PAGE_SIZE: usize = 4096;

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

    /// Creates a new page in the buffer pool.
    ///
    /// Finds an available frame, allocates a new page ID, and returns the
    /// pinned page as a `PageGuard`.
    fn new_page(&self) -> Result<Box<dyn PageGuard + '_>, BpmError>;

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
