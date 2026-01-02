use test_case::test_case;
use std::sync::Arc;
use std::thread;
use tempfile::NamedTempFile;

use buffer_pool_manager::api::{BpmError, BufferPoolManager, PageId};
use buffer_pool_manager::disk_manager::DiskManager;
use buffer_pool_manager::actor::ActorBufferPoolManager;
use buffer_pool_manager::concurrent::ConcurrentBufferPoolManager;

// Define a type alias for the BPM factory to simplify function signatures
type BPMFactory = Arc<dyn Fn(Arc<DiskManager>, usize) -> Arc<dyn BufferPoolManager + 'static> + Send + Sync>;


const TEST_POOL_SIZE: usize = 3;
const MULTITHREADED_POOL_SIZE: usize = 10;

fn get_actor_bpm_factory() -> BPMFactory {
    Arc::new(|disk_manager: Arc<DiskManager>, pool_size: usize| {
        Arc::new(ActorBufferPoolManager::new(pool_size, disk_manager))
    })
}

fn get_concurrent_bpm_factory() -> BPMFactory {
    Arc::new(|disk_manager: Arc<DiskManager>, pool_size: usize| {
        Arc::new(ConcurrentBufferPoolManager::new(pool_size, disk_manager))
    })
}

#[test_case(get_actor_bpm_factory(), TEST_POOL_SIZE ; "actor_bpm_new_page")]
#[test_case(get_concurrent_bpm_factory(), TEST_POOL_SIZE ; "concurrent_bpm_new_page")]
fn test_new_page(bpm_factory: BPMFactory, pool_size: usize) {
    let _ = env_logger::try_init();
    let temp_file = NamedTempFile::new().unwrap();
    let db_file_path = temp_file.path().to_str().unwrap();
    let disk_manager = Arc::new(DiskManager::new(db_file_path, false).unwrap());
    let bpm = bpm_factory(disk_manager, pool_size);
    let page = bpm.new_page().unwrap();
    assert_eq!(page.page_id(), 0);
    drop(page); // Unpin the page before removing the file
}

#[test_case(get_actor_bpm_factory(), TEST_POOL_SIZE ; "actor_bpm_fetch_page")]
#[test_case(get_concurrent_bpm_factory(), TEST_POOL_SIZE ; "concurrent_bpm_fetch_page")]
fn test_fetch_page(bpm_factory: BPMFactory, pool_size: usize) {
    let _ = env_logger::try_init();
    let temp_file = NamedTempFile::new().unwrap();
    let db_file_path = temp_file.path().to_str().unwrap();
    let disk_manager = Arc::new(DiskManager::new(db_file_path, false).unwrap());
    let bpm = bpm_factory(disk_manager, pool_size);
    let page = bpm.new_page().unwrap();
    let page_id = page.page_id();
    drop(page);

    let fetched_page = bpm.fetch_page(page_id).unwrap();
    assert_eq!(fetched_page.page_id(), page_id);
    drop(fetched_page); // Unpin
}

#[test_case(get_actor_bpm_factory(), TEST_POOL_SIZE ; "actor_bpm_unpin_page")]
#[test_case(get_concurrent_bpm_factory(), TEST_POOL_SIZE ; "concurrent_bpm_unpin_page")]
fn test_unpin_page(bpm_factory: BPMFactory, pool_size: usize) {
    let _ = env_logger::try_init();
    let temp_file = NamedTempFile::new().unwrap();
    let db_file_path = temp_file.path().to_str().unwrap();
    let disk_manager = Arc::new(DiskManager::new(db_file_path, false).unwrap());
    let bpm = bpm_factory(disk_manager, pool_size);

    let mut pages = Vec::new();

    // Create a page and pin it.
    let page_pinned = bpm.new_page().unwrap();
    let _page_id_pinned = page_pinned.page_id();

    // Fill the buffer pool with (pool_size - 1) new pages.
    // These should not evict the pinned 'page_pinned'.
    for _ in 0..(pool_size - 1) {
        pages.push(bpm.new_page().unwrap());
    }

    // Now, try to create one more page. This should fail if 'page_pinned' is still pinned
    // and prevents eviction, as the pool is full and no other pages can be evicted.
    let res = bpm.new_page();
    assert!(res.is_err(), "Expected NoFreeFrames error, got {:?}", res);

    drop(page_pinned); // Unpin the original page
    pages.clear(); // Drop all other pages, unpinning them.

    // Now, we should be able to create a new page because frames are free.
    let _page_c = bpm.new_page().unwrap(); // This should succeed
}

#[test_case(get_actor_bpm_factory(), MULTITHREADED_POOL_SIZE ; "actor_bpm_multithreaded")]
#[test_case(get_concurrent_bpm_factory(), MULTITHREADED_POOL_SIZE ; "concurrent_bpm_multithreaded")]
fn test_multithreaded_many_threads_no_contention(bpm_factory: BPMFactory, pool_size: usize) {
    let _ = env_logger::try_init();
    let temp_file = NamedTempFile::new().unwrap();
    let db_file_path = temp_file.path().to_str().unwrap();
    let disk_manager = Arc::new(DiskManager::new(db_file_path, false).unwrap());
    let bpm = bpm_factory(disk_manager.clone(), pool_size); // Recreate BPM for this test
    let mut threads = vec![];
    let num_threads = 5;

    for _i in 0..num_threads {
        let bpm_clone = bpm.clone();
        threads.push(thread::spawn(move || {
            let mut page = bpm_clone.new_page().unwrap();
            let page_id = page.page_id();

            // Write a unique identifier to the page using DerefMut
            page[0] = page_id as u8; // Use page_id as unique identifier

            // The PageGuard will be dropped here, unpinning the page.
            page_id
        }));
    }

    let page_ids: Vec<PageId> = threads.into_iter().map(|t| t.join().unwrap()).collect();

    // Force all dirty pages to be written to disk.
    // This is a workaround to ensure that if a page was unexpectedly evicted, we can still fetch it.
    bpm.flush_all_pages().unwrap();

    // Verify the data in each page
    for page_id in page_ids.iter() {
        let page = bpm.fetch_page(*page_id).unwrap();
        // Read the data using Deref
        assert_eq!(
            page[0],
            *page_id as u8, // Compare with page_id as u8
            "Data corruption detected for page {}",
            page_id
        );
        drop(page);
    }
}

#[test_case(get_actor_bpm_factory(), TEST_POOL_SIZE ; "actor_bpm_lru_eviction")]
#[test_case(get_concurrent_bpm_factory(), TEST_POOL_SIZE ; "concurrent_bpm_lru_eviction")]
fn test_lru_eviction(bpm_factory: BPMFactory, pool_size: usize) {
    let _ = env_logger::try_init();
    let temp_file = NamedTempFile::new().unwrap();
    let db_file_path = temp_file.path().to_str().unwrap();
    let disk_manager = Arc::new(DiskManager::new(db_file_path, false).unwrap());
    let bpm = bpm_factory(disk_manager, pool_size);

    // Create pages to fill the buffer pool.
    // They are immediately unpinned as the Page guards are dropped.
    let p1_id = bpm.new_page().unwrap().page_id();
    let p2_id = bpm.new_page().unwrap().page_id();
    let p3_id = bpm.new_page().unwrap().page_id();

    // At this point, the LRU order is p1, p2, p3 (p1 is the least recent).
    // Let's access p1 to make it the most recently used.
    {
        let _p1_fetched = bpm.fetch_page(p1_id).unwrap();
    } // _p1_fetched is dropped, unpinning the page.

    // Now the LRU order should be p2, p3, p1.
    // Requesting a new page should evict p2.
    let p4_id = bpm.new_page().unwrap().page_id();

    // Pin all pages currently in the buffer pool. These should be p1, p3, and p4.
    let _p1_pinned = bpm.fetch_page(p1_id).unwrap();
    let _p3_pinned = bpm.fetch_page(p3_id).unwrap();
    let _p4_pinned = bpm.fetch_page(p4_id).unwrap();

    // Now, try to fetch p2. Since all frames are occupied by pinned pages,
    // and p2 should have been evicted, this should fail.
    let res = bpm.fetch_page(p2_id);
    assert!(res.is_err(), "Expected an error when fetching an evicted page with a full pool of pinned pages");
}

/// A RAII guard to set a file to read-only and restore its permissions on drop.
struct ReadOnlyFileGuard<'a> {
    path: &'a std::path::Path,
}

impl<'a> ReadOnlyFileGuard<'a> {
    fn new(path: &'a std::path::Path) -> Self {
        let mut perms = std::fs::metadata(path).unwrap().permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(path, perms).unwrap();
        Self { path }
    }
}

impl<'a> Drop for ReadOnlyFileGuard<'a> {
    fn drop(&mut self) {
        // Best effort to restore permissions. Don't panic in drop.
        if let Ok(metadata) = std::fs::metadata(self.path) {
            let mut perms = metadata.permissions();
            perms.set_readonly(false);
            let _ = std::fs::set_permissions(self.path, perms);
        }
    }
}

#[test_case(get_actor_bpm_factory(), TEST_POOL_SIZE ; "actor_bpm_io_error")]
#[test_case(get_concurrent_bpm_factory(), TEST_POOL_SIZE ; "concurrent_bpm_io_error")]
fn test_dirty_page_eviction_with_io_error(bpm_factory: BPMFactory, _pool_size: usize) {
    let _ = env_logger::try_init();
    // This test uses a pool size of 1 to remove ambiguity in eviction policy.
    const POOL_SIZE: usize = 1;

    let temp_file = NamedTempFile::new().unwrap();
    let db_file_path = temp_file.path();
    let disk_manager = Arc::new(DiskManager::new(db_file_path.to_str().unwrap(), false).unwrap());
    let bpm = bpm_factory(disk_manager, POOL_SIZE);

    // Create a page, make it dirty, and unpin it. The pool is now full.
    {
        let mut p1 = bpm.new_page().unwrap();
        p1[0] = 42; // Make page dirty
    } // p1 is dropped and unpinned.

    let res = {
        // Make the database file read-only.
        let _guard = ReadOnlyFileGuard::new(db_file_path);

        // Try to create a second page. This MUST evict the dirty page p1.
        // The flush should fail, returning an error.
        bpm.new_page()
    }; // _guard is dropped here, restoring file permissions.

    assert!(res.is_err(), "Expected an I/O error when evicting a dirty page from a full pool of size 1");
    match res {
        Err(BpmError::IoError(e)) => {
            assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied, "Expected PermissionDenied error kind");
        }
        Err(other_err) => {
            panic!("Expected BpmError::IoError, but got a different error: {:?}", other_err);
        }
        Ok(_) => {
            panic!("Expected an error, but the operation succeeded");
        }
    }
}
