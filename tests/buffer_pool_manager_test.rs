use test_case::test_case;
use std::sync::Arc;
use std::fs;
use std::thread;

use buffer_pool_manager::api::{BufferPoolManager, PageId};
use buffer_pool_manager::disk_manager::DiskManager;
use buffer_pool_manager::actor::ActorBufferPoolManager;
use buffer_pool_manager::concurrent::ConcurrentBufferPoolManager;

// Define a type alias for the BPM factory to simplify function signatures
type BPMFactory = Arc<dyn Fn(Arc<DiskManager>, usize) -> Arc<dyn BufferPoolManager + 'static> + Send + Sync>;

// Helper to remove db file, useful in tests.
fn cleanup_db_file(db_file: &str) {
    let _ = fs::remove_file(db_file);
}

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

#[test_case(get_actor_bpm_factory(), "actor_new_page.db", TEST_POOL_SIZE ; "actor_bpm_new_page")]
#[test_case(get_concurrent_bpm_factory(), "concurrent_new_page.db", TEST_POOL_SIZE ; "concurrent_bpm_new_page")]
fn test_new_page(bpm_factory: BPMFactory, db_file: &str, pool_size: usize) {
    let disk_manager = Arc::new(DiskManager::new(db_file, false).unwrap());
    let bpm = bpm_factory(disk_manager, pool_size);
    let page = bpm.new_page().unwrap();
    assert_eq!(page.page_id(), 0);
    drop(page); // Unpin the page before removing the file
    cleanup_db_file(db_file);
}

#[test_case(get_actor_bpm_factory(), "actor_fetch_page.db", TEST_POOL_SIZE ; "actor_bpm_fetch_page")]
#[test_case(get_concurrent_bpm_factory(), "concurrent_fetch_page.db", TEST_POOL_SIZE ; "concurrent_bpm_fetch_page")]
fn test_fetch_page(bpm_factory: BPMFactory, db_file: &str, pool_size: usize) {
    let disk_manager = Arc::new(DiskManager::new(db_file, false).unwrap());
    let bpm = bpm_factory(disk_manager, pool_size);
    let page = bpm.new_page().unwrap();
    let page_id = page.page_id();
    drop(page);

    let fetched_page = bpm.fetch_page(page_id).unwrap();
    assert_eq!(fetched_page.page_id(), page_id);
    drop(fetched_page); // Unpin
    cleanup_db_file(db_file);
}

#[test_case(get_actor_bpm_factory(), "actor_unpin_page.db", TEST_POOL_SIZE ; "actor_bpm_unpin_page")]
#[test_case(get_concurrent_bpm_factory(), "concurrent_unpin_page.db", TEST_POOL_SIZE ; "concurrent_bpm_unpin_page")]
fn test_unpin_page(bpm_factory: BPMFactory, db_file: &str, pool_size: usize) {
    let disk_manager = Arc::new(DiskManager::new(db_file, false).unwrap());
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
    
    cleanup_db_file(db_file);
}

#[test_case(get_actor_bpm_factory(), "actor_multithreaded.db", MULTITHREADED_POOL_SIZE ; "actor_bpm_multithreaded")]
#[test_case(get_concurrent_bpm_factory(), "concurrent_multithreaded.db", MULTITHREADED_POOL_SIZE ; "concurrent_bpm_multithreaded")]
fn test_multithreaded_many_threads_no_contention(bpm_factory: BPMFactory, db_file: &str, pool_size: usize) {
    let disk_manager = Arc::new(DiskManager::new(db_file, false).unwrap());
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
    cleanup_db_file(db_file);
}
