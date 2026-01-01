use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use buffer_pool_manager::actor::ActorBufferPoolManager;
use buffer_pool_manager::api::BufferPoolManager;
use buffer_pool_manager::disk_manager::DiskManager;
use buffer_pool_manager::concurrent::ConcurrentBufferPoolManager;
use std::fs;

const POOL_SIZE: usize = 100;
const NUM_PAGES: usize = 1000;

// Helper to create a DiskManager and cleanup the db file at the end
fn setup_disk_manager(db_file: &str, use_direct_io: bool) -> Arc<DiskManager> {
    // Ensure cleanup of previous run if any
    let _ = fs::remove_file(db_file); 
    let dm = Arc::new(DiskManager::new(db_file, use_direct_io).unwrap());
    dm
}

// Benchmark function for writing new pages
fn bench_write_pages<B: BufferPoolManager + 'static>(
    c: &mut Criterion,
    id: &str,
    bpm_factory: impl Fn(Arc<DiskManager>, usize) -> Arc<B>,
    use_direct_io: bool,
) {
    let mut group = c.benchmark_group(format!("Write Pages - {}", id));
    group.sample_size(10); // Smaller sample size for quick iteration during development

    group.bench_function("new_page", |b| {
        b.iter_custom(|iters| {
            let start = std::time::Instant::now();
            for _i in 0..iters {
                // Create a fresh BPM for each iteration to avoid state interference
                // and to measure the cost of BPM creation if it's significant.
                // The DB file is created by disk_manager
                let current_disk_manager = setup_disk_manager(&format!("{}_write_{}.db", id, _i), use_direct_io);
                let current_bpm = bpm_factory(current_disk_manager.clone(), POOL_SIZE);
                for _ in 0..black_box(NUM_PAGES) {
                    let _page = black_box(current_bpm.new_page().unwrap());
                }
                black_box(current_bpm.flush_all_pages().unwrap());
                let _ = fs::remove_file(format!("{}_write_{}.db", id, _i)); // Clean up this iteration's file
            }
            start.elapsed()
        });
    });
    group.finish();
}

// Benchmark function for reading pages
fn bench_read_pages<B: BufferPoolManager + 'static>(
    c: &mut Criterion,
    id: &str,
    bpm_factory: impl Fn(Arc<DiskManager>, usize) -> Arc<B>,
    use_direct_io: bool,
) {
    let mut group = c.benchmark_group(format!("Read Pages - {}", id));
    group.sample_size(10); // Smaller sample size

    let db_file = format!("{}_read.db", id);
    let disk_manager = setup_disk_manager(&db_file, use_direct_io);
    let bpm = bpm_factory(disk_manager.clone(), POOL_SIZE);
    let mut page_ids = Vec::with_capacity(NUM_PAGES);
    for _ in 0..NUM_PAGES {
        let page = bpm.new_page().unwrap();
        page_ids.push(page.page_id());
    }
    bpm.flush_all_pages().unwrap(); // Ensure all pages are written to disk
    
    group.bench_function("fetch_page", |b| {
        b.iter(|| {
            // For reads, we can use the same BPM and just fetch pages
            for &page_id in black_box(&page_ids) {
                let _page = black_box(bpm.fetch_page(page_id).unwrap());
            }
        });
    });
    group.finish();

    // Cleanup after all read benchmarks for this group
    let _ = fs::remove_file(db_file);
}


fn bpm_benchmarks(c: &mut Criterion) {
    bench_write_pages(c, "ConcurrentBPM_OSCache", |dm, ps| Arc::new(ConcurrentBufferPoolManager::new(ps, dm)), false);
    bench_write_pages(c, "ConcurrentBPM_DirectIO", |dm, ps| Arc::new(ConcurrentBufferPoolManager::new(ps, dm)), true);
    bench_write_pages(c, "ActorBPM_OSCache", |dm, ps| Arc::new(ActorBufferPoolManager::new(ps, dm)), false);
    bench_write_pages(c, "ActorBPM_DirectIO", |dm, ps| Arc::new(ActorBufferPoolManager::new(ps, dm)), true);

    bench_read_pages(c, "ConcurrentBPM_OSCache", |dm, ps| Arc::new(ConcurrentBufferPoolManager::new(ps, dm)), false);
    bench_read_pages(c, "ConcurrentBPM_DirectIO", |dm, ps| Arc::new(ConcurrentBufferPoolManager::new(ps, dm)), true);
    bench_read_pages(c, "ActorBPM_OSCache", |dm, ps| Arc::new(ActorBufferPoolManager::new(ps, dm)), false);
    bench_read_pages(c, "ActorBPM_DirectIO", |dm, ps| Arc::new(ActorBufferPoolManager::new(ps, dm)), true);
}

criterion_group!{
    name = benches;
    config = Criterion::default().measurement_time(std::time::Duration::from_secs(10));
    targets = bpm_benchmarks
}
criterion_main!(benches);