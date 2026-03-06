
use buffer_pool_manager::disk_manager::{DiskManager, DiskManagerTrait};
use buffer_pool_manager::api::PAGE_SIZE;
use std::fs;
use std::path::Path;
use std::sync::Arc;

#[test]
fn test_disk_manager_allocate() {
    let dir = "test_disk_manager_allocate_dir";
    let table_id: u32 = 1;
    let disk_manager = DiskManager::new(Path::new(dir), false).unwrap();
    disk_manager.register_table(table_id, "t").unwrap();
    let p1 = disk_manager.allocate_page(table_id);
    let p2 = disk_manager.allocate_page(table_id);
    // Pages for the same table should differ
    assert_ne!(p1, p2);
    // Local page numbers should be 1 and 2
    assert_eq!(buffer_pool_manager::api::local_page_of(p1), 1);
    assert_eq!(buffer_pool_manager::api::local_page_of(p2), 2);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn test_disk_manager_read_write() {
    let dir = "test_disk_manager_read_write_dir";
    let table_id: u32 = 1;
    let disk_manager = Arc::new(DiskManager::new(Path::new(dir), false).unwrap());
    disk_manager.register_table(table_id, "t").unwrap();
    let page_id = disk_manager.allocate_page(table_id);

    let mut data = [0u8; PAGE_SIZE];
    for i in 0..PAGE_SIZE {
        data[i] = i as u8;
    }

    disk_manager.write_page(page_id, &data).unwrap();

    let mut read_data = [0u8; PAGE_SIZE];
    disk_manager.read_page(page_id, &mut read_data).unwrap();

    assert_eq!(data, read_data);

    fs::remove_dir_all(dir).unwrap();
}
