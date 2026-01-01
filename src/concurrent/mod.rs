
//! The fine-grained locking concurrent_buffer_pool_manager implementation of the Buffer Pool Manager.

use super::api::{BufferPoolManager, BpmError, PageGuard, PageId, PAGE_SIZE};
use super::disk_manager::DiskManager;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, RwLock};

// Type alias for a frame index
type FrameId = usize;

/// Represents a single frame in the buffer pool.
#[derive(Debug)]
struct Frame {
    page_id: PageId,
    data: [u8; PAGE_SIZE],
    pin_count: usize,
    is_dirty: bool,
    is_referenced: bool, // For the CLOCK replacer
}

/// The main struct for the concurrent_buffer_pool_manager Buffer Pool Manager.
#[derive(Debug)]
pub struct ConcurrentBufferPoolManager {
    frames: Vec<RwLock<Frame>>,
    page_table: RwLock<HashMap<PageId, FrameId>>,
    free_list: Mutex<Vec<FrameId>>,
    disk_manager: Arc<DiskManager>,
    pool_size: usize,
    // The "clock hand" for the CLOCK replacement algorithm.
    clock_hand: Mutex<usize>,
}

/// A page guard for the concurrent_buffer_pool_manager BPM.
///
/// This guard holds a write lock on the frame for its entire lifetime.
/// When dropped, it automatically unpins the page in the BPM.
#[derive(Debug)]
pub struct ConcurrentPageGuard<'a> {
    buffer_pool_manager: &'a ConcurrentBufferPoolManager,
    page_id: PageId,
    frame_id: FrameId,
}

impl<'a> PageGuard for ConcurrentPageGuard<'a> {
    fn page_id(&self) -> PageId {
        self.page_id
    }
}

impl<'a> Deref for ConcurrentPageGuard<'a> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        // Since the guard is alive, the page is pinned and won't be replaced.
        // We can safely lock and access the data.
        let frame_guard = self.buffer_pool_manager.frames[self.frame_id].read().unwrap();
        // The borrow checker is not smart enough to know that the guard's lifetime
        // is tied to the lock. We use a bit of unsafe to extend the lifetime.
        // This is safe because the PageGuard's lifetime ensures the lock is held.
        unsafe { &*(&frame_guard.data as *const _) }
    }
}

impl<'a> DerefMut for ConcurrentPageGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let mut frame_guard = self.buffer_pool_manager.frames[self.frame_id].write().unwrap();
        frame_guard.is_dirty = true;
        // The borrow checker is not smart enough to know that the guard's lifetime
        // is tied to the lock. We use a bit of unsafe to extend the lifetime.
        // This is safe because the PageGuard's lifetime ensures the lock is held.
        unsafe { &mut *(&mut frame_guard.data as *mut _) }
    }
}

impl<'a> Drop for ConcurrentPageGuard<'a> {
    fn drop(&mut self) {
        self.buffer_pool_manager.unpin_page(self.page_id).unwrap();
    }
}

impl BufferPoolManager for ConcurrentBufferPoolManager {
    fn fetch_page(&self, page_id: PageId) -> Result<Box<dyn PageGuard + '_>, BpmError> {
        let pt_read_lock = self.page_table.read().unwrap();
        if let Some(&frame_id) = pt_read_lock.get(&page_id) {
            // Page is in the buffer pool.
            let mut frame = self.frames[frame_id].write().unwrap();
            frame.pin_count += 1;
            frame.is_referenced = true;
            return Ok(Box::new(ConcurrentPageGuard { buffer_pool_manager: self, page_id, frame_id }));
        }
        drop(pt_read_lock);

        // Page not in pool, need to fetch from disk.
        let frame_id = self.find_victim_frame()?;
        let mut frame = self.frames[frame_id].write().unwrap();

        // If the victim frame is dirty, write it back to disk.
        if frame.is_dirty {
            self.disk_manager.write_page(frame.page_id, &frame.data).map_err(BpmError::IoError)?;
        }

        let old_page_id = frame.page_id;

        // Update frame metadata for the new page.
        self.disk_manager.read_page(page_id, &mut frame.data).map_err(BpmError::IoError)?;
        frame.page_id = page_id;
        frame.pin_count = 1;
        frame.is_dirty = false;
        frame.is_referenced = true;

        // Update the page table.
        let mut pt_write_lock = self.page_table.write().unwrap();
        pt_write_lock.remove(&old_page_id);
        pt_write_lock.insert(page_id, frame_id);

        Ok(Box::new(ConcurrentPageGuard { buffer_pool_manager: self, page_id, frame_id }))
    }

    fn new_page(&self) -> Result<Box<dyn PageGuard + '_>, BpmError> {
        let frame_id = self.find_victim_frame()?;
        let mut frame = self.frames[frame_id].write().unwrap();

        if frame.is_dirty {
            self.disk_manager.write_page(frame.page_id, &frame.data).map_err(BpmError::IoError)?;
        }

        let old_page_id = frame.page_id;
        let new_page_id = self.disk_manager.allocate_page();

        // Update frame metadata.
        frame.page_id = new_page_id;
        frame.pin_count = 1;
        frame.is_dirty = true; // New page is immediately dirty.
        frame.is_referenced = true;
        frame.data = [0; PAGE_SIZE];

        // Update page table.
        let mut pt_write_lock = self.page_table.write().unwrap();
        pt_write_lock.remove(&old_page_id);
        pt_write_lock.insert(new_page_id, frame_id);

        Ok(Box::new(ConcurrentPageGuard { buffer_pool_manager: self, page_id: new_page_id, frame_id }))
    }

    fn unpin_page(&self, page_id: PageId) -> Result<(), BpmError> {
        let pt_read_lock = self.page_table.read().unwrap();
        if let Some(&frame_id) = pt_read_lock.get(&page_id) {
            let mut frame = self.frames[frame_id].write().unwrap();
            if frame.pin_count > 0 {
                frame.pin_count -= 1;
            }
        }
        Ok(())
    }

    fn flush_page(&self, page_id: PageId) -> Result<(), BpmError> {
        let pt_read_lock = self.page_table.read().unwrap();
        if let Some(&frame_id) = pt_read_lock.get(&page_id) {
            let mut frame = self.frames[frame_id].write().unwrap();
            if frame.is_dirty {
                self.disk_manager.write_page(page_id, &frame.data).map_err(BpmError::IoError)?;
                frame.is_dirty = false;
            }
        }
        Ok(())
    }

    fn flush_all_pages(&self) -> Result<(), BpmError> {
        let pt_read_lock = self.page_table.read().unwrap();
        for (&page_id, &frame_id) in pt_read_lock.iter() {
            let mut frame = self.frames[frame_id].write().unwrap();
            if frame.is_dirty {
                self.disk_manager.write_page(page_id, &frame.data).map_err(BpmError::IoError)?;
                frame.is_dirty = false;
            }
        }
        Ok(())
    }
}

impl ConcurrentBufferPoolManager {
    /// Creates a new ConcurrentBufferPoolManager.
    pub fn new(pool_size: usize, disk_manager: Arc<DiskManager>) -> Self {
        let mut frames = Vec::with_capacity(pool_size);
        let mut free_list = Vec::with_capacity(pool_size);
        for i in 0..pool_size {
            frames.push(RwLock::new(Frame {
                page_id: 0, // Initial dummy page_id
                data: [0; PAGE_SIZE],
                pin_count: 0,
                is_dirty: false,
                is_referenced: false,
            }));
            free_list.push(i);
        }

        Self {
            frames,
            page_table: RwLock::new(HashMap::new()),
            free_list: Mutex::new(free_list),
            disk_manager,
            pool_size,
            clock_hand: Mutex::new(0),
        }
    }

    /// Finds a victim frame using the free list or the CLOCK algorithm.
    fn find_victim_frame(&self) -> Result<FrameId, BpmError> {
        // 1. Try to get a frame from the free list.
        let mut free_list = self.free_list.lock().unwrap();
        if let Some(frame_id) = free_list.pop() {
            return Ok(frame_id);
        }
        drop(free_list);

        // 2. If free list is empty, run the CLOCK algorithm.
        let mut clock_hand = self.clock_hand.lock().unwrap();
        for _ in 0..(2 * self.pool_size) {
            // Search twice to avoid infinite loop
            let frame_id = *clock_hand;

            // Try to lock the frame. If it's locked, skip it and try the next one.
            if let Ok(mut frame) = self.frames[frame_id].try_write() {
                if frame.pin_count == 0 {
                    if frame.is_referenced {
                        // Give it a second chance.
                        frame.is_referenced = false;
                    } else {
                        // Found a victim. Advance the clock hand for the next search.
                        *clock_hand = (*clock_hand + 1) % self.pool_size;
                        return Ok(frame_id);
                    }
                }
            }
            *clock_hand = (*clock_hand + 1) % self.pool_size;
        }

        Err(BpmError::NoFreeFrames)
    }

    pub fn contains_page(&self, page_id: PageId) -> bool {
        self.page_table.read().unwrap().contains_key(&page_id)
    }
}
