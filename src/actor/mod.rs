
//! The actor_buffer_pool_manager-based implementation of the Buffer Pool Manager.

use super::api::{BufferPoolManager, BpmError, PageGuard, PageId, PAGE_SIZE};
use super::disk_manager::DiskManager;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::mpsc::{self, Sender};
use std::sync::Arc;
use std::thread;

type FrameId = usize;

// A responder channel to send a result back to the calling thread.
type Responder<T> = mpsc::Sender<Result<T, BpmError>>;

/// Defines the messages that can be sent to the BPM actor_buffer_pool_manager.
enum BpmMessage {
    FetchPage {
        page_id: PageId,
        responder: Responder<Box<[u8; PAGE_SIZE]>>,
    },
    NewPage {
        responder: Responder<(PageId, Box<[u8; PAGE_SIZE]>)>,
    },
    Unpin {
        page_id: PageId,
        data: Box<[u8; PAGE_SIZE]>,
        is_dirty: bool,
    },
    FlushPage {
        page_id: PageId,
        responder: Responder<()>,
    },
    FlushAllPages {
        responder: Responder<()>,
    },
    Stop,
}

/// The handle for the Actor-based Buffer Pool Manager.
/// This is the public-facing struct that clients interact with.
#[derive(Debug)]
pub struct ActorBufferPoolManager {
    sender: Sender<BpmMessage>,
}

/// A page guard for the actor_buffer_pool_manager BPM.
/// It owns the page data and sends an unpin message on drop.
#[derive(Debug)]
pub struct ActorPageGuard {
    page_id: PageId,
    data: Box<[u8; PAGE_SIZE]>,
    sender: Sender<BpmMessage>,
    is_dirty: bool,
}

impl PageGuard for ActorPageGuard {
    fn page_id(&self) -> PageId {
        self.page_id
    }
}

impl Deref for ActorPageGuard {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.data[..]
    }
}

impl DerefMut for ActorPageGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.is_dirty = true;
        &mut self.data[..]
    }
}

impl Drop for ActorPageGuard {
    fn drop(&mut self) {
        // To prevent blocking on drop, we create a new owned data box.
        let mut data = Box::new([0; PAGE_SIZE]);
        data.copy_from_slice(&self.data[..]);

        let _ = self.sender.send(BpmMessage::Unpin {
            page_id: self.page_id,
            data,
            is_dirty: self.is_dirty,
        });
    }
}

impl BufferPoolManager for ActorBufferPoolManager {
    fn fetch_page(&self, page_id: PageId) -> Result<Box<dyn PageGuard + '_>, BpmError> {
        let (tx, rx) = mpsc::channel();
        self.sender.send(BpmMessage::FetchPage { page_id, responder: tx }).unwrap();
        let data = rx.recv().unwrap()?;
        Ok(Box::new(ActorPageGuard {
            page_id,
            data,
            sender: self.sender.clone(),
            is_dirty: false,
        }))
    }

    fn new_page(&self) -> Result<Box<dyn PageGuard + '_>, BpmError> {
        let (tx, rx) = mpsc::channel();
        self.sender.send(BpmMessage::NewPage { responder: tx }).unwrap();
        let (page_id, data) = rx.recv().unwrap()?;
        Ok(Box::new(ActorPageGuard {
            page_id,
            data,
            sender: self.sender.clone(),
            is_dirty: true, // New pages are always dirty
        }))
    }

    fn unpin_page(&self, _page_id: PageId) -> Result<(), BpmError> {
        // Unpinning is handled by the ActorPageGuard's drop.
        // This method is a no-op to satisfy the trait, but shouldn't be called directly.
        Ok(())
    }

    fn flush_page(&self, page_id: PageId) -> Result<(), BpmError> {
        let (tx, rx) = mpsc::channel();
        self.sender.send(BpmMessage::FlushPage { page_id, responder: tx }).unwrap();
        rx.recv().unwrap()
    }

    fn flush_all_pages(&self) -> Result<(), BpmError> {
        let (tx, rx) = mpsc::channel();
        self.sender.send(BpmMessage::FlushAllPages { responder: tx }).unwrap();
        rx.recv().unwrap()
    }
}

impl Drop for ActorBufferPoolManager {
    fn drop(&mut self) {
        let _ = self.sender.send(BpmMessage::Stop);
    }
}

impl ActorBufferPoolManager {
    /// Creates a new ActorBufferPoolManager and spawns the actor_buffer_pool_manager thread.
    pub fn new(pool_size: usize, disk_manager: Arc<DiskManager>) -> Self {
        let (sender, receiver) = mpsc::channel();
        let actor = BpmActorState::new(pool_size, disk_manager, receiver);

        thread::spawn(move || actor.run());

        Self { sender }
    }
}

// --- Actor Internals ---

#[derive(Debug)]
struct Frame {
    page_id: PageId,
    pin_count: usize,
    is_dirty: bool,
    is_referenced: bool,
}

/// This struct holds the actual state and runs on the dedicated actor_buffer_pool_manager thread.
/// It does not use any internal locks.
struct BpmActorState {
    frames: Vec<Frame>,
    frame_data: Vec<Box<[u8; PAGE_SIZE]>>,
    page_table: HashMap<PageId, FrameId>,
    free_list: Vec<FrameId>,
    disk_manager: Arc<DiskManager>,
    pool_size: usize,
    clock_hand: usize,
    receiver: mpsc::Receiver<BpmMessage>,
}

impl BpmActorState {
    fn new(
        pool_size: usize,
        disk_manager: Arc<DiskManager>,
        receiver: mpsc::Receiver<BpmMessage>,
    ) -> Self {
        let mut frames = Vec::with_capacity(pool_size);
        let mut frame_data = Vec::with_capacity(pool_size);
        let mut free_list = Vec::with_capacity(pool_size);

        for i in 0..pool_size {
            frames.push(Frame { page_id: 0, pin_count: 0, is_dirty: false, is_referenced: false });
            frame_data.push(Box::new([0; PAGE_SIZE]));
            free_list.push(i);
        }

        Self {
            frames,
            frame_data,
            page_table: HashMap::new(),
            free_list,
            disk_manager,
            pool_size,
            clock_hand: 0,
            receiver,
        }
    }

    /// The main loop for the actor_buffer_pool_manager.
    fn run(mut self) {
        while let Ok(msg) = self.receiver.recv() {
            match msg {
                BpmMessage::FetchPage { page_id, responder } => {
                    let result = self.fetch_page_logic(page_id);
                    let _ = responder.send(result);
                }
                BpmMessage::NewPage { responder } => {
                    let result = self.new_page_logic();
                    let _ = responder.send(result);
                }
                BpmMessage::Unpin { page_id, data, is_dirty } => {
                    self.unpin_logic(page_id, data, is_dirty);
                }
                BpmMessage::FlushPage { page_id, responder } => {
                    let result = self.flush_page_logic(page_id);
                    let _ = responder.send(result);
                }
                BpmMessage::FlushAllPages { responder } => {
                    let result = self.flush_all_pages_logic();
                    let _ = responder.send(result);
                }
                BpmMessage::Stop => break,
            }
        }
    }

    fn fetch_page_logic(&mut self, page_id: PageId) -> Result<Box<[u8; PAGE_SIZE]>, BpmError> {
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            self.frames[frame_id].pin_count += 1;
            self.frames[frame_id].is_referenced = true;
            return Ok(self.frame_data[frame_id].clone());
        }

        let frame_id = self.find_victim_frame()?;
        
        if self.frames[frame_id].is_dirty {
            let old_page_id = self.frames[frame_id].page_id;
            let data = &self.frame_data[frame_id];
            self.disk_manager.write_page(old_page_id, &data[..]).map_err(BpmError::IoError)?;
        }

        let old_page_id = self.frames[frame_id].page_id;
        self.disk_manager.read_page(page_id, &mut self.frame_data[frame_id][..]).map_err(BpmError::IoError)?;

        self.page_table.remove(&old_page_id);
        self.page_table.insert(page_id, frame_id);

        self.frames[frame_id] = Frame {
            page_id,
            pin_count: 1,
            is_dirty: false,
            is_referenced: true,
        };

        Ok(self.frame_data[frame_id].clone())
    }

    fn new_page_logic(&mut self) -> Result<(PageId, Box<[u8; PAGE_SIZE]>), BpmError> {
        let frame_id = self.find_victim_frame()?;

        if self.frames[frame_id].is_dirty {
            let old_page_id = self.frames[frame_id].page_id;
            let data = &self.frame_data[frame_id];
            self.disk_manager.write_page(old_page_id, &data[..]).map_err(BpmError::IoError)?;
        }

        let old_page_id = self.frames[frame_id].page_id;
        let new_page_id = self.disk_manager.allocate_page();

        self.page_table.remove(&old_page_id);
        self.page_table.insert(new_page_id, frame_id);

        self.frames[frame_id] = Frame {
            page_id: new_page_id,
            pin_count: 1,
            is_dirty: true,
            is_referenced: true,
        };
        self.frame_data[frame_id] = Box::new([0; PAGE_SIZE]);

        Ok((new_page_id, self.frame_data[frame_id].clone()))
    }

    fn unpin_logic(&mut self, page_id: PageId, data: Box<[u8; PAGE_SIZE]>, is_dirty: bool) {
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            if self.frames[frame_id].pin_count > 0 {
                self.frames[frame_id].pin_count -= 1;
            }
            if is_dirty {
                self.frames[frame_id].is_dirty = true;
                self.frame_data[frame_id] = data;
            }
        }
    }

    fn flush_page_logic(&mut self, page_id: PageId) -> Result<(), BpmError> {
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            if self.frames[frame_id].is_dirty {
                let data = &self.frame_data[frame_id];
                self.disk_manager.write_page(page_id, &data[..]).map_err(BpmError::IoError)?;
                self.frames[frame_id].is_dirty = false;
            }
        }
        Ok(())
    }

    fn flush_all_pages_logic(&mut self) -> Result<(), BpmError> {
        for (&page_id, &frame_id) in self.page_table.clone().iter() {
             if self.frames[frame_id].is_dirty {
                self.flush_page_logic(page_id)?;
            }
        }
        Ok(())
    }

    fn find_victim_frame(&mut self) -> Result<FrameId, BpmError> {
        if let Some(frame_id) = self.free_list.pop() {
            return Ok(frame_id);
        }

        for _ in 0..(2 * self.pool_size) {
            let frame_id = self.clock_hand;

            if self.frames[frame_id].pin_count == 0 {
                if self.frames[frame_id].is_referenced {
                    self.frames[frame_id].is_referenced = false;
                } else {
                    // Found a victim. Advance clock hand for next search.
                    self.clock_hand = (self.clock_hand + 1) % self.pool_size;
                    return Ok(frame_id);
                }
            }

            self.clock_hand = (self.clock_hand + 1) % self.pool_size;
        }

        Err(BpmError::NoFreeFrames)
    }
}

