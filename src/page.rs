
use crate::api::PageId;

/// The header of a page.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct PageHeader {
    /// The ID of the page.
    pub page_id: PageId,
    /// A flag indicating the type of the page.
    pub page_type: PageType,
    /// The offset of the start of the free space.
    pub free_space_pointer: u16,
    /// The number of slots in the page.
    pub slot_count: u16,
    /// The ID of the next page in the table heap.
    pub next_page_id: PageId,
}

/// The type of a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    /// A page that stores table rows.
    TablePage,
    /// A page that stores B+ tree nodes.
    IndexPage,
    /// A page that stores metadata.
    MetadataPage,
}

/// A slot in a slotted page.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct Slot {
    /// The offset of the record in the page.
    pub offset: u16,
    /// The length of the record.
    pub length: u16,
}

/// A slotted page is a page that stores variable-sized records.
/// The page is divided into a header, a slot array, and a data area.
pub struct SlottedPage<'a> {
    data: &'a mut [u8],
}

impl<'a> SlottedPage<'a> {
    /// Creates a new slotted page from a byte array.
    pub fn new(data: &'a mut [u8]) -> Self {
        Self { data }
    }

    /// Returns a reference to the page header.
    pub fn header(&self) -> &PageHeader {
        unsafe { &*(self.data.as_ptr() as *const PageHeader) }
    }

    /// Returns a mutable reference to the page header.
    pub fn header_mut(&mut self) -> &mut PageHeader {
        unsafe { &mut *(self.data.as_mut_ptr() as *mut PageHeader) }
    }

    /// Returns a reference to the slot at the given index.
    pub fn slot(&self, slot_index: u16) -> &Slot {
        let header_size = std::mem::size_of::<PageHeader>() as u16;
        let slot_offset = header_size + slot_index * std::mem::size_of::<Slot>() as u16;
        unsafe { &*(self.data.as_ptr().offset(slot_offset as isize) as *const Slot) }
    }

    /// Returns a mutable reference to the slot at the given index.
    pub fn slot_mut(&mut self, slot_index: u16) -> &mut Slot {
        let header_size = std::mem::size_of::<PageHeader>() as u16;
        let slot_offset = header_size + slot_index * std::mem::size_of::<Slot>() as u16;
        unsafe { &mut *(self.data.as_mut_ptr().offset(slot_offset as isize) as *mut Slot) }
    }

    /// Returns a slice of the page data for the given slot.
    pub fn get_record(&self, slot_index: u16) -> &[u8] {
        let slot = self.slot(slot_index);
        &self.data[slot.offset as usize..(slot.offset + slot.length) as usize]
    }

    /// Allocates a new slot and returns the index of the new slot.
    /// Returns `None` if there is not enough space.
    pub fn allocate_slot(&mut self, record_len: u16) -> Option<u16> {
        let header_size = std::mem::size_of::<PageHeader>() as u16;
        let slot_size = std::mem::size_of::<Slot>() as u16;
        let free_space_pointer = self.header().free_space_pointer;
        let slot_count = self.header().slot_count;
        let free_space = free_space_pointer - (header_size + (slot_count + 1) * slot_size);

        if free_space < record_len {
            return None;
        }

        let slot_index = slot_count;
        let new_free_space_pointer = free_space_pointer - record_len;

        let slot = self.slot_mut(slot_index);
        slot.offset = new_free_space_pointer;
        slot.length = record_len;

        let header = self.header_mut();
        header.slot_count += 1;
        header.free_space_pointer = new_free_space_pointer;

        Some(slot_index)
    }

    /// Inserts a record into the page.
    /// Returns the index of the new slot, or `None` if there is not enough space.
    pub fn insert_record(&mut self, record: &[u8]) -> Option<u16> {
        let record_len = record.len() as u16;
        if let Some(slot_index) = self.allocate_slot(record_len) {
            let slot = self.slot(slot_index);
            let offset = slot.offset as usize;
            self.data[offset..offset + record_len as usize].copy_from_slice(record);
            Some(slot_index)
        } else {
            None
        }
    }
}
