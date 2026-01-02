
use crate::api::PageId;

/// Memory layout of a page:
/// - Bytes 0-7:   page_id (usize, little-endian)
/// - Bytes 8-15:  next_page_id (usize, little-endian)
/// - Bytes 16-17: free_space_pointer (u16, little-endian)
/// - Bytes 18-19: slot_count (u16, little-endian)
/// - Byte 20:     page_type (u8)
/// - Bytes 21+:   Slot array

/// The header of a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct PageHeader {
    /// The ID of the page.
    pub page_id: PageId,
    /// The ID of the next page in the table heap.
    pub next_page_id: PageId,
    /// The offset of the start of the free space.
    pub free_space_pointer: u16,
    /// The number of slots in the page.
    pub slot_count: u16,
    /// A flag indicating the type of the page.
    pub page_type: PageType,
}

/// The type of a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    /// A page that stores table rows.
    TablePage = 0,
    /// A page that stores B+ tree nodes.
    IndexPage = 1,
    /// A page that stores metadata.
    MetadataPage = 2,
}

impl PageType {
    /// Converts a u8 value to a PageType.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(PageType::TablePage),
            1 => Some(PageType::IndexPage),
            2 => Some(PageType::MetadataPage),
            _ => None,
        }
    }

    /// Converts the PageType to a u8 value.
    pub fn as_u8(&self) -> u8 {
        *self as u8
    }
}

/// A slot in a slotted page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

    /// Reads the page header from the byte array.
    /// Returns a copy of the header.
    pub fn header(&self) -> PageHeader {
        // Ensure buffer is large enough
        const MIN_SIZE: usize = 21; // 8 + 8 + 2 + 2 + 1
        assert!(self.data.len() >= MIN_SIZE, "Buffer too small for header");

        // Parse fields from bytes (little-endian)
        let page_id = usize::from_le_bytes(
            self.data[0..8].try_into().expect("Invalid page_id bytes")
        );
        let next_page_id = usize::from_le_bytes(
            self.data[8..16].try_into().expect("Invalid next_page_id bytes")
        );
        let free_space_pointer = u16::from_le_bytes(
            self.data[16..18].try_into().expect("Invalid free_space_pointer bytes")
        );
        let slot_count = u16::from_le_bytes(
            self.data[18..20].try_into().expect("Invalid slot_count bytes")
        );
        let page_type = PageType::from_u8(self.data[20])
            .unwrap_or(PageType::TablePage); // Default to TablePage on invalid value

        PageHeader {
            page_id,
            next_page_id,
            free_space_pointer,
            slot_count,
            page_type,
        }
    }

    /// Writes the page header to the byte array.
    pub fn set_header(&mut self, header: &PageHeader) {
        const MIN_SIZE: usize = 21;
        assert!(self.data.len() >= MIN_SIZE, "Buffer too small for header");

        // Serialize fields to bytes (little-endian)
        self.data[0..8].copy_from_slice(&header.page_id.to_le_bytes());
        self.data[8..16].copy_from_slice(&header.next_page_id.to_le_bytes());
        self.data[16..18].copy_from_slice(&header.free_space_pointer.to_le_bytes());
        self.data[18..20].copy_from_slice(&header.slot_count.to_le_bytes());
        self.data[20] = header.page_type.as_u8();
    }

    /// Reads a slot from the byte array.
    /// Returns a copy of the slot.
    pub fn slot(&self, slot_index: u16) -> Slot {
        const HEADER_SIZE: usize = 21;
        const SLOT_SIZE: usize = 4; // 2 bytes offset + 2 bytes length

        let slot_offset = HEADER_SIZE + (slot_index as usize * SLOT_SIZE);
        let slot_end = slot_offset + SLOT_SIZE;

        assert!(self.data.len() >= slot_end, "Buffer too small for slot {}", slot_index);

        let offset = u16::from_le_bytes(
            self.data[slot_offset..slot_offset + 2]
                .try_into()
                .expect("Invalid slot offset bytes")
        );
        let length = u16::from_le_bytes(
            self.data[slot_offset + 2..slot_offset + 4]
                .try_into()
                .expect("Invalid slot length bytes")
        );

        Slot { offset, length }
    }

    /// Writes a slot to the byte array.
    pub fn set_slot(&mut self, slot_index: u16, slot: &Slot) {
        const HEADER_SIZE: usize = 21;
        const SLOT_SIZE: usize = 4;

        let slot_offset = HEADER_SIZE + (slot_index as usize * SLOT_SIZE);
        let slot_end = slot_offset + SLOT_SIZE;

        assert!(self.data.len() >= slot_end, "Buffer too small for slot {}", slot_index);

        self.data[slot_offset..slot_offset + 2].copy_from_slice(&slot.offset.to_le_bytes());
        self.data[slot_offset + 2..slot_offset + 4].copy_from_slice(&slot.length.to_le_bytes());
    }

    /// Returns a slice of the page data for the given slot.
    pub fn get_record(&self, slot_index: u16) -> &[u8] {
        let slot = self.slot(slot_index);
        &self.data[slot.offset as usize..(slot.offset + slot.length) as usize]
    }

    /// Allocates a new slot and returns the index of the new slot.
    /// Returns `None` if there is not enough space.
    pub fn allocate_slot(&mut self, record_len: u16) -> Option<u16> {
        const HEADER_SIZE: usize = 21;
        const SLOT_SIZE: usize = 4;

        // Read current header
        let mut header = self.header();

        let free_space_pointer = header.free_space_pointer;
        let slot_count = header.slot_count;
        let free_space = free_space_pointer
            .saturating_sub((HEADER_SIZE + (slot_count as usize + 1) * SLOT_SIZE) as u16);

        if free_space < record_len {
            return None;
        }

        let slot_index = slot_count;
        let new_free_space_pointer = free_space_pointer - record_len;

        // Write new slot
        let new_slot = Slot {
            offset: new_free_space_pointer,
            length: record_len,
        };
        self.set_slot(slot_index, &new_slot);

        // Update and write header
        header.slot_count += 1;
        header.free_space_pointer = new_free_space_pointer;
        self.set_header(&header);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::PAGE_SIZE;

    #[test]
    fn test_header_read_write() {
        let mut data = vec![0u8; PAGE_SIZE];
        let mut page = SlottedPage::new(&mut data);

        // Create and write header
        let header = PageHeader {
            page_id: 42,
            page_type: PageType::TablePage,
            free_space_pointer: 4096,
            slot_count: 0,
            next_page_id: 100,
        };
        page.set_header(&header);

        // Read back header
        let read_header = page.header();
        assert_eq!(read_header.page_id, 42);
        assert_eq!(read_header.page_type, PageType::TablePage);
        assert_eq!(read_header.free_space_pointer, 4096);
        assert_eq!(read_header.slot_count, 0);
        assert_eq!(read_header.next_page_id, 100);
    }

    #[test]
    fn test_slot_read_write() {
        let mut data = vec![0u8; PAGE_SIZE];
        let mut page = SlottedPage::new(&mut data);

        // Initialize header first
        let mut header = page.header();
        header.slot_count = 3;
        page.set_header(&header);

        // Write slots
        page.set_slot(0, &Slot { offset: 100, length: 50 });
        page.set_slot(1, &Slot { offset: 200, length: 75 });

        // Read back slots
        let slot0 = page.slot(0);
        assert_eq!(slot0.offset, 100);
        assert_eq!(slot0.length, 50);

        let slot1 = page.slot(1);
        assert_eq!(slot1.offset, 200);
        assert_eq!(slot1.length, 75);
    }

    #[test]
    fn test_insert_and_retrieve_record() {
        let mut data = vec![0u8; PAGE_SIZE];
        let mut page = SlottedPage::new(&mut data);

        // Initialize header
        let header = PageHeader {
            page_id: 1,
            page_type: PageType::TablePage,
            free_space_pointer: PAGE_SIZE as u16,
            slot_count: 0,
            next_page_id: 0,
        };
        page.set_header(&header);

        // Insert a record
        let record = b"Hello, World!";
        let slot_index = page.insert_record(record).expect("Failed to insert record");

        // Retrieve and verify the record
        let retrieved = page.get_record(slot_index);
        assert_eq!(retrieved, record);
        assert_eq!(page.header().slot_count, 1);
    }

    #[test]
    fn test_multiple_records() {
        let mut data = vec![0u8; PAGE_SIZE];
        let mut page = SlottedPage::new(&mut data);

        // Initialize header
        let header = PageHeader {
            page_id: 0,
            page_type: PageType::TablePage,
            free_space_pointer: PAGE_SIZE as u16,
            slot_count: 0,
            next_page_id: 0,
        };
        page.set_header(&header);

        // Insert multiple records
        let records = vec![
            b"First record".as_slice(),
            b"Second record".as_slice(),
            b"Third record".as_slice(),
        ];

        let mut slot_indices = Vec::new();
        for record in &records {
            let slot_index = page.insert_record(record).expect("Failed to insert record");
            slot_indices.push(slot_index);
        }

        // Verify all records
        for (i, &slot_index) in slot_indices.iter().enumerate() {
            let retrieved = page.get_record(slot_index);
            assert_eq!(retrieved, records[i]);
        }

        assert_eq!(page.header().slot_count, 3);
    }
}
