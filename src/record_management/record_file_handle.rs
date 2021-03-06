/**********************************************
  > File Name		: record_file_handle.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Mon 12 Apr 2021 11:01:57 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

use crate::utils;
use crate::page_management::page_file::{PageFileHandle, PageHandle};
use crate::errors::{Error, RecordError};

#[derive(Debug, Copy, Clone)]
pub struct RID {
    page_num: u32,
    slot_num: usize
}

pub struct Record {
    rid: RID,
    record_size: usize,
    data: *mut u8
}

/*
 * Every record file represents a table, every table has a same record 
 * size.
 *
 * In a page, all records are managed by a bitmap. The size of the bitmap
 * is calculated by the record size.
 *
 * Page data layout: bitmap | records.
 */
#[derive(Debug, Copy, Clone)]
pub struct RecordFileHeader {
    pub record_size: usize,
    pub bitmap_offset: usize,
    pub bitmap_size: usize,
    pub records_offset: usize,
    pub num_records_per_page: usize,
    pub num_pages: usize
}


#[derive(Debug, Copy, Clone)]
pub struct RecordPageHeader {
    num_records: usize,
    next_free: u32,//page num of the next free page. Don't confuse this next_free with the one in BufferPage, the next_free in BufferPage is the index at the buffer_table of the next page, this is the page num of the next free page.
}

#[derive(Debug)]
pub struct RecordFileHandle {
    header_num: u32,//page num of the header, that's right, header is stored in one of the pages. When the page file is about to be closed, we use the header_num to get the page and copy the header of this handle into it.
    free: u32,//when all of the records of a page are deleted, the page is linked for later usage. 
    header: RecordFileHeader,
    pfh: PageFileHandle
}

impl RID {
    pub fn new(page_num: u32, slot_num: usize) -> Self {
        Self {
            page_num,
            slot_num,
        }
    }

    pub fn get_page_num(&self) -> u32 {
        self.page_num
    }

    pub fn get_slot_num(&self) -> usize {
        self.slot_num
    }
}

impl Record {
    pub fn new(record_size: usize, rid: RID, data: *mut u8) -> Self {
        Self {
            record_size,
            rid,
            data,
        }
    }
}

impl RecordPageHeader {
    pub fn new(num_records: usize, next_free: u32) -> Self {
        Self {
            num_records,
            next_free,
        }
    }
}

impl RecordFileHandle {
    pub fn new(header_num: u32, header: RecordFileHeader, pfh: &mut PageFileHandle) -> Self {
        Self {
            header_num,
            free: 0,
            header,
            pfh: pfh.clone()
        }
    }

    pub fn get_record(&mut self, rid: &RID) -> Result<Record, Error> {
        let ph = match self.pfh.get_page(rid.get_page_num()) {
            Err(e) => {
                return Err(e);
            },
            Ok(v) => v
        };
        let data = ph.get_data();
        let record_ptr = unsafe {
            data.offset(self.get_record_offset(rid.get_slot_num()))
        };
        let buffer = utils::allocate_buffer(self.header.record_size);
        unsafe {
            std::ptr::copy(record_ptr, buffer, self.header.record_size);
        }

        match self.pfh.unpin_page(ph.get_page_num()) {
            Ok(_) => Ok(Record::new(self.header.record_size, *rid, buffer)),
            Err(e) => Err(e)
        }
    }

    pub fn update_record(&mut self, rec: &Record) -> Result<(), Error> {
        let rid = rec.rid;
        let ph = match self.pfh.get_page(rid.get_page_num()) {
            Err(e) => {
                return Err(e);
            },
            Ok(v) => v
        };
        let data = ph.get_data();
        let record_ptr = unsafe {
            data.offset(self.get_record_offset(rid.get_slot_num()))
        };
        unsafe {
            std::ptr::copy(rec.data, record_ptr, rec.record_size);
        }

        match self.pfh.unpin_dirty_page(ph.get_page_num()) {
            Ok(_) => Ok(()),
            Err(e) => Err(e)
        }
    }

    pub fn delete_record(&mut self, rid: &RID) -> Result<(), Error> {
        let ph = match self.pfh.get_page(rid.get_page_num()) {
            Err(e) => {
                return Err(e);
            },
            Ok(v) => v
        };
        let data = ph.get_data();
        let record_ptr = unsafe {
            data.offset(self.get_record_offset(rid.get_slot_num()))
        };
        unsafe {
            std::ptr::write_bytes(record_ptr, 0, self.header.record_size);
        }
        if let Err(e) = self.set_bitmap(rid.slot_num, data, false) {
            dbg!(&e);
            self.pfh.unpin_dirty_page(ph.get_page_num());
            return Err(Error::SetBitmapError);
        }
        let rph = unsafe {
            &mut *(data as *mut RecordPageHeader)
        };
        rph.num_records -= 1;
        rph.next_free = self.free;
        self.free = rid.get_page_num();

        match self.pfh.unpin_dirty_page(ph.get_page_num()) {
            Ok(_) => Ok(()),
            Err(e) => Err(e)
        }
    }

    /*
     * Insert a record and returns its rid.
     * Choose a slot in the next_free page, if next_free = 0 or it's full,
     * allocate a new page and let next_free = new page num;
     */
    pub fn insert_record(&mut self, data: *mut u8) -> Result<RID, Error> {
        let mut slot_num: usize = 0;
        let mut ph = PageHandle::new(0, std::ptr::null_mut());
        let mut flag = true;
        let mut new_page = false;
        while self.free != 0 && flag {
            ph = match self.pfh.get_page(self.free) {
                Err(e) => {
                    return Err(e);
                },
                Ok(v) => v
            };
            let rph = unsafe {
                &mut *(ph.get_data() as *mut RecordPageHeader)
            };
            if rph.num_records > self.header.num_records_per_page {
                dbg!(&rph.num_records);
                panic!(true);
            }
            if rph.num_records == self.header.num_records_per_page {
                self.free = rph.next_free;
                rph.next_free = 0;
                self.pfh.unpin_dirty_page(ph.get_page_num());
                continue;
            }

            match self.find_free_slot(ph.get_data()) {
                Ok(v) => {
                    slot_num = v;
                    flag = false;
                },
                Err(e) => {
                    dbg!(&e);
                    self.pfh.unpin_dirty_page(ph.get_page_num());
                    return Err(Error::FindFreeSlotError);
                }
            }
        }

        if self.free == 0 {
            ph = match self.pfh.allocate_page() {
                Ok(v) => v,
                Err(e) => {
                    dbg!(&e);
                    return Err(e);
                }
            };
            new_page = true;
            self.free = ph.get_page_num();
            //when we find a free slot, the bit corresponding to the slot is set.
            //so we don't need to set bitmap again.
            match self.find_free_slot(ph.get_data()) {
                Ok(v) => {
                    slot_num = v;
                },
                Err(e) => {
                    self.pfh.unpin_dirty_page(ph.get_page_num());
                    return Err(Error::FindFreeSlotError);
                }
            }
        }

        let record_ptr = unsafe {
            ph.get_data().offset(self.get_record_offset(slot_num))
        };

        unsafe {
            std::ptr::copy(data, record_ptr, self.header.record_size);
        }

        let rph = unsafe {
            &mut *(ph.get_data() as *mut RecordPageHeader)
        };
        
        if new_page {
            rph.num_records = 1;
        } else {
            rph.num_records += 1;
        }

        match self.pfh.unpin_dirty_page(ph.get_page_num()) {
            Ok(_) => Ok(RID {
                page_num: ph.get_page_num(),
                slot_num: slot_num
            }),
            Err(e) => Err(e)
        }
    }

    //set a bit in the bitmap accroding to a slot_num, 
    //if set is true, set the bit, else unset.
    //An error is returned if the bit is already set or unset.
    fn set_bitmap(&mut self, slot: usize, data: *mut u8, set: bool) -> Result<(), RecordError> {
        let bitmap = unsafe {
            let p = data.offset(self.header.bitmap_offset as isize);
            std::slice::from_raw_parts_mut(p, self.header.bitmap_size)
        };
        let moder = slot/8;
        let remainder = slot - moder * 8;
        let num = &mut bitmap[moder];
        let bit: u8 = *num & ((1 as u8)<<(7-remainder));
        
        if set && bit == 1 {
            return Err(RecordError::BitSet);
        }
        if !set && bit == 0 {
            return Err(RecordError::BitUnset);
        }

        if set {
            *num |= ((1 as u8)<<(7-remainder));
        } else {
            let max: u8 = 0xff;
            let temp: u8 = max ^ ((1 as u8)<<(7-remainder));
            *num &= temp;
        }

        Ok(())
    }

    fn find_free_slot(&self, data: *mut u8) -> Result<usize, RecordError> {
        let bitmap = unsafe {
            let p = data.offset(self.header.bitmap_offset as isize);
            std::slice::from_raw_parts_mut(p, self.header.bitmap_size)
        };

        for i in 0..(self.header.num_records_per_page) {
            let index: usize = i/8;
            let offset = (i - index*8) as u8;
            if bitmap[index] & (1<<(7-offset)) == 0 {
                bitmap[index] += (1<<(7-offset));
                return Ok(i);
            }
        }
        Err(RecordError::FullPage)
    }

    //the offset of a specific record in a page.
    fn get_record_offset(&self, slot: usize) -> isize {
        (self.header.records_offset + slot*self.header.record_size) as isize
    }
}
