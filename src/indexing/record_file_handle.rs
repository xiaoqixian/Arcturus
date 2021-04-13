/**********************************************
  > File Name		: record_file_handle.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Mon 12 Apr 2021 11:01:57 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

use crate::utils;

#[derive(Debug, Copy, Clone)]
pub struct RID {
    page_num: u32,
    slot_num: u32
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
struct RecordFileHeader {
    record_size: usize,
    bitmap_offset: usize,
    bitmap_size: usize,
    records_offset: usize,
    num_records_per_page: usize,
    num_pages: usize
}


#[derive(Debug, Copy, Clone)]
pub struct RecordPageHeader {
    num_records: usize,
    next_free: u32,//page num of the next free page.
}

#[derive(Debug)]
pub struct RecordFileHandle {
    header_num: u32,//page num of the header, that's right, header is stored in one of the pages. When the page file is about to be closed, we use the header_num to get the page and copy the header of this handle into it.
    header: RecordFileHeader,
    pfh: PageFileHandle
}

impl RID {
    pub fn get_page_num(&self) -> u32 {
        self.page_num
    }

    pub fn get_slot_num(&self) -> u32 {
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
    pub fn new(header_num: u32, header: RecordFileHeader, pfh: &PageFileHandle) -> Self {
        Self {
            header_num,
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
        let mut buffer = utils::allocate_buffer(self.record_size);
        unsafe {
            std::ptr::copy(record_ptr, buffer, self.record_size);
        }
        Ok(Record::new(self.record_size, *RID, buffer))
    }

    //the offset of a specific record in a page.
    fn get_record_offset(&self, slot: u32) -> isize {
        (self.header.records_offset + slot*self.record_size) as isize
    }
}
