/**********************************************
  > File Name		: mod.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time     : Wed Mar 10 07:25:33 PM CST 2021
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

/*
 * The Record Management component provides classes and methods for managing
 * files of unordered records. 
 * 
 * We will store records in paged files provided by the PageFile component.
 * To manage file contents conveniently, we will use the first page of each
 * file as a special header page which contains free space information.
 *
 * To simplify our task, we assume that every record in one page file are
 * the same size. Although record sizes may differ across files. 
 */

use crate::page_management::buffer_manager::*;
use crate::errors::RecordError;
use super::file_manager::FileHeader;
use std::fs::File;
use std::ptr::NonNull;

/*
 * We plan to represent a table using at lease one file.
 */

#[derive(Debug, Copy, Clone)]
struct RID {
    page_num: u32,
    slot_num: u32 //slot_num represents the location of a record in a page.
}

#[derive(Debug, Clone)]
struct Record {
    record_size: usize,
    rid: RID,
    data: Vec<u8>
}

impl Record {
    fn new(record_size: usize, rid: RID) -> Self {
        Record {
            record_size,
            rid,
            data: vec![0; record_size]
        }
    }
}

/*
 * RecordManager is in charge of inserting, deleting, getting a record 
 * from a page.
 */
struct RecordManager {
    buffer: BufferManager,
    fp: Option<File>,
    header: FileHeader,
    record_size: usize
}

impl RecordManager {
    pub fn new(record_size: usize) -> Self {
        RecordManager {
            buffer: BufferManager::new(),
            fp: None,
            header: FileHeader::new(),
            record_size,
        }
    }

    pub fn get_record(&mut self, rid: &RID) -> Result<NonNull<Record>, RecordError> {
        if let None = self.fp {
            return Err(RecordError::NoFilePointer);
        }
        let page_pointer = self.buffer.get_page(rid.page_num, &self.fp.as_ref().unwrap());
        if let None = page_pointer {
            return Err(RecordError::ExternalMethodsFailure);
        }
        let page = unsafe {
            &mut (*page_pointer.unwrap().as_ptr())
        };
        let offset = self.get_record_offset(rid.slot_num);
        if offset > self.buffer.get_pagesize() {
            return Err(RecordError::OffsetError);
        }
        let mut res = Box::new(Record::new(self.record_size, *rid));
        self.write_record(&mut res, &page.data[offset..]);

        self.buffer.unpin(rid.page_num);//important.
        Ok(NonNull::new(Box::into_raw(res)).unwrap())
    }

    /*
     * Write a vector slice into a record data field.
     * With memory copying method.
     */
    fn write_record(&self, record: &mut Box<Record>, data: &[u8]) {
        unsafe {
            std::ptr::copy(data.as_ptr(), record.data.as_mut_ptr(), self.record_size);
        }
    }

    fn get_record_offset(&self, slot_num: u32) -> usize {
        let record_size = self.record_size;
        let record_offset = std::mem::size_of::<FileHeader>() + (slot_num as usize) * record_size;
        record_offset
    }
}
