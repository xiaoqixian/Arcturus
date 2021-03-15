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

use crate::page_management::page_file::*;
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
    data: *mut u8
}

impl Record {
    fn new(record_size: usize, rid: RID) -> Self {
        Record {
            record_size,
            rid,
            data: BufferManager::allocate_buffer(record_size)
        }
    }

    fn get_page_num(&self) -> u32 {
        self.rid.page_num
    }

    fn get_slot_num(&self) -> u32 {
        self.rid.slot_num
    }
}

/*
 * RecordManager is in charge of inserting, deleting, getting a record 
 * from a page.
 */
struct RecordManager {
    page_file_manager: PageFileManager,
    fp: Option<File>,
    record_size: usize
}

/*
 * When we create a new manager to manage records.
 * Either we create a new DB file to represent a 
 * table, or open an old file. Both ways request 
 * us to pass in a reference of File.
 */
impl RecordManager {
    pub fn new(fp: &File, record_size: usize) -> Self {
        RecordManager {
            page_file_manager: PageFileManager::new(fp),
            fp: Some(fp.try_clone().unwrap()),
            record_size,
        }
    }

    pub fn get_record(&mut self, rid: &RID) -> Result<NonNull<Record>, RecordError> {
        if let None = self.fp {
            return Err(RecordError::NoFilePointer);
        }
        let page_pointer = self.page_file_manager.get_page(rid.page_num);
        if let None = page_pointer {
            return Err(RecordError::GetPageError);
        }
        let page = unsafe {
            &mut *page_pointer.unwrap().as_ptr()
        };
        let offset = self.get_record_offset(rid.slot_num);
        if offset > self.page_file_manager.get_pagesize() {
            return Err(RecordError::OffsetError);
        }
        if offset % self.record_size != 0 {
            return Err(RecordError::MismatchRecordOffset);
        }
        let mut res = Box::new(Record::new(self.record_size, *rid));
        self.write_record(&mut res, page);

        self.page_file_manager.unpin_page(rid.page_num);//important.
        Ok(NonNull::new(Box::into_raw(res)).unwrap())
    }

    /*
     * get_record doesn't necessarily modify the data of the record.
     * but update_record definitely modify the data of the record.
     * so we have to call the mark_dirty method inside.
     */
    pub fn update_record(&mut self, record: NonNull<Record>) -> Result<(), RecordError> {
        if let None = self.fp {
            return Err(RecordError::NoFilePointer);
        }
        let rec = unsafe {
            record.as_ref()
        };
        let offset = self.get_record_offset(rec.rid.slot_num);
        if offset > self.page_file_manager.get_pagesize() {
            return Err(RecordError::OffsetError);
        }
        let page_pointer = self.page_file_manager.get_page(rec.rid.page_num);
        if let None = page_pointer {
            return Err(RecordError::GetPageError);
        }
        let page = unsafe {
            &mut *page_pointer.unwrap().as_ptr()
        };
        match self.write_page(record, page) {
            Err(e) => {
                dbg!(e);
                panic!("write page error");
            },
            Ok(()) => {}
        }
        page.mark_dirty();
        self.page_file_manager.unpin_page(rec.rid.page_num);
        Ok(())
    }

    /*
     * Inserting a record takes a lot of work.
     * When we need to insert a new record, if there is space
     * in the last page, then we just insert a record in it.
     * If not, we need to allocate a new page.
     *
     * The BufferManager is not able to allocate or delete a 
     * page, therefore, we need to create a page_file struct
     * to manage pages in files. Then we dont' directly ask 
     * for a page from the buffer, we ask for a page from 
     * the page file manager. And the manager has the buffer.
     */
    pub fn insert_record(&mut self, record: NonNull<Record>) {
        
    }

    /*
     * Copy the memory of a record from a page to a Record struct.
     * With memory copying method.
     */
    fn write_record(&self, record: &mut Box<Record>, page: &mut BufferPage) {
        unsafe {
            let record_offset = self.get_record_offset(record.get_slot_num());
            let record_slot = unsafe {
                page.data.offset(record_offset as isize)
            };
            std::ptr::copy(record_slot, record.data, self.record_size);
        }
    }

    /*
     * write a record data into the certain location of a page.
     */
    fn write_page(&self, record_p: NonNull<Record>, page: &mut BufferPage) -> Result<(), RecordError> {
        let record = unsafe {
            &mut *record_p.as_ptr()
        };
        if record.data.is_null() || page.data.is_null() {
            return Err(RecordError::NullPointerError);
        }
        let record_offset = self.get_record_offset(record.get_slot_num());
        
        unsafe {
            std::ptr::copy(record.data, page.data.offset(record_offset as isize), self.record_size);
        }
        Ok(())
    }

    fn get_record_offset(&self, slot_num: u32) -> usize {
        std::mem::size_of::<PageHeader>() + (slot_num as usize) * self.record_size
    }
}
