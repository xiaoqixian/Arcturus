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
use std::fs::File;
use std::ptr::NonNull;
use std::mem::size_of;

/*
 * We plan to represent a table using at lease one file.
 */

#[derive(Debug, Copy, Clone)]
pub struct RID {
    page_num: u32,
    slot_num: u32 //slot_num represents the location of a record in a page.
}

impl RID {
    pub fn new(page_num: u32, slot_num: u32) -> Self {
        RID {
            page_num,
            slot_num
        }
    }
}

#[derive(Debug)]
pub struct Record {
    record_size: usize,
    rid: RID,
    data: *mut u8
}

impl Clone for Record {
    fn clone(&self) -> Self {
        Record {
            record_size: self.record_size,
            rid: self.rid,
            data: {
                let p = BufferManager::allocate_buffer(self.record_size);
                unsafe {
                    std::ptr::copy(self.data, p, self.record_size);
                }
                p
            }
        }
    }
}

impl Record {
    fn new(record_size: usize, rid: RID) -> Self {
        Record {
            record_size,
            rid,
            data: BufferManager::allocate_buffer(record_size)
        }
    }

    fn new_with_data(record_size: usize, rid: RID, data: *mut u8) -> Self {
        Record {
            record_size,
            rid,
            data,
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
pub struct RecordManager {
    pub page_file_manager: PageFileManager,
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
        println!("Initializing Record Manager");
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
        let mut res = Box::new(Record::new(self.record_size, *rid));
        let page_pointer = self.page_file_manager.get_page(rid.page_num);
        if let None = page_pointer {
            //when the None is returned, the specific page is not pinned, so no need to
            //unpin the page.
            return Err(RecordError::GetPageError);
        }
        let page = unsafe {
            &mut *page_pointer.unwrap().as_ptr()
        };
        if let Err(e) = self.write_record(&mut res, page) {
            return Err(e);
        }

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
        page.mark_dirty();//important
        self.page_file_manager.unpin_page(rec.get_page_num());//important
        Ok(())
    }

    /*
     * Inserting a record takes a lot of work.
     * When we need to insert a new record, if there is space
     * in the last page, then we just insert a record in it.
     * If not, we need to allocate a new page.
     *
     * The rid also need to be allocated, after allocation, the 
     * rid is returned.
     */
    pub fn insert_record(&mut self, data: *mut u8) -> Result<RID, RecordError> {
        println!("Inserting a record");
        let mut page_num = self.page_file_manager.get_first_free();
        let mut page: &mut BufferPage;
        if page_num == 0 {
            println!("Need to allocate a new page.");
            let res = self.page_file_manager.allocate_page();
            if let Err(e) = res {
                dbg!(&e);
                return Err(RecordError::AllocatePageError);
            }
            page = unsafe {
                &mut *res.unwrap().as_ptr()
            };
        } else {
            let res = self.page_file_manager.get_page(page_num);
            if let None = res {
                return Err(RecordError::GetPageError);
            }
            page = unsafe {
                &mut *res.unwrap().as_ptr()
            };
        }
        page_num = page.get_page_num();
        let res = self.get_free_slot(page);
        let slot_num: u32;
        if let Err(RecordError::PageFull) = res {
            println!("");
            self.page_file_manager.unpin_page(page_num);
            let res = self.page_file_manager.allocate_page();
            if let Err(e) = res {
                dbg!(&e);
                return Err(RecordError::AllocatePageError);
            }
            page = unsafe {
                &mut *res.unwrap().as_ptr()
            };
            page_num = page.get_page_num();
            slot_num = self.get_free_slot(page).expect("Get free slot error after a new page is allocated.");
        } else {
            slot_num = res.unwrap();
        }
        dbg!(&slot_num);
        let rid = RID::new(page_num, slot_num);
        //copy data into page.
        let record_offset = self.get_record_offset(rid.slot_num).expect("Get Record Offset Error");
        dbg!(&record_offset);
        let record_ptr = unsafe {
            page.data.offset(record_offset as isize)
        };
        unsafe {
            std::ptr::copy(data, record_ptr, self.record_size);
        }
        page.mark_dirty();
        page.add_num_records();
        page.dbg_header();
        self.page_file_manager.unpin_page(page.get_page_num());
        Ok(rid)
    }

    /*
     * Delete a record.
     */
    pub fn delete_record(&mut self, rid: RID) -> Result<(), RecordError> {
        let page_num = rid.page_num;
        let slot_num = rid.slot_num;
        dbg!(&page_num);
        let res = self.page_file_manager.get_page(page_num);
        if let None = res {
            return Err(RecordError::GetPageError);
        }
        let page = unsafe {
            &mut *res.unwrap().as_ptr()
        };
        let bitmap_offset = size_of::<PageHeader>() as isize;
        let index = (slot_num / 8) as usize;
        let offset = slot_num % 8;
        let sli = unsafe {
            std::slice::from_raw_parts_mut(page.data.offset(bitmap_offset), self.page_file_manager.get_bitmap_size())
        };
        let temp = 1<<(7-offset);
        if sli[index] & temp == 0 {
            return Err(RecordError::RecordDeleted);
        }
        dbg!(&sli[index]);
        sli[index] ^= temp;
        dbg!(&sli[index]);
        page.dec_num_records();
        //now the page is definitely not full, we need to link it to the available page list.
        self.page_file_manager.link_page(page);
        page.mark_dirty();
        self.page_file_manager.unpin_page(page_num);
        Ok(())
    }

    /*
     * Copy the memory of a record from a page to a Record struct.
     * With memory copying method.
     */
    fn write_record(&self, record: &mut Box<Record>, page: &mut BufferPage) -> Result<(), RecordError> {
        unsafe {
            let res = self.get_record_offset(record.get_slot_num());
            if let Err(e) = res {
                dbg!(&e);
                return Err(e);
            }
            let record_offset = res.unwrap();
            let record_slot = page.data.offset(record_offset as isize);
            std::ptr::copy(record_slot, record.data, self.record_size);
        }
        Ok(())
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
        let res = self.get_record_offset(record.get_slot_num());
        if let Err(e) = res {
            return Err(e);
        }
        let record_offset = res.unwrap();
        
        unsafe {
            std::ptr::copy(record.data, page.data.offset(record_offset as isize), self.record_size);
        }
        Ok(())
    }

    /*
     * Get a free slot of a page, normally we call this method for
     * inserting a record, so we directly set the corresponding bit
     * in the bitmap.
     */
    pub fn get_free_slot_(&self, page: &BufferPage) -> Result<u32, RecordError> {
        let bitmap_offset = size_of::<PageHeader>() as isize;
        dbg!(&bitmap_offset);
        dbg!(size_of::<PageHeader>());
        let bitmap = unsafe {
            page.data.offset(bitmap_offset)
        };
        let bitmap_size = self.page_file_manager.get_bitmap_size();
        let sli = unsafe {
            std::slice::from_raw_parts_mut(bitmap, bitmap_size)
        };
        println!("{:?}", sli);
        Ok(32)
    }

    pub fn get_free_slot(&self, page: &BufferPage) -> Result<u32, RecordError> {
        let bitmap_offset = size_of::<PageHeader>() as isize;
        dbg!(&bitmap_offset);
        let bitmap = unsafe {
            page.data.offset(bitmap_offset)
        };
        dbg!(&bitmap);
        let bitmap_size = self.page_file_manager.get_bitmap_size();
        let sli = unsafe {
            std::slice::from_raw_parts_mut(bitmap, bitmap_size)
        };
        println!("{:?}", &sli);
        let mut index: usize = 0xffffffff;
        for i in 0..(bitmap_size as usize) {
            if sli[i] < 0xff {
                index = i;
                break;
            }
        }
        if index == 0xffffffff {
            return Err(RecordError::PageFull);
        }
        let mut temp = sli[index];
        dbg!(&index);
        dbg!(&sli[index]);
        let mut res: usize = index * 8;
        for i in 0..(8 as usize) {
            dbg!(&temp);
            if temp & 0x80 == 0 {
                dbg!(&i);
                res += i;
                let s = 1<<(7-i);
                sli[index] |= s;
                dbg!(&sli[index]);
                break;
            }
            temp <<= 1;
        }
        println!("{:?}", &sli);
        Ok(res as u32)
    }

    pub fn get_record_offset(&self, slot_num: u32) -> Result<usize, RecordError> {
        let offset = std::mem::size_of::<PageHeader>() + self.page_file_manager.get_bitmap_size() + (slot_num as usize) * self.record_size;
        if offset > self.page_file_manager.get_pagesize() {
            return Err(RecordError::OffsetError);
        }
        Ok(offset)
    }
}
