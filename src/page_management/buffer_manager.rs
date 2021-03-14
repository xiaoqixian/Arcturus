/**********************************************
  > File Name		: buffer_manager.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Mon 01 Mar 2021 07:52:27 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

use std::fs::File;
use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::FileExt;
use std::ptr::{self, NonNull};
use std::fs::OpenOptions;
use std::mem::size_of;

use crate::errors::PageFileError;
use super::page_file;
use log::{*};
extern crate env_logger;

use std::{println as info, println as debug, println as warn, println as error};
/*
 * Memory and References.
 * Let me explain how I resolve memory passing between functions
 * and references lifetimes.
 * 
 * Buffer Pages Storage:
 * All buffer pages are allocated on heap, and the pointers are
 * stored in a vector.
 * You may wonder why I don't use Box to store the pointers 
 * which is safe. 
 * However, in that way, one page can only be refered mutablly 
 * once. And what we need is one page can be multiplly refered 
 * mutablly, because multiple records exist in one page and we
 * need be able to write to a same page simultaneously.
 * In Rust, only raw pointers are allowed for multiple mutable 
 * references. So they are our choices.
 */

/*
 * Data structure to represent a page.
 * Notice that the data structure for a page in main memory 
 * is different from it in a file.
 */

#[derive(Debug)]
pub struct BufferPage {
    pub data: *mut u8,//data including page header, bitmap, and the records data.
    next: i32,
    prev: i32,
    dirty: bool,
    pin_count: u32,
    page_num: u32,
    fp: Option<File>
}

impl BufferPage {
    pub fn new() -> Self {
        BufferPage {
            data: ptr::null_mut(),
            next: -1,
            prev: -1,
            dirty: false,
            pin_count: 0,
            page_num: 0, //o is an invalid page number, so we use it for page initialization.
            fp: None
        }
    }

    pub fn clone_metadata(&self) -> Self {
        BufferPage {
            data: ptr::null_mut(),
            next: self.next,
            prev: self.prev,
            dirty: self.dirty,
            pin_count: self.pin_count,
            page_num: self.page_num,
            fp: {
                match &self.fp {
                    None => None,
                    Some(v) => {
                        Some(v.try_clone().unwrap())
                    }
                }
            }
        }
    }

    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    pub fn page_num(&self) -> u32 {
        self.page_num
    }

    pub fn get_next_free(&self) -> u32 {
        unsafe {
            (*(self.data as *const page_file::PageHeader)).next_free
        }
    }

    pub fn set_next_free(&mut self, num: u32) {
        unsafe {
            (*(self.data as *mut page_file::PageHeader)).next_free = num;
        }
    }

    pub fn init_page_header(&mut self, page_num: u32) {
        let header = unsafe {
            &mut *(self.data as *mut page_file::PageHeader)
        };
        header.page_num = page_num;
        header.num_records = 0;
        header.next_free = 0;
        self.dirty = true;//at least the header is dirty.
    }
}

/*
 * Accessing data on a page of a file requires first reading 
 * the page into a buffer pool in main memory. While a page 
 * is in memory and its data is available for manipulation, 
 * the page is said to be "pinned". After the manipulation 
 * is done, the page is "unpinned". Unpinning a page does 
 * not necessarily cause the page to be remove from the buffer.
 * An unpinned page is kept in memory as long as its space in 
 * the buffer pool is not needed.
 *
 * It is important not to leave pages in memory unnecessarily.
 */
#[derive(Debug, Clone)]
pub struct BufferManager {
    buffer_table: Vec<NonNull<BufferPage>>, 
    num_pages: u32, //number of pages in the buffer pool, free pages not included.
    page_size: usize,//this page_size is the real page size, including page header, bitmap, and the data field. Will be provided when the buffer is created.
    first: i32, //most recently used page number.
    last: i32, //last recently used page number.
    /* page number of the first free page.
     * all free pages are linked by the page 
     * in their data structure.*/
    free: i32,
    page_table: HashMap<u32, usize> //we need this table to get a page quickly.
}

impl BufferManager {
    pub fn new(page_size: usize) -> Self {
        BufferManager {
            buffer_table: {
                //let mut v = vec![NonNull::new(Box::into_raw(Box::new(BufferPage::new()))).unwrap(); 128];
                let mut v: Vec<NonNull<BufferPage>> = Vec::with_capacity(128);
                for i in 0..127 {
                    let mut temp = Box::new(BufferPage::new());
                    temp.next = i+1;
                    v.push(NonNull::new(Box::into_raw(temp)).unwrap());
                }
                v.push(NonNull::new(Box::into_raw(Box::new(BufferPage::new()))).unwrap());
                //link all free pages
                debug!("buffer initial length = {}", v.len());
                v
            },
            num_pages: 0,
            page_size,
            first: -1,
            last: -1,
            free: 0,
            page_table: HashMap::new()
        }
    }

    pub fn get_pagesize(&self) -> usize {
        self.page_size
    }

    fn resize_buffer(&mut self) {
        let cap = self.buffer_table.capacity() as u32;
        if self.num_pages < cap {
            debug!("No need to resize buffer");
            return ;
        }
        dbg!((cap<<1) as usize);
        let mut new_table: Vec<NonNull<BufferPage>> = Vec::with_capacity((cap<<1) as usize);
        debug!("Not crashed yet.");
        for i in 0..cap {
            new_table[i as usize] = self.buffer_table[i as usize];
        }
        self.buffer_table = new_table;
        info!("Buffer pool new capacity: {}", self.buffer_table.capacity());
        //link all free pages.
        let start = cap as i32;
        for i in 0..(start-1) {
            unsafe {
                let mut new_page = Box::new(BufferPage::new());
                new_page.next = start+i+1;
                self.buffer_table[(start+i) as usize] = NonNull::new(Box::into_raw(new_page)).unwrap();
            }
        }
        self.buffer_table[((cap<<1) - 1) as usize] = NonNull::new(Box::into_raw(Box::new(BufferPage::new()))).unwrap();
        self.free = start;
    }

    fn get_page_offset(&self, index: usize) -> u64 {
        (size_of::<page_file::PageFileHeader>() + index * self.page_size) as u64
    }

    /*
     * read page header and data.
     */
    fn read_page(&mut self, page_num: u32, fp: &File, index: usize) -> Result<(), PageFileError> {
        let file_page_index = (page_num & 0x0000ffff) as usize;
        let buffer_page = unsafe {
            &mut *self.buffer_table[index].as_ptr()
        };
        
        if buffer_page.data.is_null() {
            //let mut temp: Box<Vec<u8>> = Box::new(vec![0; self.page_size]);
            let mut temp: [u8; 5000] = [0; 5000];
            buffer_page.data = temp.as_mut_ptr();
        }
        debug!("page data allocated ");

        let sli = unsafe {
            std::slice::from_raw_parts_mut(buffer_page.data, self.page_size)
        };
        let res = fp.read_at(sli, self.get_page_offset(file_page_index));
        dbg!(&res);

        if let Err(_) = res {
            return Err(PageFileError::ReadAtError);
        }
        
        let read_bytes = res.unwrap();
        if read_bytes < self.page_size {
            return Err(PageFileError::IncompleteRead);
        }
        
        Ok(())
    }

    /*
     * Write a page back to its file.
     * There's a problem: We may read records from multiple files.
     * So how should we allocate page numbers so page numbers won't conincide
     * and we can know the location that the page in its file from its page 
     * number?
     * 
     * Here's my solution: 
     * As a page number is a 32 bits integer, we can split it up as 16 bits and
     * 16 bits. The left 16 bits are to represent the file number, and the right
     * 16 bits are to represent the location. After all, there is a tiny 
     * possibility that there are more than 1<<16 pages in one file.
     * In this way, we can make sure each page number is identical.
     */
    fn write_page(&self, page_num: u32, fp: &File, index: usize) -> Result<(), PageFileError> {
        let file_page_index = (page_num & 0x0000ffff) as usize;
        let buffer_page = unsafe {
            &mut *self.buffer_table[index].as_ptr()
        };

        if buffer_page.data.is_null() {
            return Err(PageFileError::DataUnintialized);
        }

        let sli = unsafe {
            std::slice::from_raw_parts(buffer_page.data, self.page_size)
        };
        let res = fp.write_at(sli, self.get_page_offset(file_page_index));

        if let Err(v) = res {
            dbg!(v);
            return Err(PageFileError::WriteAtError);
        }

        let write_bytes = res.unwrap();
        if write_bytes < self.page_size {
            return Err(PageFileError::IncompleteWrite);
        }

        Ok(())
    }

    /*
     * Free a page in buffer, the page must be unpinned.
     * Method needed when there is no free page.
     */
    fn free_page(&mut self, index: usize) -> Result<(), PageFileError> {
        let temp = -1;
        if index == (temp as usize) {
            return Err(PageFileError::NoPage);
        }
        if index > self.buffer_table.len() {
            return Err(PageFileError::OutOfIndex);
        }
        let page = unsafe {
            self.buffer_table[index].as_ref()
        };
        if page.pin_count != 0 {
            return Err(PageFileError::PagePinned);
        }
        if (self.first as usize) != index && page.prev == -1 {
            //means the page is in the free list.
            return Err(PageFileError::PageFreed);
        }
        if page.dirty {
            if let None = &page.fp {
                return Err(PageFileError::NoFilePointer);
            }
            let res = self.write_page(page.page_num, &page.fp.as_ref().unwrap(), index);
            if let Ok(()) = res {
                
            } else {
                return res;
            }
        }
        self.unlink(index);
        let page = unsafe {
            self.buffer_table[index].as_mut()
        };
        self.page_table.remove(&page.page_num);
        //set the new free page.
        page.dirty = false;
        page.page_num = 0;
        //link the page to the free list.
        page.next = self.free;
        page.prev = -1;
        page.fp = None;
        self.free = index as i32;
        self.num_pages -= 1;
        Ok(())
    }

    /*
     * Unlink a page from the unused list.
     */
    fn unlink(&mut self, index: usize) {
        let page = unsafe {
            & *self.buffer_table[index].as_ptr()
        };
        let int_index = index as i32;
        if page.prev == -1 {
            self.first = page.next;
        } else {
            unsafe {
                self.buffer_table[page.prev as usize].as_mut().next = page.next;
            }
        }
        if page.next == -1 {
            self.last = page.prev;
        } else {
            unsafe {
                self.buffer_table[page.next as usize].as_mut().prev = page.prev;
            }
        }
    }

    fn link(&mut self, index: usize) {
        let page = unsafe {
            &mut *self.buffer_table[index].as_ptr()
        };
        page.next = self.first;
        if self.first != -1 {
            unsafe {
                self.buffer_table[self.first as usize].as_mut().prev = index as i32;
            }
        }
        self.first = index as i32;
        if self.last == -1 {
            debug!("self.last points to {}", index);
            self.last = index as i32;
        }
        //dbg!(page.clone_metadata());
    }

    /*
     * When a page is read from the buffer, we need to update
     * some data of the page and the buffer pool.
     * If the page is already pinned, then increase pin count by
     * 1 and return.
     * If it is in the unused list, then unlink it from the 
     * unused list.
     */
    fn update_page(&mut self, index: usize) {
        let pin_count: u32;
        let prev: i32;
        let next: i32;
        {
            let page = unsafe {
                self.buffer_table[index].as_ref()
            };
            pin_count = page.pin_count;
            prev = page.prev;
            next = page.next;
        }
        if pin_count > 1 {
            unsafe {
                (*self.buffer_table[index].as_ptr()).pin_count += 1;
            }
            return ;
        }
        //remove the page from the unused list.
        if prev == -1 {
            self.first = next;
        } else {
            unsafe {
                (*self.buffer_table[prev as usize].as_ptr()).next = next;
            }
        }
        if next == -1 {
            self.last = prev;
        } else {
            unsafe {
                (*self.buffer_table[next as usize].as_ptr()).prev = prev;
            }
        }
        let page = unsafe {
            self.buffer_table[index].as_mut()
        };
        page.pin_count += 1;
        page.prev = -1;
        page.next = -1;
    }

    fn internal_alloc(&mut self) -> Result<usize, PageFileError> {
        if self.free == -1 {
            debug!("No free pages");
            dbg!(&self.last);
            match self.free_page(self.last as usize) {
                Ok(()) => {},
                Err(PageFileError::NoPage) => {
                    debug!("resizing buffer");
                    self.resize_buffer();
                },
                Err(e) => {
                    return Err(e);
                }
            }
        }
        let temp = self.free as usize;
        unsafe {
            self.free = self.buffer_table[temp].as_mut().next;
            dbg!(&self.free);
        }
        self.num_pages += 1;
        Ok(temp)
    }

    pub fn get_page(&mut self, page_num: u32, fp: &File) -> Option<NonNull<BufferPage>> {
        let cap = self.buffer_table.capacity();
        let index: usize = match self.page_table.get(&page_num) {
            None => cap,//index cannot be equal to or greater than the buffer_table capacity.
            Some(v) => *v
        };
        if index < cap {
            debug!("Getting page with page_num={:#010x} from buffer", page_num);
            self.update_page(index);
            Some(self.buffer_table[index])
        } else {
            debug!("Reading page with page_num={:#010x} from file.", page_num);
            
            let res = self.internal_alloc();
            dbg!(&res);
            if let Err(v) = res {
                dbg!(v);
                return None;
            }
            let newpage_index = res.unwrap();
            match self.read_page(page_num, fp, newpage_index) {
                Ok(()) => {},
                Err(PageFileError::ReadAtError) => {
                    error!("read_at function error.");
                    return None;
                },
                Err(PageFileError::IncompleteRead) => {
                    error!("read_at function IncompleteRead");
                    return None;
                }
                Err(PageFileError::DestShort) => {
                    error!("Unexpected error: page data length is too short");
                    return None;
                },
                Err(_) => {
                    error!("Unexpected error occured");
                    return None;
                }
            }
            debug!("read page succeed");

            self.page_table.insert(page_num, newpage_index);
            dbg!(&self.page_table);
            debug!("I guess here");
            let new_page = unsafe {&mut *self.buffer_table[newpage_index].as_ptr()};
            debug!("read error");
            new_page.next = -1;
            new_page.pin_count = 1;
            new_page.page_num = page_num;
            new_page.fp = Some(fp.try_clone().unwrap());
            Some(self.buffer_table[newpage_index])
        }
    }

    /*
     * Allocate a page in the buffer, the page never occurs in
     * the buffer or file. 
     * When the page is allocated in the buffer, it is not 
     * allocated in the file yet. 
     * And when the page is unpinned and get written back to 
     * the file, the page is officially allocated in the file.
     *
     * Also, the newpage will not be initialized. The 
     * initialization work will be done when the page is used.
     */
    pub fn allocate_page(&mut self, page_num: u32, fp: &File) -> Option<NonNull<BufferPage>> {
        if let Some(_) = self.page_table.get(&page_num) {
            debug!("The page is in the buffer");
            dbg!(page_num);
        }
        let res = self.internal_alloc();
        if let Err(e) = res {
            dbg!(e);
            return None;
        }
        let newpage_index = res.unwrap();
        self.page_table.insert(page_num, newpage_index);
        let page = unsafe {
            &mut *self.buffer_table[newpage_index].as_ptr()
        };
        page.page_num = page_num;
        page.fp = Some(fp.try_clone().unwrap());
        page.pin_count = 1;
        Some(self.buffer_table[newpage_index])
    }

    /*
     * Unpin a page.
     * When an operation to a page is done, the function that calls 
     * the get_page method need to unpin the page.
     * If the pin count of a page decreases to 0, the page will be 
     * linked to the unused list.
     *
     * This is also a public interface, so internal errors won't get
     * passed out either.
     */
    pub fn unpin(&mut self, page_num: u32) {
        let index: usize;
        match self.page_table.get(&page_num) {
            None => {
                error!("No such page in the buffer, must be wrong page number");
                return ;
            },
            Some(v) => {
                index = *v;
            }
        }
        let page = unsafe {
            &mut *self.buffer_table[index].as_ptr()
        };
        if page.pin_count == 0 {
            debug!("The page is already unpinned");
            return ;
        }
        page.pin_count -= 1;
        if page.pin_count == 0 {
            self.link(index);
        }
    }

    /*
     * Release all pages that belong to a same file as the same 
     * time. All pages must be unpinned.
     * Useful when we need to exit the database.
     * 
     * Main problem: How we can determine if a page belongs to a file?
     * By referencing the same_file crate, comparing the metadata of 
     * a file can be an appropriate method.
     */
    pub fn flush_pages(&self, fp: &File) {
        
    }
}


