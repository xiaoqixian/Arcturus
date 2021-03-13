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

//use std::{println as info, println as debug, println as warn, println as error};
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
    pub fn new(page_size: usize) -> Self {
        BufferPage {
            data: ptr::null();
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
            data: ptr::null(),
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

    /*
     * clean a page's header and bitmap.
     * Normally a reset page will be linked in the free list of 
     * page file manager, so the next_free parameter is 
     * provided.
     */
    pub fn reset_page(&mut self, next_free: u32, bitmap_size: usize) -> Result<(), PageFileError> {
        self.page_header.num_records = 0;
        self.next_free = next_free;
        let bits: Vec<u8> = vec![0; bitmap_size];
        unsafe {
            std::ptr::copy(bits.as_ptr(), )
        }
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
                let mut v = Vec::with_capacity(128);
                for i in 0..128 {
                    v.push(NonNull::new(Box::into_raw(Box::new(BufferPage::new()))).unwrap());
                }
                //link all free pages
                let len = v.len() as i32;
                debug!("buffer initial length = {}", len);
                for i in 0..(len-1) {
                    unsafe {
                        v[i as usize].as_mut().next = i+1;
                    }
                }
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
            return ;
        }
        self.buffer_table.resize((cap << 1) as usize, NonNull::new(Box::into_raw(Box::new(BufferPage::new()))).unwrap());
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
        (size_of::<PageFileHeader>() + index * self.page_size) as u64
    }

    /*
     * read page header and data.
     */
    fn read_page(&mut self, page_num: u32, fp: &File, index: usize) -> PageFileError {
        let file_page_index = (page_num & 0x0000ffff) as usize;
        let buffer_page = unsafe {
            &mut *self.buffer_table[index].as_ptr()
        };
        
        if buffer_page.data.is_null() {
            buffer_page.data = vec![0; self.page_size].as_mut_ptr();
        }

        let sli = unsafe {
            std::slice::from_raw_parts_mut(buffer_page.data, self.page_size)
        };
        let res = fp.read_at(sli, self.get_page_offset(file_page_index));

        if let Err(_) = res {
            return PageFileError::ReadAtError;
        }
        
        let read_bytes = res.unwrap();
        if read_bytes < self.page_size {
            return PageFileError::IncompleteRead;
        }
        
        PageFileError::Okay
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
    fn write_page(&self, page_num: u32, fp: &File, index: usize) -> PageFileError {
        let file_page_index = (page_num & 0x0000ffff) as usize;
        let buffer_page = unsafe {
            &mut *self.buffer_table[index].as_ptr()
        };

        if buffer_page.data.is_null() {
            return PageFileError::DataUnintialized.
        }

        let sli = unsafe {
            std::slice::from_raw_parts(buffer_page.data, self.page_size)
        };
        let res = fp.write_at(sli, self.get_page_offset(file_page_index));

        if let Err(v) = res {
            dbg!(v);
            return PageFileError::WriteAtError;
        }

        let write_bytes = res.unwrap();
        if write_bytes < self.page_size {
            return PageFileError::IncompleteWrite;
        }

        PageFileError::Okay
    }

    /*
     * Free a page in buffer, the page must be unpinned.
     * Method needed when there is no free page.
     */
    fn free_page(&mut self, index: usize) -> PageFileError {
        let temp = -1;
        if index == (temp as usize) {
            return PageFileError::NoPage;
        }
        if index > self.buffer_table.len() {
            return PageFileError::OutOfIndex;
        }
        let page = unsafe {
            self.buffer_table[index].as_ref()
        };
        if page.pin_count != 0 {
            return PageFileError::PagePinned;
        }
        if (self.first as usize) != index && page.prev == -1 {
            //means the page is in the free list.
            return PageFileError::PageFreed;
        }
        if page.dirty {
            if let None = &page.fp {
                return PageFileError::NoFilePointer;
            }
            let res = self.write_page(page.page_num, &page.fp.as_ref().unwrap(), index);
            if let PageFileError::Okay = res {

            } else {
                return res;
            }
        }
        self.unlink(index);
        let page = unsafe {
            self.buffer_table[index].as_mut()
        };
        //clear all the data in the buffer page.
        unsafe {
            std::ptr::write_bytes(page.data.as_mut_ptr(), 0x00, page.data.capacity());
        }
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
        PageFileError::Okay
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
            if self.free == -1 {
                debug!("No free pages");
                debug!("Free page with index={}", self.last as usize);
                match self.free_page(self.last as usize) {
                    PageFileError::Okay => {},
                    PageFileError::OutOfIndex => {
                        error!("Trying to free page with index={}", index);
                    }
                    PageFileError::Unix => {
                        error!("Unix read error.");
                        return None;
                    },
                    PageFileError::IncompleteRead => {
                        error!("Incomplete read when read a new page.");
                        return None;
                    },
                    PageFileError::HashPageExist => {
                        error!("HashPageExist: This is impossible.");
                        return None;
                    },
                    PageFileError::NoPage => {
                        self.resize_buffer();
                    },
                    PageFileError::PageFreed => {
                        debug!("Tha page is already freed.");
                    },
                    PageFileError::PagePinned => {
                        debug!("The page is pinned");
                    },
                    PageFileError::NoFilePointer => {
                        debug!("The page lost its file pointer ");
                        return None;
                    },
                    PageFileError::WriteAtError => {
                        debug!("write_at method error");
                        return None;
                    },
                    _ => {
                        eprintln!("Unexpected errors happend");
                        return None;
                    }
                }
            }
            let newpage_index = self.free as usize;
            match self.read_page(page_num, fp, newpage_index) {
                PageFileError::Okay => {},                    
                PageFileError::ReadAtError => {
                    error!("read_at function error.");
                    return None;
                },
                PageFileError::IncompleteRead => {
                    error!("read_at function IncompleteRead");
                    return None;
                }
                PageFileError::DestShort => {
                    error!("Unexpected error: page data length is too short");
                    return None;
                },
                _ => {
                    error!("Unexpected error occured");
                    return None;
                }
            }
            self.page_table.insert(page_num, newpage_index);
            self.num_pages += 1;
            let new_page = unsafe {&mut *self.buffer_table[newpage_index].as_ptr()};
            self.free = new_page.next;
            debug!("self.free = {}", self.free);
            new_page.next = -1;
            new_page.pin_count = 1;
            new_page.page_num = page_num;
            new_page.fp = Some(fp.try_clone().unwrap());
            Some(self.buffer_table[newpage_index])
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    
    /*
     * Test1:
     * Read 129 pages, none of them get unpinned, see if the buffer
     * will resized.
     */
//    #[test]
    //fn buffer_manager_test1() {
        //let mut buffer = BufferManager::new();
        //let f = OpenOptions::new().read(true).write(true).open("/home/lunar/Documents/fzf").unwrap();
        //let file_num: u32 = 1<<16;
        //for i in 0..129 {
            //match buffer.get_page(file_num | (i as u32), &f) {
                //None => {
                    //panic!("read page_num={:#010x} failed", file_num|(i as u32));
                //},
                //Some(_) => {}
            //}
        //}
    //}

    /*
     * Test2:
     * Read 128 pages, make them dirty, unpin half of them, then read another 128 pages.
     */
    //#[test]
    //fn buffer_manager_test2() {
        //let mut buffer = BufferManager::new();
        //let mut f = OpenOptions::new().read(true).write(true).open("/home/lunar/Documents/fzf").unwrap();
        //let file_num: u32 = 1<<16;
        //for i in 0..128 {
            //match buffer.get_page(file_num | (i as u32), &f) {
                //None => {
                    //panic!("read page_num={:#010x} failed", file_num|(i as u32));
                //},
                //Some(v) => {
                    //v.data[0] = 127;
                    //v.dirty = true;
                //}
            //}
        //}
        //for i in 0..64 {
            //debug!("unpin page_num={:#010x}", file_num | (i as u32));
            //buffer.unpin(file_num | (i as u32));
        //}
        //for i in 0..32 {
            //match buffer.get_page(file_num | (i as u32), &f) {
                //None => {
                    //panic!("read page_num={:#010x} failed");
                //},
                //Some(_) => {}
            //}
        //}
        //for i in 128..256 {
            //match buffer.get_page(file_num | (i as u32), &f) {
                //None => {
                    //panic!("read page_num={:#010x} failed", file_num|(i as u32));
                //},
                //Some(_) => {}
            //}
        //}
    //}

    /*
     * BufferManager Test3
     * Discontinously Read Pages.
     */
    //#[test]
    //fn buffer_manager_test3() {
        //let mut buffer = BufferManager::new();
        //let mut f = OpenOptions::new().read(true).write(true).open("/home/lunar/Documents/fzf").unwrap();
        //let file_num: u32 = 1<<16;
        //for i in 0..65 {
            //let page_num = file_num | (2*i as u32);
            //match buffer.get_page(page_num, &f) {
                //None => {
                    //panic!("Reading page_num={:#010x} failed", page_num);
                //},
                //Some(v) => {
                    
                //}
            //}
        //}
    //}

    #[test]
    fn buffer_manager_test4() {
        let mut buffer = BufferManager::new();
        let mut f = OpenOptions::new().read(true).write(true).open("/home/lunar/Documents/fzf").unwrap();
        let file_num: u32 = 1<<16;
        let res = buffer.get_page(file_num, &f).unwrap();
        let p = res;
        unsafe {
            (*res.as_ptr()).data[0] = 2;
            (*p.as_ptr()).data[0] += 2;
            println!("{}", (*p.as_ptr()).data[0]);
        }
    }
}
