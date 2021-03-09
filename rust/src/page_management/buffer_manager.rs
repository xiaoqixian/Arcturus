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
use std::ptr::NonNull;

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
    data: Vec<u8>,
    next: i32,
    prev: i32,
    dirty: bool,
    pin_count: u32,
    page_num: u32,
    fp: Option<&'static File>
}

impl BufferPage {
    pub fn new() -> Self {
        BufferPage {
            data: vec![0; 4096],
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
            data: vec![0;0],
            next: self.next,
            prev: self.prev,
            dirty: self.dirty,
            pin_count: self.pin_count,
            page_num: self.page_num,
            fp: self.fp
        }
    }
}

impl Clone for BufferPage {
    fn clone(&self) -> Self {
        BufferPage {
            data: self.data.clone(),
            next: self.next,
            prev: self.prev,
            dirty: self.dirty,
            pin_count: self.pin_count,
            page_num: self.page_num,
            fp: self.fp
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
pub struct BufferManager {
    buffer_table: Vec<NonNull<BufferPage>>, 
    num_pages: u32, //number of pages in the buffer pool, free pages not included.
    page_size: usize,
    first: i32, //most recently used page number.
    last: i32, //last recently used page number.
    /* page number of the first free page.
     * all free pages are linked by the page 
     * in their data structure.*/
    free: i32,
    page_table: HashMap<u32, usize> //we need this table to get a page quickly.
}

impl BufferManager {
    pub fn new() -> Self {
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
            page_size: 4096, //every page is 4KB.
            first: -1,
            last: -1,
            free: 0,
            page_table: HashMap::new()
        }
    }

    fn resize_buffer(&mut self) {
        let cap = self.buffer_table.capacity() as u32;
        if self.num_pages < cap {
            dbg!("No need to resize buffer");
            dbg!("Buffer capacity = {}, while number of pages = {}", cap, self.num_pages);
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
                if i < 10 {
                    dbg!(start+i);
                    dbg!(new_page.clone_metadata());}
                self.buffer_table[(start+i) as usize] = NonNull::new(Box::into_raw(new_page)).unwrap();
            }
            dbg!(unsafe {self.buffer_table[128].as_ref().clone_metadata()});
        }
        dbg!((cap<<1) -1 );
        self.buffer_table[((cap<<1) - 1) as usize] = NonNull::new(Box::into_raw(Box::new(BufferPage::new()))).unwrap();
        self.free = start;
        dbg!(self.free);
        dbg!(unsafe {self.buffer_table[128].as_ref().clone_metadata()});
    }

    fn read_page(&mut self, page_num: u32, fp: &File, index: usize) -> PageFileError {
        let location = (page_num & 0x0000ffff) as usize;
        let offset = (location * self.page_size + page_file::PAGE_FILE_HEADER_SIZE) as u64;
        if unsafe {(*self.buffer_table[index].as_ptr()).data.len()} < self.page_size {
            return PageFileError::DestShort;
        }
        /*
         * read_at is a method of File only provided for unix 
         * systems.
         * we use it here for more convinient reading and writing.
         */
        let res = fp.read_at(unsafe {
            (*self.buffer_table[index].as_ptr()).data.as_mut_slice()}, offset);
        if let Err(_) = res {
            return PageFileError::ReadAtError;
        }
        let read_bytes = res.unwrap();
        if read_bytes < self.page_size {
            unsafe {
                //clear the page
                std::ptr::write_bytes((*self.buffer_table[index].as_ptr()).data.as_mut_ptr(), 0x00, self.page_size);
            }
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
    fn write_page(&self, page_num: u32, page_data: &Vec<u8>, fp: Option<&File>) -> PageFileError {
        if let None = fp {
            return PageFileError::NoFilePointer;
        }
        let location = (page_num & 0x0000ffff) as usize;
        let offset = (location * self.page_size + page_file::PAGE_FILE_HEADER_SIZE) as u64;
        let res = fp.unwrap().write_at(page_data.as_slice(), offset);
        if let Err(_) = res {
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
     * Function needed when there is no free page.
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
            let res = self.write_page(page.page_num, &page.data, page.fp);
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
        dbg!(page.clone_metadata());
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

    pub fn get_page<'a>(&'a mut self, page_num: u32, fp: &File) -> Option<&'a mut BufferPage> {
        let cap = self.buffer_table.capacity();
        let index: usize = match self.page_table.get(&page_num) {
            None => cap,//index cannot be equal to or greater than the buffer_table capacity.
            Some(v) => *v
        };
        if index < cap {
            debug!("Getting page with page_num={:#010x} from buffer", page_num);
            self.update_page(index);
            Some(unsafe {
                &mut *self.buffer_table[index].as_ptr()
            })
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
                    }
                    _ => {
                        error!("Unexpected errors happened.");
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
            Some(new_page)
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
    #[test]
    fn buffer_manager_test1() {
        let mut buffer = BufferManager::new();
        let fp = File::open("/home/lunar/Documents/fzf");
        if let Err(_) = fp {
            panic!("Open file failed.");
        }
        let f = &(fp.unwrap());
        let file_num: u32 = 1<<16;
        for i in 0..129 {
            dbg!(i);
            match buffer.get_page(file_num | (i as u32), f) {
                None => {
                    panic!("read page_num={:#010x} failed", file_num|(i as u32));
                },
                Some(_) => {}
            }
        }
    }

    /*
     * Test2:
     * Read 128 pages, unpin half of them, then read another 128 pages.
     */
    #[test]
    fn buffer_manager_test2() {
        let mut buffer = BufferManager::new();
        let fp = File::open("/home/lunar/Documents/fzf");
        if let Err(_) = fp {
            panic!("Open file failed.");
        }
        let f = &(fp.unwrap());
        let file_num: u32 = 1<<16;
        for i in 0..128 {
            match buffer.get_page(file_num | (i as u32), f) {
                None => {
                    panic!("read page_num={:#010x} failed", file_num|(i as u32));
                },
                Some(_) => {}
            }
        }
        for i in 0..64 {
            debug!("unpin page_num={:#010x}", file_num | (i as u32));
            buffer.unpin(file_num | (i as u32));
        }
        for i in 128..256 {
            dbg!(i);
            match buffer.get_page(file_num | (i as u32), f) {
                None => {
                    panic!("read page_num={:#010x} failed", file_num|(i as u32));
                },
                Some(_) => {}
            }
        }
    }
}
