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
use std::cell::RefCell;

use crate::errors::PageFileError;
use super::page_file;

/*
 * Memory and References.
 * Let me explain how I resolve memory passing between functions
 * and references lifetimes.
 * 
 * Buffer Pages Storage:
 * All buffer pages are allocated on heap, and the Boxes are 
 * stored within a vector.
 * In this way, when we need to return a page in buffer pool,
 * we can just return a copy of the Box and avoid returning 
 * a refernce and causing some serious refernce lifetime 
 * problems.
 */

/*
 * Data structure to represent a page.
 * Notice that the data structure for a page in main memory 
 * is different from it in a file.
 */

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
    buffer_table: Vec<Box<BufferPage>>, 
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
                let mut v = vec![Box::new(BufferPage::new()); 128];
                //link all free pages
                let len = v.len() as i32;
                for i in 0..(len-1) {
                    v[i as usize].next = i+1;
                }
                v
            },
            num_pages: 0,
            page_size: 4096,
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
            return ;
        }
        self.buffer_table.resize((cap << 1) as usize, Box::new(BufferPage::new()));
        //link all free pages.
        let start = cap as i32;
        for i in 0..(start-1) {
            self.buffer_table[(start+i) as usize].next = start+i+1;
        }
        self.free = start;
    }

    fn read_page(&mut self, page_num: u32, fp: &File, index: usize) -> PageFileError {
        let location = (page_num & 0x0000ffff) as usize;
        let offset = (location * self.page_size + page_file::PAGE_FILE_HEADER_SIZE) as u64;
        if self.buffer_table[index].data.len() < self.page_size {
            return PageFileError::DestShort;
        }
        /*
         * read_at is a method of File only provided for unix 
         * systems.
         * we use it here for more convinient reading and writing.
         */
        let res = fp.read_at(self.buffer_table[index].data.as_mut_slice(), offset);
        if let Err(_) = res {
            return PageFileError::ReadAtError;
        }
        let read_bytes = res.unwrap();
        if read_bytes < self.page_size {
            unsafe {
                //clear the page
                std::ptr::write_bytes(self.buffer_table[index].data.as_mut_ptr(), 0x00, self.page_size);
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
        let page = &self.buffer_table[index];
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
        let page = &mut self.buffer_table[index];
        //clear all the data in the buffer page.
        unsafe {
            std::ptr::write_bytes(page.data.as_mut_ptr(), 0x00, page.data.capacity());
        }
        self.page_table.remove(&page.page_num);
        self.last = page.prev;
        if self.first == (index as i32) {
            self.first = -1;
        }
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
     * When a page is read from the buffer, we need to update
     * some data of the page and the buffer pool.
     */
    fn update_page(&mut self, index: usize) {
        let pin_count: u32;
        let prev: i32;
        let next: i32;
        {
            let page = &self.buffer_table[index];
            pin_count = page.pin_count;
            prev = page.prev;
            next = page.next;
        }
        if pin_count > 1 {
            self.buffer_table[index].pin_count += 1;
            return ;
        }
        //remove the page from the unused list.
        if prev == -1 {
            self.first = next;
        } else {
            self.buffer_table[prev as usize].next = next;
        }
        if next == -1 {
            self.last = prev;
        } else {
            self.buffer_table[next as usize].prev = prev;
        }
        let page = &mut self.buffer_table[index];
        page.pin_count += 1;
        page.prev = -1;
        page.next = -1;
    }

    /*
     * Methods of the Page File Interface
     * get a page which is immutable.
     * The interface doesn't pass errors out, if errors occur,
     * either eprintln! macro or dbg! macro is used for debuging. 
     * We assume that the all buffer pages live as long as 
     * the buffer pool itself.
     */
    pub fn get_page<'a>(&'a mut self, page_num: u32, fp: &File) -> Option<&'a Box<BufferPage>> {
        let cap = self.buffer_table.capacity();
        let index: usize = match self.page_table.get(&page_num) {
            None => cap,//index cannot be equal to or greater than the buffer_table capacity.
            Some(v) => *v
        };
        if index != cap {
            dbg!("Get page with page_num={} from buffer", page_num);
            self.update_page(index);
            Some(&self.buffer_table[index])
        } else {
            dbg!("Read page with page_num={} from file.", page_num);
            if self.free == -1 {
                match self.free_page(self.last as usize) {
                    PageFileError::Okay => {},
                    PageFileError::Unix => {
                        eprintln!("Unix read error.");
                        return None;
                    },
                    PageFileError::IncompleteRead => {
                        eprintln!("Incomplete read when read a new page.");
                        return None;
                    },
                    PageFileError::HashPageExist => {
                        eprintln!("HashPageExist: This is impossible.");
                        return None;
                    },
                    PageFileError::NoPage => {
                        self.resize_buffer();
                    }
                    _ => {
                        eprintln!("Unexpected errors happened.");
                    }
                }
            }
            let newpage_index = self.free as usize;
            match self.read_page(page_num, fp, newpage_index) {
                PageFileError::Okay => {},                    
                PageFileError::ReadAtError => {
                    eprintln!("read_at function error.");
                },
                PageFileError::IncompleteRead => {
                    eprintln!("read_at function IncompleteRead");
                }
                PageFileError::DestShort => {
                    eprintln!("Unexpected error: page data length is too short");
                },
                _ => {
                    eprintln!("Unexpected error occured");
                }
            }
            self.page_table.insert(page_num, newpage_index);
            self.num_pages += 1;
            let new_page = &mut self.buffer_table[newpage_index];
            self.free = new_page.next;
            new_page.next = -1;
            new_page.pin_count = 1;
            new_page.page_num = page_num;
            Some(new_page)
        }
    }
}
