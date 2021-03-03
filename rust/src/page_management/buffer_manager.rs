/**********************************************
  > File Name		: buffer_manager.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Mon 01 Mar 2021 07:52:27 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

use std::fs::File;
use super::page_file;
use std::io::prelude::*;
use std::io::SeekFrom;
use rust::errors::Errors::PageFileError;

/*
 * Data structure to represent a page.
 * Notice that the data structure for a page in main memory 
 * is different from it in a file.
 */
struct BufferPage {
    data: Vec<u8>,
    next: i32, //next and prev are used to link the page when it's free.
    prev: i32, //if they are -1, means no page linked before or after.
    dirty: bool, //true if the page is dirty.
    pin_count: i16,
    page_num: i32,
    fp: Option<&File> //we need this File reference to write the page back.
}

impl Clone for BufferPage {
    fn clone(&mut self) -> Self {
        BufferPage {
            data: self.data,
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
struct BufferManager {
    buffer_table: Vec<BufferPage>,
    num_pages: i32, //number of pages in the buffer pool.
    page_size: usize,
    first: i32, //most recently used page number.
    last: i32, //last recently used page number.
    /* page number of the first free page.
     * all free pages are linked by the page 
     * in their data structure.*/
    free: i32,
    page_table: HashMap<i32, usize> //we need this table to get a page quickly.
}

/*
 * Now we need to specify what a page buffer pool need to do.
 * 1. Read a new page into the buffer and manipulate its data.
 * 2. Unpin a page. When a page manipulation is done, we need 
 *    to unpin the page.
 * 3. Rewrite a page back to the file when we need to remove 
 *    the page from the buffer pool.
 * 4. Get a page with providing a page number.
 *
 * How we are gonna manage the buffer pool to make sure excellent performance?
 * 1. We use a vector of BufferPage to store all buffer pages, which is 
 *    the self.buffer_table.
 * 2. Array is not good at searching, so we need a HashMap to quickly get a 
 *    page's location or index by its page number.
 */
impl BufferManager {
    //The fp parameter is needed because our pages may be read from multiple files.
    //So we have to specify the file we're reading pages from.
    pub fn get_page(&mut self, page_num: i32, fp: &File) -> &'static mut BufferPage {
        match self.page_table.get(page_num) {
            Some(v) => {
                /*
                 * If the page we are searching for is in the buffer.
                 * 1. Pin the page.
                 * 2. Place the page in the head of the used list.
                 * 3. Return a reference of the page with static lifetime.
                 */
                self.update_newest(v);
                &mut self.buffer_table[v]
            },
            None => {
                /*
                 * If the page is not in the buffer.
                 * We need to add a new page, two cases:
                 * 1. If there is free pages: allocate one of them.
                 * 2. If no free pages left, write the least-recently-used
                 *    page back, and allocate its memory to the new page.
                 */
                if self.free == -1 { //no free space.
                    match self.free_page(self.last) {
                        PageFileError::Okay => {},
                        PageFileError::Unix => {
                            eprintln!("Unix errors happened when try to write a page back to its file.");
                        },
                        PageFileError::IncompleteWrite => {
                            eprintln!("File Incomplete Write.");
                        },
                        PageFileError::NoPage => {
                            eprintln!("Impossible to happen. Cause when there is no used page, there must be all free pages.");
                        },
                        PageFileError::AllPagesPinned => {
                            self.resize_buffer();
                        }
                        _ => {
                            eprintln!("Unexpected errrors happened.");
                        }
                    }
                }
                let new_page = &mut self.buffer_table[self.free];
                match self.read_page(page_num, f, &mut new_page.data) {
                    PageFileError::Okay => {},
                    PageFileError::Unix => {
                        eprintln!("Unix read error.");
                    },
                    PageFileError::IncompleteRead => {
                        eprintln!("Incomplete read when read a new page.");
                    },
                    PageFileError::HashPageExist => {
                        eprintln!("HashPageExist: This is impossible.");
                    },
                }
                new_page
            }
        }
    }

    /*
     * Read a page from the file.
     */
    fn read_page(&mut self, page_num: i32, f: &File, dest: &mut Vec<u8>)-> Errors::PageFileError {
        let location = page_num & 0x0000ffff;
        let offset = location * self.page_size + page_file::PAGE_FILE_HEADER_SIZE;
        f.seek(SeekFrom::Start(offset));
        if dest.len() < self.page_size {
            return PageFileError::DestShort;
        }
        let read_bytes = f.read(dest.as_mut_slice());
        if read_bytes < 0 {
            return Errors::PageFileError::Unix;
        }
        if read_bytes < self.page_size {
            return Errors::PageFileError::IncompleteRead;
        }
        PageFileError::Okay
    }

    fn resize_buffer(&mut self) {
        self.buffer_table.resize(self.buffer_table.len() << 1, BufferPage {
            data: vec![0; self.page_size],
            next: -1,
            prev: -1,
            dirty: false,
            pin_count: 0,
            page_num: -1,
            fp: None,
        });
    }

    fn unpin(&mut self, page_num: i32) {
        
    }

    /*
     * When a page is read, we need to place it in the head of the used list.
     */
    fn update_newest(&mut self, index: usize) {
        let page = &mut self.buffer_table[index];
        if page.prev == -1 { //already the head of the used list.
            return ;
        }
        let prev_page = &mut self.buffer_table[page.prev as usize];
        if page.next != -1 {
            let next_page = &mut self.buffer_table[page.next as usize];
            next_page.prev = page.prev;
        } else {
            self.last = page.prev;
        }
        prev_page.next = page.next;
        page.next = self.first;
        self.first = index as i32;
        self.pin_count += 1; //pin count increase by 1.
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
    fn write_page(&mut self, page: &mut BufferPage) -> PageFileError {
        if let None = page.fp {
            return PageFileError::NoFilePointer;
        }
        let location = page.page_num & 0x0000ffff as usize;
        page.fp.unwrap().seek(SeekFrom::Start(location * self.page_size));
        let write_bytes = page.fp.unwrap().write(page.data.as_slice());
        if write_bytes < 0 {
            return PageFileError::Unix;
        }
        if write_bytes < self.page_size {
            return PageFileError::IncompleteWrite;
        }
        PageFileError::Okay
    }

    /*
     * Function needed when no free pages for using.
     */
    fn free_page(&mut self) -> PageFileError {
        let mut page: &mut BufferPage;
        if self.last == -1 {
            return PageFileError::NoPage;
        }
        page = &mut self.buffer_table[self.last as usize];
        while page.pin_count != 0 {
            page = &mut self.buffer_table[page.prev as usize];
        }
        if page.pin_count != 0 {
            return PageFileError::AllPagesPinned;
        }
        if page.dirty {
            let res = self.write_page(page);
            if let Errors::Okay = res {
            } else {
                return res;
            }
        }
        //clear all the data in the buffer page.
        unsafe {
            std::ptr::write_bytes(page.data.as_mut_ptr(), 0x00, page.data.capacity());
        }
        page.dirty = false;
        page.pin_count = 0;
        page.page_num = 0;
        //add it to the free list.
        page.next = self.free;
        page.prev = -1;
        self.free = index;
        PageFileError::Okay
    }
}
