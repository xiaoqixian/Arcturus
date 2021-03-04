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
    num_pages: u32, //number of pages in the buffer pool.
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
                 * 2. If the page is not pinned before, remove it from the 
                 *    unused list.
                 * 3. Return a reference of the page with static lifetime.
                 */
                self.update_page(v);
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
                            self.resize_buffer();
                        },
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
                self.free = new_page.next;
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
        //f.seek(SeekFrom::Start(offset));
        if dest.len() < self.page_size {
            return PageFileError::DestShort;
        }
        /*
         * read_at is a method of File only provided for unix systems.
         * we use it here for more convinient reading and writing.
         * */
        let res = f.read_at(dest.as_mut_slice(), offset as u64);
        if let None = res {
            return PageFileError::ReadAtError;
        }
        let read_bytes = res.unwrap();
        if read_bytes < 0 {
            return Errors::PageFileError::Unix;
        }
        if read_bytes < self.page_size {
            unsafe {
                std::ptr::write_bytes(dest.as_mut_ptr(), 0x00, self.page_size);
            }
            return Errors::PageFileError::IncompleteRead;
        }
        self.num_pages += 1;
        self.buffer_table.insert(page_num, self.free);
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
        //link all free pages 
        let start = self.buffer_table.len() >> 1;
        for i in 0..(start-1) {
            self.buffer_table[start+i].next = start+i+1;
        }
        self.free = start;
    }

    /*
     * When a page is in buffer, two possibilities:
     * 1. The page is already pinned.
     * 2. The page is in the unused list, we need to remove it from the 
     *    unused list.
     */
    fn update_page(&mut self, index: usize) {
        let page = &mut self.buffer_table[index];
        page.pin_count += 1;
        if page.pin_count > 1 {//already in using.
            return ;
        }
        if page.prev == -1 {//page is the head of the unused list.
            self.first = page.next;
        } else {
            let prev_page = &mut self.buffer_table[page.prev as usize];
            prev_page.next = page.next;
        }
        if page.next != -1 {
            let next_page = &mut self.buffer_table[page.next as usize];
            next_page.prev = page.prev;
        } else {
            self.last = page.prev;
        }
        page.prev = -1;
        page.next = -1;
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
        let offset = location * self.page_size + page_file::PAGE_FILE_HEADER_SIZE;
        let f = page.fp.unwrap();
        let res = f.write_at(page.data.as_mut_slice(), offset as u64);
        if let None = res {
            return PageFileError::WriteAtError;
        }
        let write_bytes = res.unwrap();
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
     * Applied for unused pages.
     */
    fn free_page(&mut self, index: usize) -> PageFileError {
        let page = &mut self.buffer_table[index];
        if page.free != -1 {
            return PageFileError::PageFreed;
        }
        if page.pin_count != 0 {
            return PageFileError::PagePinned;
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
        self.last = page.prev;
        //remove the page entry from the page_table.
        self.page_table.remove(&page.page_num);
        page.dirty = false;
        page.pin_count = 0;
        page.page_num = 0;
        //add it to the free list.
        page.next = self.free;
        page.prev = -1;
        self.free = index;
        self.num_pages -= 1;
        PageFileError::Okay
    }

    fn mark_dirty(&mut self, page_num: i32) -> PageFileError {
        match self.page_table.get(page_num) {
            None => {
                return PageFileError::PageNotInBuf;
            },
            Some(v) => {
                let page = &mut self.buffer_table[v as usize];
                if page.free != -1 { //means it's a free page.
                    return PageFileError::PageNotInBuf;
                }
                if page.pin_count == 0 {
                    return PageFileError::PageUnpinned;
                }
                page.dirty = true;
            }
        }
        PageFileError::Okay
    }

    /*
     * Flush pages:
     * Release all pages of the file and write them back to the file.
     * Return an error if one of the pages is pinned.
     *
     * Main difficulty:
     * Pages of one file are not collected. So we have to search the
     * whole buffer array.
     * And in Rust, when we open a file, we get a File reference instead
     * of a file descriptor. So we can't directly compare two File references.
     * 
     * So I write a file comparing function.
     */
    fn flush_pages(&mut self, fp: &File) -> PageFileError {
        if self.first == -1 {
            return PageFileError::NoPage;
        }
        let mut page1: &mut BufferPage;
        let mut page2: &mut BufferPage;
        let mut i = self.first as usize;
        let mut k = self.last as usize;
        while i <= k {
            page1 = &mut self.buffer_table[i];
            page2 = &mut self.buffer_table[k];
            if page1.pin_count != 0 || page2.pin_count != 0 {
                return PageFileError::PageUnpinned;
            }
            if Self::compare_file(page1.fp, fp) {
                self.free_page(i);
            }
            i = page1.next as usize;
            if i > k {
                break;
            }
            if Self::compare_file(page2.fp, fp) {
                self.free_page(k);
            }
            k = page1.prev as usize;
        }
        PageFileError::Okay
    }

    /*
     * Compare two files by comparing their dev info and inode info 
     * in their metadata.
     * Enlightened by the **same_file** crate: 
     * https://github.com/BurntSushi/same_file.git
     */
    pub fn compare_file(f1: &File, f2: &File) -> bool {
        let m1 = f1.metadata();
        let m2 = f2.metadata();
        if m1.dev() == m2.dev() && m1.ino() == m2.ino() {
            true
        } else {
            false
        }
    }
}
