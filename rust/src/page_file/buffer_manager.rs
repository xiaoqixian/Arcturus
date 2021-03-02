/**********************************************
  > File Name		: buffer_manager.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Mon 01 Mar 2021 07:52:27 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

use std::fs::File;
use rust::page_file;
use std::io::prelude::*;
use std::io::SeekFrom;
use rust::errors;

/*
 * Data structure to represent a page.
 * Notice that the data structure for a page in main memory 
 * is different from it in the file.
 */
struct BufferPageDesc {
    data: Box<[char]>,
    next: i32,
    prev: i32,
    dirty: bool, //true if the page is dirty.
    pin_count: i16,
    page_num: i32,
    fp: &File
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
    buffer_table: &BufferPageDesc,
    num_pages: i32, //number of pages in the buffer pool.
    page_size: usize,
    first: i32, //most recently used page number.
    last: i32, //last recently used page number.
    /* page number of the first free page.
     * all free pages are linked by the page 
     * in their data structure.*/
    free: i32,
    page_table: HashMap<i32, BufferPageDesc> //we need this table to get a page quickly.
}

/*
 * Now we need to specify what a page buffer pool need to do.
 * 1. Read a new page into the buffer and manipulate its data.
 * 2. Unpin a page. When a page manipulation is done, we need 
 *    to unpin the page.
 * 3. Rewrite a page back to the file when we need to remove 
 *    the page from the buffer pool.
 * 4. Get a page with providing a page number.
 */
impl BufferManager {
    //The fp parameter is needed because our pages may be read from multiple files.
    //So we have to specify the file we're reading pages from.
    pub fn get_page(&self, page_num: i32, fp: &File, dest: &mut [u8]) -> &mut BufferPageDesc {
        match self.page_table.get(page_num) {
            Some(v) => {
                v
            },
            None => {//if the page is not in the buffer...
                self.read_page(page_num, fp);
                self.page_table.get(page_num).unwrap()
            }
        }
    }

    /*
     * Read a page from the file.
     */
    fn read_page(&mut self, page_num: i32, fp: &File) -> Errors::PageFileError {
        let offset = page_num * self.page_size + page_file::PAGE_FILE_HEADER_SIZE;
        f.seek(SeekFrom::Start(offset));
        let read_bytes = f.read(dest);
        if read_bytes < 0 {
            return Errors::PageFileError::Unix;
        }
        if read_bytes < self.page_size {
            return Errors::PageFileError::
        }
    }

    fn unpin(&mut self, page_num: i32) {
        
    }


}
