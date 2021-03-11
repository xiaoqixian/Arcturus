/**********************************************
  > File Name		: page_file.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Mon 01 Mar 2021 07:31:48 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

/*
 * Introduction:
 *
 * The page_file_manager component provides facilities for higer-level
 * components to perform file I/O in terms of pages.
 *
 * In the page_file_manager component, methods are provided to create,
 * destroy, open and close paged files, to scan through the pages of a 
 * given file, to read a specific page of a given file, to add and 
 * delete pages of a given file, and to obtain and release pages for 
 * scratch use.
 *
 * Accessing data on a page of a file requires first reading the page 
 * into a buffer pool in main memory, then manipulating its data there.
 *
 * When we need to read a new page into the buffer pool and there is no 
 * space in the buffer pool. We need to remove an old page from the 
 * buffer pool. The page_file_manager component uses a 
 * Least_Recently_Used (LRU) page replacement policy. When a page is 
 * removed from the buffer pool, it is copied back to the file on disk 
 * if and only if the page is marked as "dirty".
 */

use std::fs::File;
use super::buffer_manager::BufferManager;
use std::os::unix::fs::FileExt;
use std::mem::size_of;

pub const PAGE_FILE_HEADER_SIZE: usize = 40;
pub const PAGE_SIZE: usize = 4096;

/*
 * We need a data structure to represent a page.
 * Every page data structure is stored right next to its data.
 * We don't want to store the data inside the struct.
 * Because we don't get in trouble with struct attribute offset
 * problem.
 */
#[derive(Debug, Clone)]
pub struct Page {
    page_num: u32, //page number
    num_records: usize, //number of records in the page.
}

impl Page {
    pub fn new(page_num: u32) -> Self {
        Page {
            page_num,
            num_records: 0,
        }
    }
 }

#[derive(Debug, Clone, Copy)]
struct PageFileHeader {
    file_num: u16,
    first_free_page: u32, //the number of the first free page.
    num_pages: usize, //number of pages.
}

impl PageFileHeader {
    pub fn new(file_num: u16) -> Self {
        PageFileHeader {
            file_num,
            first_free_page: 0,
            num_pages: 0
        }
    }
}

/*
 * The PageFileManager class handles the creation, deletion, opening, 
 * and closing of paged files, along with the allocation and disposal
 * of scratch pages.
 */
struct PageFileManager {
    fp: Option<File>, //opend file pointer.
    open_flag: bool, //file open flag.
    changed_flag: bool, //mark if the file is changed.
    file_header: PageFileHeader,
    buffer_manager: BufferManager //the buffer manager and the page file manager are interrelated.
}

impl PageFileManager {
    pub fn new(fp: &File) -> Self {
        PageFileManager {
            fp: Some(fp.try_clone().unwrap()),
            open_flag: false,
            changed_flag: false,
            file_header: Self::read_header(fp),
            buffer_manager: BufferManager::new()
        }
    }

    fn read_header(fp: &File) -> PageFileHeader {
        let mut pf_header = PageFileHeader::new(0);
        unsafe {
            let slice_header = std::slice::from_raw_parts_mut(&mut pf_header as *mut _ as *mut u8, size_of::<PageFileHeader>());
            fp.read_at(slice_header, size_of::<PageFileHeader>() as u64);
        }
        pf_header
    }

    /*
     * allocate a page in the file.
     * not necessarily read the page into the buffer.
     */
    pub fn allocate_page(&mut self) {
        let page_num = (self.file_header.file_num as u32) << 16 | (self.file_header.num_pages as u32);
        let page = Page::new(page_num);
        let data: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        let page_offset = Self::get_page_offset(self.file_header.num_pages);
        let data_offset = Self::get_data_offset(self.file_header.num_pages);
        unsafe {
            //write page header.
            let slice_header = std::slice::from_raw_parts(&page as *const _ as *const u8, size_of::<Page>());
            self.fp.as_ref().unwrap().write_at(slice_header, page_offset);
            self.fp.as_ref().unwrap().write_at(&data, data_offset);
        }
        self.file_header.num_pages += 1;
    }

    fn get_page_offset(index: usize) -> u64 {
        (size_of::<PageFileHeader>() + index*(size_of::<Page>() + PAGE_SIZE)) as u64
    }

    fn get_data_offset(index: usize) -> u64 {
        Self::get_page_offset(index) + (size_of::<Page>() as u64)
    }
}
