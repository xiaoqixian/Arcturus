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
use std::ptr::NonNull;
use std::{println as info, println as debug, println as warn, println as error};

use crate::errors::PageFileError;
use super::buffer_manager::BufferPage;

pub const PAGE_SIZE: usize = 4096;

/*
 * We need a data structure to represent a page header.
 * We don't want to store the data inside the struct.
 * Because we don't get in trouble with struct attribute offset
 * problem.
 *
 * Bitmap: we use bitmap instead of linked list to represent
 * the used amount of a page. Therefore, we have to specify the
 * number of records when create a new page. Because the page
 * size is determined, once we know record size, we know how
 * many records there are in one page. And the record size is 
 * determined when create a new file (which represents a table
 * in a database). After all, all the records in a table are 
 * the same size.
 * However, the bitmap is not defined inside the Page struct.
 * It is just written right next to the page header. The bitmap
 * size should be determined when the table or I should say the 
 * file is created.
 *
 * So here's a real Page: {PageHeader, Bitmap, Data(Records)}.
 */
#[derive(Debug, Clone, Copy)]
pub struct PageHeader {
    pub page_num: u32, //page number
    pub num_records: usize, //number of records in the page.
    pub next_free: u32, //next_free is the page_num of the next free page. All free pages are linked together by this.
}

impl PageHeader {
    pub fn new(page_num: u32) -> Self {
        Self {
            page_num,
            num_records: 0,
            next_free: 0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PageFileHeader {
    file_num: u16,
    first_free_page: u32, //the number of the first free page.
    num_pages: usize, //number of pages.
    free: u32, //page number of next free page, if equals to 0, there is no free page.
    record_size: usize,
    bitmap_size: usize,
    page_size: usize,
}

impl PageFileHeader {
    pub fn new(file_num: u16, record_size: usize) -> Self {
        PageFileHeader {
            file_num,
            first_free_page: 0,
            num_pages: 0,
            free: 0,
            record_size,
            bitmap_size: {
                if record_size == 0 {
                    0
                } else {
                    PAGE_SIZE/record_size/8
                }
            },
            page_size: {
                if record_size == 0 {
                    0
                } else {
                    size_of::<PageHeader>() + PAGE_SIZE/record_size/8 + PAGE_SIZE
                }
            }
        }
    }
}

/*
 * The PageFileManager class handles the creation, deletion, 
 * opening, and closing of paged files, along with the 
 * allocation and disposal of scratch pages.
 *
 * Let me explain to you how I manage free pages. Like in the 
 * buffer, when we dispose a page, we don't actually erase them
 * from the file, instead, we link them together for next 
 * allocation.
 *
 * When a page is read into buffer, the page header is read 
 * into buffer along with its data. So when we get a page from
 * the buffer, we just need to make the pointer (next_free) 
 * points to the next free page until all free pages are
 * allocated. Then we start to allocate new page and expand
 * the file. When next_free = 0, means there is no free page,
 * cause 0 is an invalid page number.
 */
pub struct PageFileManager {
    fp: File, //opend file pointer.
    first_free: u32, //first free page.
    file_header: PageFileHeader,
    buffer_manager: BufferManager //the buffer manager and the page file manager are interrelated.
}

impl PageFileManager {
    pub fn new(fp: &File) -> Self {
        let temp = Self::read_header(fp);
        if let Err(_) = temp {
            panic!("read header error");
        }
        let header = temp.unwrap();
        PageFileManager {
            fp: fp.try_clone().unwrap(),
            first_free: header.free,
            file_header: header,
            buffer_manager: BufferManager::new(header.page_size)
        }
    }

    pub fn get_pagesize(&self) -> usize {
        self.buffer_manager.get_pagesize()
    }

    fn read_header(fp: &File) -> Result<PageFileHeader, PageFileError> {
        let mut pf_header = PageFileHeader::new(0, 0);
        unsafe {
            let slice_header = std::slice::from_raw_parts_mut(&mut pf_header as *mut _ as *mut u8, size_of::<PageFileHeader>());
            let res = fp.read_at(slice_header, size_of::<PageFileHeader>() as u64);
            if let Err(_) = res {
                dbg!(res);
                return Err(PageFileError::ReadAtError);
            }
            let read_bytes = res.unwrap();
            if read_bytes < size_of::<PageFileHeader>() {
                dbg!("read_bytes");
                return Err(PageFileError::IncompleteRead);
            }
        }
        //Ok(pf_header)
        //TODO
        Ok(PageFileHeader::new(1, 24))
    }

    /*
     * allocate a page in the file, may get page which was 
     * previously disposed.
     * After allocation, the page is read into buffer if it
     * is not in the buffer, and the BufferPage pointer is 
     * returned.
     */
    pub fn allocate_page(&mut self) -> Result<NonNull<BufferPage>, PageFileError> {
        if self.first_free > 0 {
            /*
             * For a previously allocated page, we don't need
             * any initialization. Cause the work was already 
             * done when the page was disposed.
             */
            debug!("Allocate a previously allocated page");
            match self.buffer_manager.get_page(self.first_free, &self.fp) {
                None => {
                    Err(PageFileError::GetPageError)
                },
                Some(v) => {
                    self.first_free = unsafe {
                        v.as_ref().get_next_free()
                    };
                    Ok(v)
                }
            }
        } else {
            /* A new page first occurs in the buffer, and will
             * not be written in file until it is freed and the
             * buffer makes it to do so.
             */
            debug!("Allocate a new page");
            let page_num = self.get_page_num(self.file_header.num_pages);
            let res = self.buffer_manager.allocate_page(page_num, &self.fp);
            if let None = res {
                return Err(PageFileError::AllocatePageError);
            }
            unsafe {
                res.unwrap().as_mut().init_page_header(page_num);
            }
            self.file_header.num_pages += 1;
            Ok(res.unwrap())
        }
    }

    /*
     * Dispose a page.
     * The disposed page will be linked and all its data will
     * not be cleared.
     */
    pub fn dispose_page(&mut self, page_num: u32) -> Result<(), PageFileError> {
        match self.buffer_manager.get_page(page_num, &self.fp) {
            None => {
                dbg!(page_num);
                Err(PageFileError::GetPageError)
            },
            Some(mut v) => {
                let page = unsafe {
                    v.as_mut()
                };
                page.set_next_free(self.first_free);
                self.first_free = page_num;
                dbg!(&page);
                page.mark_dirty();
                self.buffer_manager.unpin(page_num);
                Ok(())
            }
        }
    }

    pub fn get_page(&mut self, page_num: u32) -> Option<NonNull<BufferPage>> {
        self.buffer_manager.get_page(page_num, &self.fp)
    }

    pub fn unpin_page(&mut self, page_num: u32) {
        self.buffer_manager.unpin(page_num);
    }

    fn get_page_num(&self, page_index: usize) -> u32 {
        ((self.file_header.file_num as u32) << 16) | (page_index as u32)
    }


    pub fn get_page_offset(index: usize, page_size: usize) -> u64 {
        (size_of::<PageFileHeader>() + index*page_size) as u64
    }

    fn get_bitmap_offset(index: usize, page_size: usize) -> u64 {
        Self::get_page_offset(index, page_size) + (size_of::<PageHeader>() as u64)
    }

    fn get_data_offset(index: usize, page_size: usize) -> u64 {
        Self::get_page_offset(index, page_size) + ((page_size - PAGE_SIZE) as u64)
    }
}
