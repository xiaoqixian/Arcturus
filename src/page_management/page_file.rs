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

use std::fs::{File, OpenOptions};
use super::buffer_manager::BufferManager;
use std::os::unix::fs::FileExt;
use std::mem::size_of;
use std::ptr::NonNull;
use std::{println as info, println as debug, println as warn, println as error};

use crate::errors::{Error, PageFileError};
use super::buffer_manager::BufferPage;

pub const PAGE_SIZE: usize = 4096;
const BUFFER_SIZE: usize = 128;

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
 * So here's a real Page: {PageHeader, data(data can be decided by different clients.)}.
 */

/*
 * How do we represent a file.
 * As we are building a page management client and its page may have multiple format,
 * so we just collect some common data for page managing.
 * Like in record management module, we use bitmap to manage all records in a page.
 * And in indexing module, we use linked list to manage all index entries. 
 * These are decided by the client, the page management module just need to provide a 
 * page to store PAGE_SIZE long data. And returns a pointer whenever a page num is 
 * provided.
 */
#[derive(Debug, Clone, Copy)]
pub struct PageHeader {
    pub page_num: u32, //page number
    pub next_free: u32, //next_free is the page_num of the next free page. All free pages are linked together by this.
}

impl PageHeader {
    pub fn new(page_num: u32) -> Self {
        Self {
            page_num,
            next_free: 0,
        }
    }
}

/*
 * PageHandle is used to represent a page between modules,
 * implement some functions that clients may operate on a page.
 */
#[derive(Debug, Copy, Clone)]
pub struct PageHandle {
    page_num: u32,
    data: *mut u8
}

impl PageHandle {
    pub fn new(page_num: u32, data: *mut u8) -> Self {
        Self {
            page_num,
            data
        }
    }

    pub fn get_page_num(&self) -> u32 {
        self.page_num
    }

    pub fn get_data(&self) -> *mut u8 {
        self.data
    }
}

/*
 * PageFile layout:
 *  |PageFileHeader|pages|
 */
#[derive(Debug, Clone, Copy)]
pub struct PageFileHeader {
    file_num: u16,
    num_pages: usize, //number of pages, including disposed pages.
    free: u32, //page number of next free page, if equals to 0, there is no free page.
}

impl PageFileHeader {
    pub fn new(file_num: u16) -> Self {
        PageFileHeader {
            file_num,
            num_pages: 0,
            free: 0,
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
#[derive(Debug)]
pub struct PageFileManager {
    num_files: u16,//num_files is permenant, which means even after the database is closed. Next time it opens, num_files will still be the same. So num_files actually represent the number of all tables ever created. Even after tables are dropped later. Every time the database is opend, this data is read from a specific file.
    buffer_manager: BufferManager//place where the only BufferManager get instaniated, every time a page file is opened, a reference to this instance is created and saved in the corresponding PageFileHandle.
}

impl PageFileManager {
    pub fn new() -> Self {
        Self {
            num_files: 1,
            buffer_manager: BufferManager::new(BUFFER_SIZE)
        }
    }
    /*
     * create a page file.
     */
    pub fn create_file(&mut self, file_name: &String) -> Result<PageFileHandle, Error> {
        let file_header = PageFileHeader {
            file_num: self.num_files, 
            num_pages: 0,
            free: 0
        };
        self.num_files += 1;
        match OpenOptions::new().read(true).write(true).create(true).open(file_name) {
            Err(e) => {
                dbg!(&e);
                Err(Error::CreatePageFileError)
            },
            Ok(fp) => {
                let sli = unsafe {
                    std::slice::from_raw_parts(&file_header as *const _ as *const u8, size_of::<PageFileHeader>())
                };
                match fp.write_at(sli, 0) {
                    Err(e) => {
                        dbg!(&e);
                        panic!("write at error");
                    },
                    Ok(write_bytes) => {
                        if write_bytes < size_of::<PageFileHeader>() {
                            dbg!(write_bytes);
                            return Err(Error::IncompleteWrite);
                        }
                    }
                }
                Ok(PageFileHandle::new(&fp, &mut self.buffer_manager as *mut _))
            }
        }
    }

    pub fn open_file(&mut self, file_name: &String) -> Result<PageFileHandle, Error> {
        match File::open(file_name) {
            Err(e) => {
                dbg!(&e);
                Err(Error::FileOpenError)
            },
            Ok(f) => {
                Ok(PageFileHandle::new(&f, &mut self.buffer_manager as *mut _))
            }
        }
    }
}


/*
 * Every page file is associated with a PageFileHandle, once you open a file, a 
 * PageFileHandle is returned. 
 * Once you have a PageFileHandle, you can use it for page allocation, page getting, or
 * page disposition.
 * 
 * All PageFileHandles share a same BufferManager, otherwise it will be big waste if 
 * we create a BufferManager for each page file. 
 * As we have a muttable reference of a BufferManager instance in each PageFileHandle, 
 * it breaks the borrowing rules of Rust, so we mutablly refer the instance by its raw
 * pointer.
 */
#[derive(Debug)]
pub struct PageFileHandle {
    fp: File,
    header: PageFileHeader,
    header_changed: bool,//set true when the header is changed, then we need to write the header back to file when the file is about to be closed.
    buffer_manager: &'static mut BufferManager
}

impl PageFileHandle {
    pub fn clone(&mut self) -> Self {
        Self {
            fp: self.fp.try_clone().expect("clone file pointer error"),
            header: self.header,
            header_changed: self.header_changed,
            buffer_manager: unsafe {
                &mut *(self.buffer_manager as *mut _)//my way of copying a reference.
            }
        }
    }

    pub fn new(f: &File, bm: *mut BufferManager) -> Self {
        Self {
            fp: f.try_clone().expect("File pointer cloning error"),
            header: {
                let res = Self::read_header(f);
                match res {
                    Err(e) => {
                        dbg!(&e);
                        panic!("Read PageFileHeader error");
                    },
                    Ok(v) => v
                }
            },
            header_changed: false,
            buffer_manager: unsafe {
                &mut *bm
            }
        }
    }

    fn read_header(fp: &File) -> Result<PageFileHeader, PageFileError> {
        let mut pf_header = PageFileHeader::new(0);
        unsafe {
            let slice_header = std::slice::from_raw_parts_mut(&mut pf_header as *mut _ as *mut u8, size_of::<PageFileHeader>());
            let res = fp.read_at(slice_header, 0);
            if let Err(_) = res {
                dbg!(res);
                return Err(PageFileError::ReadAtError);
            }
            let read_bytes = res.unwrap();
            if read_bytes < size_of::<PageFileHeader>() {
                dbg!(read_bytes);
                return Err(PageFileError::IncompleteRead);
            }
        }
        Ok(pf_header)
    }

    /*
     * allocate a page in the file, may get page which was 
     * previously disposed. How we know where is the previously disposed page?
     * All disposed pages are linked together, and file header reserves the header
     * of the linked list.
     *
     * After allocation, the page is read into buffer if it
     * is not in the buffer, and the data pointer is returned.
     *
     * Method only called when the page that self.first_free points 
     * to is full. 
     */
    pub fn allocate_page(&mut self) -> Result<PageHandle, Error> {
        let page_num: u32;
        let first_free = self.header.free;
        let mut page_header: &mut PageHeader;
        let data: *mut u8;

        if first_free > 0 {
            /*
             * For a previously allocated page, we don't need
             * any initialization. Cause the work was already 
             * done when the page was disposed.
             */
            debug!("Allocate a previously allocated page");
            page_num = first_free;
            data = match self.buffer_manager.get_page(first_free, &self.fp) {
                Err(e) => {
                    dbg!(&e);
                    return Err(Error::GetPageError);
                },
                Ok(v) => v
            };
            //update page file header linked list.
            self.header_changed = true;
            page_header = unsafe {
                &mut *(data as *mut PageHeader)
            };
            self.header.free = page_header.next_free;
        } else {
            debug!("Allocate a new page");
            page_num = self.get_page_num(self.header.num_pages);
            self.header.num_pages += 1;
            data = match self.buffer_manager.allocate_page(page_num, &self.fp) {
                Err(e) => {
                    dbg!(&e);
                    return Err(Error::AllocatePageError);
                },
                Ok(v) => v
            };
            page_header = unsafe {
                &mut *(data as *mut PageHeader)
            };
        }

        page_header.next_free = 0;
        page_header.page_num = page_num;
        dbg!(&page_header);
        self.header_changed = true;
        //zero out the page data.
        unsafe {
            let p = data.offset(size_of::<PageHeader>() as isize);
            std::ptr::write_bytes(p, 0, PAGE_SIZE);
        }
        match self.mark_dirty(page_num) {
            Ok(_) => Ok(PageHandle::new(page_num, data)),
            Err(e) => Err(e)
        }
   }

    /*
     * Dispose a page.
     * The disposed page will be linked and all its data will
     * not be cleared.
     */
    pub fn dispose_page(&mut self, page_num: u32) -> Result<(), Error> {
        match self.buffer_manager.get_page(page_num, &self.fp) {
            Err(e) => {
                dbg!(page_num);
                dbg!(&e);
                Err(Error::GetPageError)
            },
            Ok(v) => {
                let page_header = unsafe {
                    &mut *(v as *mut PageHeader)
                };
                if page_header.next_free != 0 {
                    dbg!(&page_header);
                    return Err(Error::PageDisposed);
                }
                page_header.next_free = self.header.free;
                dbg!(&page_header);
                self.header.free = page_num;
                dbg!(&self.header.free);
                self.header_changed = true;
                self.mark_dirty(page_num);//page header changed.
                self.buffer_manager.unpin(page_num);
                Ok(())
            }
        }
    }

    pub fn get_page(&mut self, page_num: u32) -> Result<PageHandle, Error> {
        match self.buffer_manager.get_page(page_num, &self.fp) {
            Err(e) => {
                dbg!(&e);
                Err(Error::GetPageError)
            },
            Ok(v) => {
                Ok(PageHandle::new(page_num, v))
            }
        }
    }

    pub fn get_first_page(&mut self) -> Result<PageHandle, Error> {
        let page_num = (self.header.file_num as u32) << 16;
        self.get_page(page_num)
    }

    pub fn unpin_page(&mut self, page_num: u32) -> Result<(), Error> {
        if let Err(e) = self.buffer_manager.unpin(page_num) {
            dbg!(&e);
            Err(Error::UnpinPageError)
        } else {
            Ok(())
        }
    }

    pub fn mark_dirty(&mut self, page_num: u32) -> Result<(), Error> {
        if let Err(e) = self.buffer_manager.mark_dirty(page_num) {
            dbg!(&e);
            Err(Error::MarkDirtyError)
        } else {
            Ok(())
        }
    }

    pub fn unpin_dirty_page(&mut self, page_num: u32) -> Result<(), Error> {
        match self.mark_dirty(page_num) {
            Ok(_) => {},
            err => {
                return err;
            }
        }
        self.unpin_page(page_num)
    }

    fn get_page_num(&self, page_index: usize) -> u32 {
        ((self.header.file_num as u32) << 16) | (page_index as u32)
    }
}
