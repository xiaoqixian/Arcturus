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
use crate::buffer_manager::BufferManager;

/*
 * We need a data structure to represent a page.
 */
struct Page {
    page_num: i32, //page number
    page_data: Box<[char]>
}

struct PageFileHeader {
    first_free_page: i32, //the number of the first free page.
    num_pages: i32, //number of pages.
}

/*
 * The PageFileManager class handles the creation, deletion, opening, 
 * and closing of paged files, along with the allocation and disposal
 * of scratch pages.
 */
struct PageFileManager {
    fp: &File, //opend file reference.
    open_flag: i32, //file open flag.
    changed_flag: i32, //mark if the file is changed.
    file_header: PageFileHeader,
    buffer_manager: BufferManager
}


