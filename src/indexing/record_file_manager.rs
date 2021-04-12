/**********************************************
  > File Name		: record_file_manager.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Mon 12 Apr 2021 09:48:43 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::mem::size_of;
use std::{println as info, println as debug, println as warn, println as error};
use std::os::unix::fs::FileExt;

use crate::page_management::page_file::PageFileHandle;
use crate::errors::RecordError;


/*
 * RecordFileManager is an encapsulation of PageFileManager, as records
 * module use page_file to store records.
 * When need to create a new records file, a page is first allocated to
 * store the record file header.
 */

/*
 * Every record file represents a table, every table has a same record 
 * size.
 *
 * In a page, all records are managed by a bitmap. The size of the bitmap
 * is calculated by the record size.
 */
struct RecordFileHeader {

}

pub struct RecordFileManager {
    
}
