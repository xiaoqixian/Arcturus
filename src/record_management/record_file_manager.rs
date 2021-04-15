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

use crate::page_management::page_file::{PageFileHandle, PageHandle, PageFileManager, PAGE_SIZE};
use crate::errors::{RecordError, Error};
use super::record_file_handle::{RecordFileHeader, RecordFileHandle, RecordPageHeader};


/*
 * RecordFileManager is an encapsulation of PageFileManager, as records
 * module use page_file to store records.
 * When need to create a new records file, a page is first allocated to
 * store the record file header.
 */

pub struct RecordFileManager {

}

impl RecordFileManager {
    fn calc_num_records_per_page(record_size: usize) -> usize {
        8*PAGE_SIZE/(8*record_size + 1)
    }

    fn calc_bitmap_size(size: usize) -> usize {
        let mut bitmap_size: usize = size/8;
        if bitmap_size * 8 < size {
            bitmap_size += 1;
        }
        bitmap_size
    }

    pub fn create_file(file_name: &String, pfm: &mut PageFileManager, record_size: usize) -> Result<RecordFileHandle, Error> {
        let mut pfh = match pfm.create_file(file_name) {
            Err(e) => {
                return Err(e);
            },
            Ok(v) => v
        };
        let ph = match pfh.allocate_page() {
            Ok(v) => v,
            Err(e) => {
                return Err(e);
            }
        };
        dbg!(&ph);
        let data = ph.get_data();
        let header = unsafe {
            &mut *(data as *mut RecordFileHeader)
        };
        header.bitmap_offset = size_of::<RecordPageHeader>();
        header.num_records_per_page = Self::calc_num_records_per_page(record_size);
        header.bitmap_size = Self::calc_bitmap_size(header.num_records_per_page);
        header.records_offset = header.bitmap_offset + header.bitmap_size;
        header.num_pages = 0;
        header.record_size = record_size;
        dbg!(&header);

        if let Err(e) = pfh.unpin_dirty_page(ph.get_page_num()) {
            return Err(e);
        }

        Ok(RecordFileHandle::new(ph.get_page_num(), *header, &mut pfh))
    }

    pub fn open_file(file_name: &String, pfm: &mut PageFileManager, record_size: usize) -> Result<RecordFileHandle, Error> {
        let mut pfh = match pfm.open_file(file_name) {
            Err(e) => {
                return Err(e);
            },
            Ok(v) => v
        };
        let ph = match pfh.get_first_page() {
            Err(e) => {
                return Err(e);
            },
            Ok(v) => v
        };
        let data = ph.get_data();
        let header = unsafe {
            &mut *(data as *mut RecordFileHeader)
        };
        
        if let Err(e) = pfh.unpin_page(ph.get_page_num()) {
            return Err(e);
        }

        Ok(RecordFileHandle::new(ph.get_page_num(), *header, &mut pfh))
    }
}
