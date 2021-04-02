/**********************************************
  > File Name		: data_manager.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Fri 02 Apr 2021 11:28:19 AM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::mem::size_of;
use std::io::ErrorKind;

static META_DATA_FILE_NAME: &'static str = "DB_META_FILE";

#[derive(Debug, Copy, Clone)]
struct DBMetaData {
    num_record_files: u16,
    num_indexing_files: u16
}

pub struct DatabaseManager {
    meta_data: DBMetaData,
    fp: File
}

impl DatabaseManager {
    pub fn new() -> Self {
        let mut meta_data = DBMetaData {
            num_record_files: 0,
            num_indexing_files: 0
        };

        let mut fp: File;
        match File::open(&META_DATA_FILE_NAME) {
            Ok(v) => {
                fp = v;
            },
            Err(e) => match e.kind() {
                ErrorKind::NotFound => {
                    fp = Self::create(&meta_data);
                },
                other_error => {
                    panic!(String::from(format!("Database Metadata File Open Error: {:?}", other_error)));
                }
            }
        }

        let sli = unsafe {
            std::slice::from_raw_parts_mut(&mut meta_data as *mut _ as *mut u8, size_of::<DBMetaData>())
        };
        let read_bytes = fp.read_at(sli, 0).expect("Unix Read Error");
        if read_bytes < size_of::<DBMetaData>() {
            dbg!(read_bytes);
            panic!("Database Metadata Incomplete Read: {}");
        }

        dbg!(&meta_data);

        Self {
            meta_data: meta_data,
            fp: fp.try_clone().unwrap()
        }
    }

    pub fn close(&mut self, num_record_files: u16, num_indexing_files: u16) {
        let meta_data = DBMetaData {
            num_record_files,
            num_indexing_files
        };
        dbg!(&meta_data);
        let sli = unsafe {
            std::slice::from_raw_parts(&meta_data as *const _ as *const u8, size_of::<DBMetaData>())
        };
        let write_bytes = self.fp.write_at(sli, 0).expect("Unix Write Error");
        if write_bytes < size_of::<DBMetaData>() {
            dbg!(write_bytes);
            panic!("Database Metadata Incomplete Write");
        }
    }

    fn create(meta_data: &DBMetaData) -> File {
        let mut fp = OpenOptions::new().read(true).write(true).create(true).open(&META_DATA_FILE_NAME).expect("Database Metadata File Creation Error");
        let sli = unsafe {
            std::slice::from_raw_parts(meta_data as *const _ as *const u8, size_of::<DBMetaData>())
        };
        let write_bytes = fp.write_at(sli, 0).expect("Unix Write Error");
        if write_bytes < size_of::<DBMetaData>() {
            dbg!(write_bytes);
            panic!("Database Metadata Incomplete Write");
        }
        fp
    }
}
