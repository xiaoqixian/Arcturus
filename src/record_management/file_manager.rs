/**********************************************
  > File Name		: file_manager.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Wed 10 Mar 2021 09:22:10 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

/*
 * File Manager.
 * Client for table creating, table opening, table closing, etc.
 * As each file represents a table.
 * FileManager maintains a hashmap which contains all file pointers 
 * of opend tables. Key: file name, Value: file pointer.
 */

use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::mem::size_of;
use std::{println as info, println as debug, println as warn, println as error};
use std::os::unix::fs::FileExt;

use crate::page_management::page_file::PageFileHeader;
use crate::errors::RecordError;

pub struct FileManager {
    num_files: u32,
    fps: HashMap<String, File>
}

impl FileManager {
    pub fn new() -> Self {
        Self {
            num_files: 0,
            fps: HashMap::new()
        }
    }

    pub fn create_file(&mut self, file_name: &String, record_size: usize) -> Result<File, RecordError> {
        if let Some(_) = self.fps.get(file_name) {
            debug!("Table already exists");
            return Err(RecordError::FileExist);
        }
        let fp = OpenOptions::new().read(true).write(true).create(true).open(file_name).expect("Create file error");

        //write in file header.
        self.num_files += 1;
        let page_file_header = PageFileHeader::new(self.num_files as u16, record_size);
        dbg!(&page_file_header);
        let sli = unsafe {
            std::slice::from_raw_parts(&page_file_header as *const _ as *const u8, size_of::<PageFileHeader>())
        };

        let write_bytes = fp.write_at(sli, 0).expect("Write File Header Error");
        if write_bytes < size_of::<PageFileHeader>() {
            debug!("Write File Header Error");
            return Err(RecordError::IncompleteWrite);
        }

        dbg!(&write_bytes);

        self.fps.insert(file_name.clone(), fp.try_clone().unwrap());
        Ok(fp.try_clone().unwrap())
    }

    pub fn open_file(&mut self, file_name: &String) -> Result<File, RecordError> {
        if let Some(v) = self.fps.get(file_name) {
            return Ok(v.try_clone().unwrap());
        }
        let fp = match File::open(file_name) {
            Err(e) => {
                dbg!(&e);
                return Err(RecordError::FileOpenError);
            },
            Ok(v) => v
        };
        self.fps.insert(file_name.clone(), fp.try_clone().unwrap());
        Ok(fp.try_clone().unwrap())
    }
}
