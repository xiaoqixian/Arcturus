/**********************************************
  > File Name		: indexing/file_manager.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Tue 30 Mar 2021 09:10:51 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

/*
 * Indexing File Manager is in charge of creating an index file.
 * It uses page_management/page_file module as a client.
 */

use crate::page_management::page_file::{PageFileManager, PageFileHeader};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::collections::HashMap;
use std::mem::size_of;
use super::AttrType;
use crate::errors::IndexingError;

#[derive(Debug, Clone)]
pub struct IndexFileManager {
    num_files: u16,
    fps: HashMap<String, File>
}

impl IndexFileManager {
    pub fn new(num_files: u16) -> Self {
        Self {
            num_files,
            fps: HashMap::new()
        }
    }
    /*
     * index_num is for helping create the name of the index file, in 
     * case of duplicate names.
     */
    pub fn create_file(&mut self, file_name: &String, index_num: u32, attr_type: AttrType, attr_length: usize) -> Result<File, IndexingError> {
        if let Some(_) = self.fps.get(file_name) {
            return Err(IndexingError::FileExist);
        }
        if !Self::check_attr_validity(attr_type, attr_length) {
            dbg!(&(attr_type, attr_length));
            return Err(IndexingError::InvalidAttr);
        }

        let mut new_name = file_name.clone();
        new_name.push_str(&index_num.to_string());
        let res = OpenOptions::new().read(true).write(true).create(true).open(&new_name);
        if let Err(e) = res {
            dbg!(e);
            return Err(IndexingError::CreateFileError);
        }
        let f = res.unwrap();
        
        self.num_files += 1;
        self.fps.insert(new_name.clone(), f.try_clone().unwrap());

        //write in page file header.
        let pfh = PageFileHeader::new(self.num_files, attr_length);
        let sli = unsafe {
            std::slice::from_raw_parts(&pfh as *const _ as *const u8, size_of::<PageFileHeader>())
        };
        let write_bytes = f.write_at(sli, 0).expect("Unix Write Error");
        if write_bytes < size_of::<PageFileHeader>() {
            return Err(IndexingError::IncompleteWrite);
        }
        Ok(f.try_clone().unwrap())
    }

    pub fn open_file(&mut self, file_name: &String) -> Result<File, IndexingError> {
        if let Some(v) = self.fps.get(file_name) {
            return Ok(v.try_clone().unwrap());
        }
        let f = OpenOptions::new().read(true).write(true).open(file_name).expect("Open Index File Error");
        self.fps.insert(file_name, f.try_clone.unwrap());
        Ok(f.try_clone.unwrap())
    }

    fn check_attr_validity(attr_type: AttrType, attr_length: usize) -> bool {
        match attr_type {
            INT|FLOAT => {
                if attr_length == 4 {
                    true
                } else {
                    false
                }
            },
            STRING => {
                if attr_length <= MAX_STRING_LEN {
                    true
                } else {
                    false
                }
            }
        }
    }
}
