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
use std::io::ErrorKind;

#[derive(Debug)]
pub struct IndexFileManager {
}

impl IndexFileManager {
    /*
     * open a file, if the file not found, create one.
     *
     * index_num is for helping create the name of the index file, in 
     * case of duplicate names.
     */
    pub fn open_file(file_name: &String, index_num: u32, attr_type: AttrType, attr_length: usize) -> Result<File, IndexingError> {
        if let Some(_) = self.fps.get(file_name) {
            return Err(IndexingError::FileExist);
        }
        if !Self::check_attr_validity(attr_type, attr_length) {
            dbg!(&(attr_type, attr_length));
            return Err(IndexingError::InvalidAttr);
        }

        let mut new_name = file_name.clone();
        new_name.push_str(&index_num.to_string());
        
        let mut fp: File;
        match File::open(&new_name) {
            Ok(v) => {
                fp = v.try_clone().unwrap();
            },
            Err(e) => match e.kind() {
                ErrorKind::NotFound => {
                    fp = OpenOptions::new().read(true).write(true).create(true).open(&new_name).expect("Create File Error");
                },
                other_error => {
                    dbg!(&other_error);
                    panic!(true);
                }
            }
        }
        
        self.num_files += 1;
        self.fps.insert(new_name.clone(), fp.try_clone().unwrap());

        //write in page file header.
        let pfh = PageFileHeader::new(self.num_files, attr_length);
        let sli = unsafe {
            std::slice::from_raw_parts(&pfh as *const _ as *const u8, size_of::<PageFileHeader>())
        };
        let write_bytes = fp.write_at(sli, 0).expect("Unix Write Error");
        if write_bytes < size_of::<PageFileHeader>() {
            dbg!(&write_bytes);
            return Err(IndexingError::IncompleteWrite);
        }
        Ok(f.try_clone().unwrap())
    }

    fn check_attr_validity(attr_type: AttrType, attr_length: usize) -> bool {
        match attr_type {
            AttrType::INT | AttrType::FLOAT => {
                if attr_length == 4 {
                    true
                } else {
                    false
                }
            },
            AttrType::STRING => {
                if attr_length <= super::MAX_STRING_LEN {
                    true
                } else {
                    false
                }
            }
        }
    }
}
