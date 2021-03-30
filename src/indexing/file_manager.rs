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
use super::AttrType;

#[derive(Debug, Clone)]
pub struct IndexFileManager {
    page_file_manager: PageFileManager
}

impl IndexFileManager {
    /*
     * index_num is for helping create the name of the index file, in 
     * case of duplicate names.
     */
    pub fn create_file(file_name: &mut String, index_num: u32, attr_type: AttrType, attr_length: usize) -> File {
        let new_name = file_name.push_str(&index_num.to_string());
        let f = OpenOptions::new().read(true).write(true).create(true).open(new_name).expect("Create Index File Error");
        
        //write in page file header.
        let 
    }

    pub fn open_file(file_name: &String) -> File {
        OpenOptions::new().read(true).write(true).open(file_name).expect("Open Index File Error")
    }

    fn check_attr_validity(attr_type: AttrType, attr_length: usize) -> bool {
        match attr_type {
            
        }
    }
}
