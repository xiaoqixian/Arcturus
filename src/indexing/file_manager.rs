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
use std::mem::size_of;
use super::AttrType;
use super::index_handle::{IndexHandle, IndexFileHeader};
use crate::errors::IndexingError;
use std::io::ErrorKind;
use crate::utils;

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
    pub fn open_file(file_name: &String, index_num: u32, pfm: &mut PageFileManager,  attr_type: AttrType, attr_length: usize) -> Result<IndexHandle, IndexingError> {
        if !Self::check_attr_validity(attr_type, attr_length) {
            dbg!(&(attr_type, attr_length));
            return Err(IndexingError::InvalidAttr);
        }

        let mut new_name = file_name.clone();
        new_name.push_str(&index_num.to_string());
        
        let mut pfh = match pfm.open_file(&new_name) {
            Err(e) => {
                dbg!(&e);
                return Err(IndexingError::FileOpenError);
            },
            Ok(v) => v
        };
        
        let header_ph = match pfh.get_first_page() {
            Err(e) => {
                dbg!(&e);
                return Err(IndexingError::GetFirstPageError);
            },
            Ok(v) => v
        };

        let header = utils::get_header::<IndexFileHeader>(header_ph.get_data());

        let mut root_ph = match pfh.allocate_page() {
            Err(e) => {
                dbg!(e);
                return Err(IndexingError::AllocatePageError);
            },
            Ok(v) => v
        };

        Ok(IndexHandle::new(&mut pfh, header, root_ph))
    }
    
    pub fn create_file(file_name: &String, index_num: u32, pfm: &mut PageFileManager,  attr_type: AttrType, attr_length: usize) -> Result<IndexHandle, IndexingError> {
        if !Self::check_attr_validity(attr_type, attr_length) {
            dbg!(&(attr_type, attr_length));
            return Err(IndexingError::InvalidAttr);
        }

        let mut new_name = file_name.clone();
        new_name.push_str(&index_num.to_string());
        
        let mut pfh = match pfm.create_file(&new_name) {
            Err(e) => {
                dbg!(&e);
                return Err(IndexingError::FileCreationError);
            },
            Ok(v) => v
        };

        let header_ph = match pfh.allocate_page() {
            Err(e) => {
                dbg!(e);
                return Err(IndexingError::AllocatePageError);
            },
            Ok(v) => v
        };

        let root_ph = match pfh.allocate_page() {
            Err(e) => {
                dbg!(e);
                return Err(IndexingError::AllocatePageError);
            },
            Ok(v) => v
        };

        let header = IndexFileHeader::new(attr_length, attr_type, root_ph.get_page_num());

        unsafe {
            std::ptr::copy(&header as *const _ as *const u8, header_ph.get_data(), size_of::<IndexFileHeader>());
        }

        Ok(IndexHandle::new(&mut pfh, &header, root_ph))
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
