/**********************************************
  > File Name		: tests.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Fri 26 Mar 2021 08:26:32 AM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

const record_size: usize = 128;

use super::record_manager::RecordManager;
use super::file_manager::FileManager;
use std::io;
use std::io::prelude::*;

fn init() -> RecordManager {
    let mut fm = FileManager::new();
    let table_name = String::from("Table1");
    let mut fp = match fm.open_file(&table_name) {
        Err(_) => {
            match fm.create_file(&table_name, record_size) {
                Err(e) => {
                    dbg!(e);
                    panic!("create file error");
                },
                Ok(v) => v
            }
        },
        Ok(v) => v
    };
    RecordManager::new(&mut fp, record_size)
}

fn get_data() -> *mut u8 {
    use std::fs::OpenOptions;
    let mut fp = OpenOptions::new().read(true).write(false).open("/home/lunar/Documents/fzf").expect("Open file failed");
    let buffer = crate::page_management::buffer_manager::BufferManager::allocate_buffer(record_size);
    let sli = unsafe {
        std::slice::from_raw_parts_mut(buffer, record_size)
    };
    let res = fp.read(sli);
    if let Err(e) = res {
        panic!("read error");
    }
    let read_bytes = res.unwrap();
    if read_bytes < record_size {
        panic!("Incomplete read");
    }
    buffer
}

#[test]
fn record_manager_test1() {
    let mut rm = init();
    let data = get_data();

    dbg!(&rm.page_file_manager);
//    let mut p = rm.page_file_manager.allocate_page().expect("allocate page error");
    //let page = unsafe {
        //p.as_mut()
    //};
    //dbg!(&page);
    //let slot_num = rm.get_free_slot(page).expect("free slot error");
    //dbg!(&slot_num);
    //let offset = rm.get_record_offset(slot_num).expect("record offset error");
    //dbg!(&offset);

    for i in 0..128 {
        match rm.insert_record(data) {
            Ok(v) => {
                dbg!(v);
            },
            Err(e) => {
                dbg!(e);
                panic!(format!("Insert {}th record error!", i));
            }
        }
    }
    std::fs::remove_file("~/pros/arcturus/Table1");
}
