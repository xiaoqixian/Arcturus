/**********************************************
  > File Name		: tests.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Fri 26 Mar 2021 08:26:32 AM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

const record_size: usize = 128;

use crate::page_management::page_file;
use std::io;
use std::io::prelude::*;

fn get_data() -> *mut u8 {
    use std::fs::OpenOptions;
    let mut fp = OpenOptions::new().read(true).write(false).open("/home/lunar/Documents/fzf").expect("Open file failed");
    let buffer = crate::utils::allocate_buffer(record_size);
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
    let mut pfm = page_file::PageFileManager::new();
    let mut rfh = super::record_file_manager::RecordFileManager::create_file(&String::from("Table1"), &mut pfm, record_size).expect("create rfh failed");
    let data = get_data();
    use crate::record_management::record_file_handle::RID;

    let mut recs: Vec<RID> = Vec::new();

    for i in 0..40 {
        match rfh.insert_record(data) {
            Ok(v) => {
                dbg!(v);
                recs.push(v);
            },
            Err(e) => {
                dbg!(e);
                panic!(format!("Insert {}th record error!", i));
            }
        }
    }

    println!("\n----------Inserting New Records----------\n");
    for i in 0..40 {
        match rfh.insert_record(data) {
            Ok(v) => {
                dbg!(v);
                recs.push(v);
            },
            Err(e) => {
                dbg!(e);
                panic!(format!("Insert {}th record error!", i));
            }
        }
    }

    println!("\n--------Deleting Records------------\n");
    for rec in &recs {
        dbg!(&rec);
        rfh.delete_record(rec).expect(format!("delete record {:?}  error", rec).as_str());
    }
    
    //rfh.delete_record(recs[39]).expect(format!("delete record {:?} error", recs[39]).as_str());
    //for i in 0..1 {
        //match rm.insert_record(data) {
            //Ok(v) => {
                //dbg!(v);
                //recs.push(v);
            //},
            //Err(e) => {
                //dbg!(e);
                //panic!(format!("Insert {}th record error!", i));
            //}
        //}
    //}
   

    std::fs::remove_file("~/pros/arcturus/Table1");
}
