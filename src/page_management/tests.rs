/**********************************************
  > File Name		: page_management/tests.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Fri 26 Mar 2021 10:20:35 AM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

use super::buffer_manager::*;
use super::page_file::*;
use std::fs::File;
use std::fs::OpenOptions;

fn open() -> File {
    OpenOptions::new().read(true).write(true).open("/home/lunar/Documents/fzf").unwrap()
}

/*
 * Test1:
 * Read 128 pages, make them dirty, unpin half of them, then read another 128 pages.
 */
//#[test]
//fn buffer_manager_test1() {
    //let mut buffer = BufferManager::new(128);
    //let mut f = OpenOptions::new().read(true).write(true).open("/home/lunar/Documents/fzf").unwrap();
    //let file_num: u32 = 1<<16;
    //for i in 0..128 {
        //match buffer.get_page(file_num | (i as u32), &f) {
            //Err(_) => {
                //panic!("read page_num={:#010x} failed", file_num|(i as u32));
            //},
            //Ok(mut v) => {
                //let page = unsafe {
                    //&mut *v.as_ptr()
                //};
                //page.mark_dirty();
                ////dbg!(&page);
            //}
        //}
    //}
    //for i in 0..64 {
        //println!("unpin page_num={:#010x}", file_num | (i as u32));
        //buffer.unpin(file_num | (i as u32));
    //}
    //println!("Unpin finished");
    //for i in 0..32 {
        //match buffer.get_page(file_num | (i as u32), &f) {
            //Err(_) => {
                //panic!("read page_num={:#010x} failed");
            //},
            //Ok(_) => {}
        //}
    //}
    //for i in 128..256 {
        //match buffer.get_page(file_num | (i as u32), &f) {
            //Err(_) => {
                //panic!("read page_num={:#010x} failed", file_num|(i as u32));
            //},
            //Ok(_) => {}
        //}
    //}
//}

/*
 * Page File Unit Test1.
 * 
 */
#[test]
fn page_file_test1() {
    let mut pf = crate::page_management::page_file::PageFileManager::new();
    let table_name = String::from("Table1");
    pf.create_file(&table_name);
    let mut fh = pf.open_file(&table_name).expect("");
    for i in 0..128 {
        let p = fh.allocate_page();
        if let Err(e) = p {
            dbg!(&e);
            panic!("get {}th page error", i);
        }
        let ph = p.unwrap();
        dbg!(&ph);
        fh.mark_dirty(ph.get_page_num());
        fh.unpin_page(ph.get_page_num());
    }
}

