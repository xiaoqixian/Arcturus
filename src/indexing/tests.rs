/**********************************************
  > File Name		: indexing/tests.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Sat 08 May 2021 10:03:42 AM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

use crate::page_management::page_file;
use crate::record_management::record_file_manager;

const record_size: usize = 128;

fn init() -> (page_file::PageFileManager, record_file_manager::RecordFileManager) {
    (page_file::PageFileManager::new(), record_file_manager::RecordFileManager::create_file(&String::from("Table1"), &mut pfm, record_size).expect("Create RecordFileManager failed"))
}


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


fn records_insertion(pfm: &mut page_file::PageFileManager, rfh: &mut record_file_manager::RecordFileManager) {
    let data = get_data();
    use crate::record_management::record_file_handle::RID;

    let mut recs: Vec<RID> = Vec::new();

    for i in 0..240 {
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
}

#[test]
fn indexing_test1() {
    let v = init();
    let mut pfh = v.0;
    let mut rfh = v.1;

    records_insertion(&mut pfh, &mut rfh);
}
