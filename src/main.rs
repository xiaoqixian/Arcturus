use std::fs::OpenOptions;
use rust::page_management::buffer_manager;

fn main() {
    let mut buffer = buffer_manager::BufferManager::new();
    let mut f = OpenOptions::new().read(true).write(true).open("/home/lunar/Documents/fzf").unwrap();
    let file_num: u32 = 1<<16;
    for i in 0..129 {
        let page_num = file_num | (2*i as u32);
        match buffer.get_page(page_num, &f) {
            None => {
                panic!("Reading page_num={:#010x} failed", page_num);
            },
            Some(v) => {}
        }
    }
}
