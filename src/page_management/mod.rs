/**********************************************
  > File Name		: mod.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Tue 02 Mar 2021 10:31:37 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

pub mod buffer_manager;
pub mod page_file;

#[cfg(test)]
mod tests {
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
    #[test]
    fn buffer_manager_test1() {
        let mut buffer = BufferManager::new(5000);
        let mut f = OpenOptions::new().read(true).write(true).open("/home/lunar/Documents/fzf").unwrap();
        let file_num: u32 = 1<<16;
        for i in 0..128 {
            match buffer.get_page(file_num | (i as u32), &f) {
                None => {
                    panic!("read page_num={:#010x} failed", file_num|(i as u32));
                },
                Some(mut v) => {
                    let page = unsafe {
                        &mut *v.as_ptr()
                    };
                    page.mark_dirty();
                    //dbg!(&page);
                }
            }
        }
        for i in 0..64 {
            println!("unpin page_num={:#010x}", file_num | (i as u32));
            buffer.unpin(file_num | (i as u32));
        }
        println!("Unpin finished");
        for i in 0..32 {
            match buffer.get_page(file_num | (i as u32), &f) {
                None => {
                    panic!("read page_num={:#010x} failed");
                },
                Some(_) => {}
            }
        }
        for i in 128..256 {
            match buffer.get_page(file_num | (i as u32), &f) {
                None => {
                    panic!("read page_num={:#010x} failed", file_num|(i as u32));
                },
                Some(_) => {}
            }
        }
    }

    #[test]
    fn buffer_manager_test2() {
        let mut buffer = BufferManager::new(5000);
        let mut f = OpenOptions::new().read(true).write(true).open("/home/lunar/Documents/fzf").unwrap();
        let file_num: u32 = 1<<16;
        for i in 0..128 {
            match buffer.get_page(file_num | (i as u32), &f) {
                None => {
                    panic!("read page_num={:#010x} failed", file_num|(i as u32));
                },
                Some(mut v) => {
                    let page = unsafe {
                        &mut *v.as_ptr()
                    };
                    page.mark_dirty();
                    //dbg!(&page);
                }
            }
        }

        for i in 0..64 {
            buffer.unpin(file_num | (i as u32));
        }
        for i in 0..32 {
            match buffer.get_page(file_num | (i*2 as u32), &f) {
                None => {
                    panic!("read page_num={:#010x} failed");
                },
                Some(_) => {}
            }
        }
    }
   
    /*
     * Page File Unit Test1.
     * 
     */
    #[test]
    fn page_file_test1() {
        let f = open();
        let mut pf = crate::page_management::page_file::PageFileManager::new(&f);
        let file_num: u32 = 1<<16;
        for i in 0..128 {
            let p = pf.allocate_page();
            dbg!(&p);
            if let Err(e) = p {
                dbg!(&e);
                panic!("get {}th page error", i);
            }
            pf.unpin_page(p.unwrap());
        }
    }
}
