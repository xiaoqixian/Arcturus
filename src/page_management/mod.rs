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
    /*
     * Test1:
     * Read 129 pages, none of them get unpinned, see if the buffer
     * will resized.
     */
//    #[test]
    //fn buffer_manager_test1() {
        //let mut buffer = BufferManager::new();
        //let f = OpenOptions::new().read(true).write(true).open("/home/lunar/Documents/fzf").unwrap();
        //let file_num: u32 = 1<<16;
        //for i in 0..129 {
            //match buffer.get_page(file_num | (i as u32), &f) {
                //None => {
                    //panic!("read page_num={:#010x} failed", file_num|(i as u32));
                //},
                //Some(_) => {}
            //}
        //}
    //}

    /*
     * Test2:
     * Read 128 pages, make them dirty, unpin half of them, then read another 128 pages.
     */
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
                    dbg!(&page);
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

    /*
     * BufferManager Test3
     * Discontinously Read Pages.
     */
    //#[test]
    //fn buffer_manager_test3() {
        //let mut buffer = BufferManager::new();
        //let mut f = OpenOptions::new().read(true).write(true).open("/home/lunar/Documents/fzf").unwrap();
        //let file_num: u32 = 1<<16;
        //for i in 0..65 {
            //let page_num = file_num | (2*i as u32);
            //match buffer.get_page(page_num, &f) {
                //None => {
                    //panic!("Reading page_num={:#010x} failed", page_num);
                //},
                //Some(v) => {
                    
                //}
            //}
        //}
    //}
    
    fn open() -> File {
        OpenOptions::new().read(true).write(true).open("/home/lunar/Documents/fzf").unwrap()
    }

}
