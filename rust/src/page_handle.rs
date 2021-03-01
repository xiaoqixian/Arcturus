/**********************************************
  > File Name		: page_handle.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Mon 01 Mar 2021 04:27:24 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

static PAGE_SIZE: i32 = 4096;

struct PageHandle {
    page_num: i32,
    page_data: Box<[char]>,
}

impl PageHandle {
    pub fn new(page_num: i32, page_data: ) -> Self {
        PageHandle 
    }

    pub fn get_page_num(&self) -> i32 {
        self.page_num
    }

    pub fn get_data(&self) -> Box<[char]> {
        self.page_data
    }
}

fn main() {

}
