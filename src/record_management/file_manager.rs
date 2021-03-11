/**********************************************
  > File Name		: file_manager.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Wed 10 Mar 2021 09:22:10 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

pub struct FileHeader {
    num_pages: usize,
}

impl FileHeader {
    pub fn new() -> Self {
        FileHeader {
            num_pages: 0
        }
    }
}
