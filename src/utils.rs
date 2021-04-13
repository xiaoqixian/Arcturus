/**********************************************
  > File Name		: utils.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Thu 11 Mar 2021 03:54:41 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

/*
 * Utils functions for global usage.
 */

/*
 * Write a data structure into a file.
 */


pub fn allocate_buffer(size: usize) -> *mut u8 {
    use std::alloc::{self, Layout};
    use std::mem::size_of;
    let layout = Layout::from_size_align(size, size_of::<u8>()).expect("create layout error");
    unsafe {
        alloc::alloc(layout)
    }
}

pub fn deallocate_buffer(ptr: *mut u8, size: usize) {
    use std::alloc::{self, Layout};
    use std::mem::size_of;
    let layout = Layout::from_size_align(size, size_of::<u8>()).expect("create layout error");
    unsafe {
        alloc::dealloc(ptr, layout);
    }
}
