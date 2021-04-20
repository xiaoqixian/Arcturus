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


//provide a raw pointer, offset, and array length. 
//return a reference to an array.
pub fn get_arr<T>(p: *const u8, offset: usize, len: usize) -> &'static [T] {
    unsafe {
        let ap = p.offset(offset as isize) as *const T;
        std::slice::from_raw_parts(ap, len)
    }
}

pub fn get_arr_mut<T>(p: *mut u8, offset: usize, len: usize) -> &'static mut [T] {
    unsafe {
        let ap = p.offset(offset as isize) as *mut T;
        std::slice::from_raw_parts_mut(ap, len)
    }
}

//get header from a raw pointer. offset is 0 by default
//this is generic function.
pub fn get_header<T>(data: *mut u8) -> &'static T {
    unsafe {
        & *(data as *const T)
    }
}

pub fn get_header_mut<T>(data: *mut u8) -> &'static mut T {
    unsafe {
        &mut *(data as *mut T)
    }
}
