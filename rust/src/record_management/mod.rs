/**********************************************
  > File Name		: mod.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Fri 05 Mar 2021 03:35:53 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

/*
 * The Record Management component provides classes and methods for managing
 * files of unordered records. 
 * 
 * We will store records in paged files provided by the PageFile component.
 * To manage file contents conveniently, we will use the first page of each
 * file as a special header page which contains free space information.
 *
 * To simplify our task, we assume that every record in one page file are
 * the same size. Although record sizes may differ across files. 
 */

use super::page_file::*;

struct RID {
    page_num: i32,
    slot_num: i32
}

struct Record {
    record_size: i32,
    rid: RID,
    data: Vec<u8>
}


