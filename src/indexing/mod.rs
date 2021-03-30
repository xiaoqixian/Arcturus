/**********************************************
  > File Name		: mod.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Mon 29 Mar 2021 10:46:17 AM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

/*
 * The indexing component provides classes and methods for managing 
 * persistent indexes over unordered data reocrds stored in paged files.
 * The indexes ultimately will be used to speed up processing of relational
 * selections, joins and condition-based update and delete operations.
 *
 * The indexes are stored in paged files, just like records data themselves.
 * So we will use page_management module as a client to manage paged indexes.
 *
 * B+ tree the data structure will be selected for implementing indexes 
 * management when they are read into memory.
 */

static MAX_STRING_LEN: usize = 255;

#[derive(Debug, Copy, Clone)]
enum AttrType {
    INT,
    FLOAT,
    STRING
}
