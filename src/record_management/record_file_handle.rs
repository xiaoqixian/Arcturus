/**********************************************
  > File Name		: record_file_handle.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Mon 12 Apr 2021 11:01:57 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

/*
 * Every record file represents a table, every table has a same record 
 * size.
 *
 * In a page, all records are managed by a bitmap. The size of the bitmap
 * is calculated by the record size.
 *
 * Page data layout: bitmap | records.
 */
#[derive(Debug, Copy, Clone)]
struct RecordFileHeader {
    record_size: usize,
    bitmap_offset: usize,
    bitmap_size: usize,
    records_offset: usize,
    num_records_per_page: usize,
    num_pages: usize
}


#[derive(Debug, Copy, Clone)]
pub struct RecordPageHeader {
    num_records: usize,
    next_free: u32,//page num of the next free page.
}

#[derive(Debug)]
pub struct RecordFileHandle {
    header_num: u32,//page num of the header, that's right, header is stored in one of the pages. When the page file is about to be closed, we use the header_num to get the page and copy the header of this handle into it.
    header: RecordFileHeader,
    pfh: PageFileHandle
}

impl RecordFileHandle {
    pub fn new(header_num: u32, header: RecordFileHeader, pfh: &PageFileHandle) -> Self {
        Self {
            header_num,
            header,
            pfh: pfh.clone()
        }
    }
}
