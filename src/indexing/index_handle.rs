/**********************************************
  > File Name		: index_manager.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Tue Apr  6 10:24:27 2021
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

/*
 * All indexing entries are stored in pages, while a page 
 * is represented by a B+ tree node. 
 * The tree nodes are divided into internal nodes and leaf nodes.
 *
 * All nodes contain a header and many entries. These entries are
 * linked together like a linked list. In the header, we just need 
 * to keep the first slot(which points to the minimum entry) and 
 * the free slot(all free slots are linked together) for new 
 * entries insertion. 
 *
 * Every entry has two types: new and duplicate.
 * In the previous step, when we compare an entry to be interted with
 * all existed entries. We will find if equivalent entries exist. If 
 * so, a dup parameter(a reference passed in) will be set true. In that
 * case, the newly inserted entry will set as duplicate entry, otherwise
 * will be set as new entry.
 *
 * We use buckets to manage duplicate entries. Buckets are also stored in
 * pages.
 * If it's a duplicate entry, we need to check if the previous entry is a 
 * new entry, as the previous entry is equal to the current one. If not,
 * means there is already a bucket for duplicate entries with the same value
 * as the current entry. Else, a new bucket need to be created. Create a new
 * bucket is just to ask for a new page from the page_file_manager and initialize
 * it. 
 * Then we insert entries into the bucket. If the bucket is newly created, then we
 * need to insert the previous entry and this one. Otherwise we just need to insert
 * this one. 
 * A bucket may have multiple pages. If one page is full, we create a new page and 
 * link all pages together.
 * 
 * Page/Node layout:
 *     1. header: store data of the page, including RID associated with the index.
 *     2. entries: array of all entries metadata.
 *     3. keys
 *
 * Entry Insertion:
 *  parameters:
 *    data,
 *    RID associated with the index.
 *  steps:
 *    1. retrieve the root header
 *    2. if the root is full, create a new root node. And split the previous root
 *       node, make the new root node their parent node.
 * 
 * Insert Into a Non Full Node:
 *  parameters:
 *      1. Header of the node that we need to insert into.
 *      2. data to be inserted.
 *  steps:
 *    check if it's a leaf node or an internal node:
 *      1. If it's a leaf node, directly intert into it. All insertion details have 
 *         been metioned above.
 *      2. If it's an internal node, first find an appropriate location or should I
 *         say a page index. Then check if the page is full, if it's full, we need to
 *         split the node. I will elaborate how to split a node below.
 *         After split, we recursively call this method except the first parameter is 
 *         the new node.
 *
 * Node Split:
 *  parameters:
 *    1. Header of the parent node.
 *    2. old header: the header of the full node to split.
 *    3. old page num: the page num of the full node to split.
 *    4. index: the index into which to insert the new node into the parent node.
 *    5. new key index: the index of the first key that points to the new node.
 *    6. new page num: page num of the new node.
 *  steps:
 *    If it's an interna node:
 *      1. move half of max number of entries to the new node.
 *         Including entries data and attribute data.
 *      2. insert parent key into parent at index specified in parameters.
 *         Then we just need to copy the key at the index
 *         to parent node. Corresponding entries are updated too.
 *      3. And if it's a leaf node, new node and old node have to be linked together.
 * 
 * DeleteFromLeaf:
 *   parameters:
 *     1. node header
 *     2. data
 *     3. RID reference associated with the index
 *     4. toDelete: a bool reference, set true if the node become empty after deletion.
 *   steps:
 *     1. find the appropriate index, check if it's a duplicate entry.
 *        If it's duplicate, delete from the corresponding bucket.
 *        Else, just delete it from entries and keys.
 *     2. If the leaf is empty, delete it from the tree. 
 *        According to the source code, there is no such mechanism that nodes that has 
 *        less than half of max keys will merge with each other.
 *
 * DeleteFromBucket:
 *   parameters:
 *     1. bucket header
 *    returns:
 *     1. RID signifying the last RID remaining
 *     2. next bucket page num that this bucket points to.
 *   steps:
 *     1. check if this bucket has a next bucket, if so, search first in the next
 *        bucket(recursively calls this method).
 *     2. If this is the last bucket, search in all entries and check for an entry 
 *        that page num and slot num match RID.
 *        If this bucket has 1 or less key left, then just delete it.
 *     3. If this is not the last bucket, after our search in the next bucket. If
 *        the deletePage flag is set, and there is 1 or less key left in the next 
 *        bucket, the next bucket is deleted.
 *
 * DeleteFromNode:
 *   A recursive function.
 *   parameters:
 *     1.  
 *
 * Entry Deletion:
 *  Let me make this clear, we only delete one entry at a time, and the entry is 
 *  identified by a RID provided. Delete all entries that have a same index value
 *  part is in the IndexScan module.
 *  parameters:
 *    data,
 *    RID associated with the index.
 *  steps:
 *    
 */

use super::AttrType;
use crate::page_management::page_file::{PageHandle, PageFileHandle};
use crate::errors::{IndexingError, Error};
use crate::utils;
use std::cmp::Ordering;
use crate::record_management::record_file_handle::{RID};

const NO_MORE_SLOTS: u32 = 0xffffffff;//as 0 is a valid slot num, so we use 0xffffffff to represent a invalid slot_num.
const BEGINNING_OF_SLOT: u32 = 0xfffffffe;

#[derive(Debug, Copy, Clone)]
pub struct IndexFileHeader {
    num_entries: usize,
    attr_length: usize,
    attr_type: AttrType,
    
    keys_offset: usize,
    node_entries_offset: usize,
    bucket_entries_offset: usize,

    max_keys: usize,
    max_node_entries: usize,
    max_bucket_entries: usize,

    root_page: u32,

    //comparator: fn(val1: &T, val2: &T) -> std::cmp::Ordering
}

#[derive(Debug, Copy, Clone)]
pub struct EntryHeader {
    is_leaf: bool,
    is_empty: bool,

    num_keys: usize,
    free_slot: u32,
    first_slot: u32,//the pointer to the first node of the linked list.

    num1: u32, //invalid unless the entry is determined as a leaf node or an internal node.
    num2: u32, 
}

#[derive(Debug, Copy, Clone)]
pub struct LeafHeader {
    is_leaf: bool,
    is_empty: bool,

    num_keys: usize,
    free_slot: u32,
    first_slot: u32,//the pointer to the first node of the linked list.

    prev_page: u32,
    next_page: u32
}

#[derive(Debug, Copy, Clone)]
pub struct InternalHeader {
    is_leaf: bool,
    is_empty: bool,

    num_keys: usize,
    free_slot: u32,
    first_slot: u32,//the pointer to the first node of the linked list.

    first_child: u32,//page num of the first child node.
    num2: u32
}

#[derive(Debug, Copy, Clone)]
struct BucketHeader {
    num_keys: usize,
    free_slot: u32,
    first_slot: u32,
    next_bucket: u32
}

#[derive(Debug, Copy, Clone)]
enum EntryType {
    Unoccupied,
    New,
    Duplicate,//set when an index entry with a same value is inserted. At that time, the entry is linekd to a bucket which contains all relative RIDs.
}

#[derive(Debug, Copy, Clone)]
struct NodeEntry {
    et_type: EntryType,
    next_slot: u32,//points to the next entry in the node.
    page_num: u32,//page_num and slot_num associated with the record.
    slot_num: u32,//if this is a duplicate entry, the page_num is set to be the bucket page num.
}

#[derive(Debug, Copy, Clone)]
struct BucketEntry {
    next_slot: u32,
    page_num: u32,
    slot_num: u32,
}

/*
 * As we have three value types to consider about, we use generics to write a 
 * general handle. 
 */
#[derive(Debug)]
pub struct IndexHandle {
    header: IndexFileHeader,
    header_changed: bool,
    pfh: PageFileHandle,
    root_ph: PageHandle //PageHandle associated with the root page.
}

impl IndexHandle {
    pub fn new(pfh: &mut PageFileHandle, header: &IndexFileHeader, root_ph: PageHandle) -> Self {
        Self {
            header: *header,
            header_changed: false,
            pfh: pfh.clone(),
            root_ph,
        }
    }

    pub fn insert_entry(&mut self, data: *mut u8, rid: &RID) -> Result<(), Error> {
        let root_header = unsafe {
            &mut *(self.root_ph.get_data() as *mut EntryHeader)
        };
        
        //if the root page is full.
        if root_header.num_keys == self.header.max_keys {
            let new_ph = match self.create_new_node(false) {
                Err(e) => {
                    dbg!(&e);
                    return Err(Error::CreateNewNodeError);
                },
                Ok(v) => v
            };
            let new_root_header = unsafe {
                &mut *(new_ph.get_data() as *mut InternalHeader)
            };
            new_root_header.is_empty = false;
            new_root_header.first_child = self.root_ph.get_page_num();
        }
        Ok(())
    }

    fn insert_into_nonfull_node(&mut self, node: PageHandle, key_val: *mut u8, rid: &RID) -> Result<(), IndexingError> {
        let node_header = unsafe {
            &mut *(node.get_data() as *mut EntryHeader)
        };
        let entries = self.get_node_entries(node.get_data());
        let keys = unsafe {
            node.get_data().offset(self.header.keys_offset as isize)
        };

        if node_header.is_leaf {
            let (prev_index, is_dup) = match self.find_node_insert_index(key_val, node.get_data()) {
                Err(e) => {
                    dbg!(&e);
                    return Err(IndexingError::FindInsertIndexError);
                },
                Ok((a, b)) => (a, b)
            };

            if !is_dup {
                //copy key_val to keys
                let index = node_header.free_slot as usize;
                unsafe {
                    std::ptr::copy(key_val, keys.offset((index * self.header.attr_length) as isize), self.header.attr_length);
                }
                node_header.is_empty = false;
                node_header.num_keys += 1;
                node_header.free_slot = entries[index].next_slot;

                entries[index].et_type = EntryType::New;
                entries[index].page_num = rid.get_page_num();
                entries[index].slot_num = rid.get_slot_num();

                if prev_index == BEGINNING_OF_SLOT {
                    entries[index].next_slot = NO_MORE_SLOTS;
                    node_header.first_slot = index as u32;
                } else {
                    entries[index].next_slot = entries[prev_index].next_slot;
                    entries[prev_index].next_slot = index as u32;
                }

            } else {
                let prev_entry = &mut entries[prev_index];
                match prev_entry.et_type {
                    EntryType::Unoccupied => {
                        dbg!(&prev_entry);
                        return Err(IndexingError::AbnormalEntryType);
                    },
                    EntryType::New => {
                        let bucket_ph = match self.create_new_bucket() {
                            Err(e) => {
                                return Err(e);
                            },
                            Ok(v) => v
                        };
                        match self.insert_into_bucket(&bucket_ph, rid) {
                            Err(e) => {
                                return Err(e);
                            },
                            Ok(_) => {}
                        }
                        match self.insert_into_bucket(&bucket_ph, RID::new(prev_entry.page_num, prev_entry.slot_num)) {
                            Err(e) => {
                                return Err(e);
                            },
                            Ok(_) => {}
                        }
                        prev_entry.et_type = EntryType::Duplicate;
                        prev_entry.page_num = bucket_ph.get_page_num();
                    }
                }
            }
        }
    }
    
    fn create_new_node(&mut self, is_leaf: bool) -> Result<PageHandle, IndexingError> {
        let new_ph = match self.pfh.allocate_page() {
            Err(e) => {
                dbg!(&e);
                return Err(IndexingError::AllocatePageError);
            },
            Ok(v) => v
        };
        let new_eh = unsafe {
            &mut *(new_ph.get_data() as *mut EntryHeader)
        };
        new_eh.is_empty = true;
        new_eh.is_leaf = is_leaf;
        new_eh.num_keys = 0;
        new_eh.free_slot = 0;
        new_eh.first_slot = NO_MORE_SLOTS;
        new_eh.num1 = 0;
        new_eh.num2 = 0;
        
        let entries = self.get_node_entries(new_ph.get_data());

        for i in 0..self.header.max_keys {
            entries[i].et_type = EntryType::Unoccupied;
            entries[i].page_num = 0;//0 is an invalid page num
            if i == self.header.max_keys - 1 {
                entries[i].next_slot = NO_MORE_SLOTS;
            } else {
                entries[i].next_slot = (i+1) as u32;
            }
        }
        
        match self.pfh.unpin_dirty_page(new_ph.get_page_num()) {
            Err(e) => {
                dbg!(&e);
                Err(IndexingError::UnpinPageError)
            },
            Ok(_) => Ok(new_ph)
        }
    }

    fn create_new_bucket(&mut self) -> Result<PageHandle, IndexingError> {
        let new_ph = match self.pfh.allocate_page() {
            Err(e) => {
                dbg!(&e);
                return Err(IndexingError::AllocatePageError);
            },
            Ok(v) => v
        };
        let new_bh = unsafe {
            &mut *(new_ph.get_data() as *mut BucketHeader)
        };
        
        new_bh.num_keys = 0;
        new_bh.free_slot = 0;
        new_bh.first_slot = NO_MORE_SLOTS;
        new_bh.next_bucket = 0;

        let entries = self.get_bucket_entries(new_ph.get_data());

        for i in 0..(self.header.max_bucket_entries) {
            entries[i].page_num = 0;
            if i == self.header.max_bucket_entries - 1 {
                entries[i].next_slot = NO_MORE_SLOTS;
            } else {
                entries[i].next_slot = (i+1) as u32;
            }
        }

        match self.pfh.unpin_dirty_page(new_ph.get_page_num()) {
            Err(e) => {
                dbg!(e);
                Err(IndexingError::UnpinPageError)
            },
            Ok(_) => Ok(new_ph)
        }
    }

    /*
     * Find an appropriate insert index for an entry with a key whose value is val.
     * If success, return a tuple, usize represents the index, bool represents if 
     * the index entry is a duplicate one.
     *
     * Keys and entries are both arrays, and associated elements are at same index.
     */
    fn find_node_insert_index(&mut self, val: *mut u8, node_data: *mut u8) -> Result<(usize, bool), IndexingError> {
        let node_entries = self.get_node_entries(node_data);
        let keys = unsafe {
            node_data.offset(self.header.keys_offset as isize)
        };
        let entry_header = unsafe {
            &mut *(node_data as *mut EntryHeader)
        };
        
        let mut prev_index = BEGINNING_OF_SLOT as usize;
        let mut curr_index = entry_header.first_slot as usize;
        let mut is_dup = false;

        let mut ptr: *mut u8;

        while curr_index != NO_MORE_SLOTS as usize {
            ptr = unsafe {
                keys.offset((self.header.attr_length * curr_index) as isize)
            };
            match Self::compare(val, ptr, self.header.attr_type, self.header.attr_length) {
                Ordering::Greater => {},
                Ordering::Less => {
                    break;
                },
                Ordering::Equal => {
                    is_dup = true;
                }
            }
            prev_index = curr_index;
            curr_index = node_entries[curr_index].next_slot as usize;
        }
        Ok((prev_index, is_dup))
    }

    fn compare(val1: *mut u8, val2: *mut u8, attr_type: AttrType, len: usize) -> Ordering {
        match attr_type {
            AttrType::INT => {
                let v1 = unsafe {
                    & *(val1 as *mut i32)
                };
                let v2 = unsafe {
                    & *(val2 as *mut i32)
                };
                v1.cmp(v2)
            },
            AttrType::FLOAT => {
                let v1 = unsafe {
                    *(val1 as *mut f32)
                };
                let v2 = unsafe {
                    *(val2 as *mut f32)
                };
                if v1 < v2 {
                    Ordering::Less
                } else if v1 == v2 {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            },
            AttrType::STRING => {
                let v1 = unsafe {
                    std::mem::ManuallyDrop::new(String::from_raw_parts(val1, len, len))
                };
                let v2 = unsafe {
                    std::mem::ManuallyDrop::new(String::from_raw_parts(val2, len, len))
                };
                v1.cmp(&v2)
            }
        }
    }

    fn get_node_entries(&self, data: *mut u8) -> &'static mut [NodeEntry] {
        utils::get_arr_mut::<NodeEntry>(data, self.header.node_entries_offset, self.header.max_node_entries)
    }

    fn get_bucket_entries(&self, data: *mut u8) -> &'static mut [BucketEntry] {
        utils::get_arr_mut::<BucketEntry>(data, self.header.bucket_entries_offset, self.header.max_bucket_entries)
    }
}
