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
const NO_MORE_PAGES: u32 = 0;

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
    free_slot: usize,
    first_slot: usize,//the pointer to the first node of the linked list.

    num1: u32, //invalid unless the entry is determined as a leaf node or an internal node.
    num2: u32,
}

#[derive(Debug, Copy, Clone)]
pub struct LeafHeader {
    is_leaf: bool,
    is_empty: bool,

    num_keys: usize,
    free_slot: usize,
    first_slot: usize,//the pointer to the first node of the linked list.

    prev_page: u32,
    next_page: u32
}

#[derive(Debug, Copy, Clone)]
pub struct InternalHeader {
    is_leaf: bool,
    is_empty: bool,

    num_keys: usize,
    free_slot: usize,
    first_slot: usize,//the pointer to the first node of the linked list.

    first_child: u32,//page num of the first child node.
    num2: u32
}

#[derive(Debug, Copy, Clone)]
struct BucketHeader {
    num_keys: usize,
    free_slot: usize,
    first_slot: usize,
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
    next_slot: usize,//points to the next entry in the node.
    page_num: u32,//page_num and slot_num associated with the record.
    slot_num: usize,//if this is a duplicate entry, the page_num is set to be the bucket page num.
}

#[derive(Debug, Copy, Clone)]
struct BucketEntry {
    next_slot: usize,
    page_num: u32,
    slot_num: usize,
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
        let node_header = utils::get_header_mut::<EntryHeader>(node.get_data());
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
                let index = node_header.free_slot;
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
                        //insert_into_bucket is in charge of unpinning the page
                        //no matter if it's dirty or not.
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
                    },
                    EntryType::Duplicate => {
                        let bucket_ph = match self.pfh.get_page(prev_entry.page_num) {
                            Err(e) => {
                                dbg!(&e);
                                return Err(IndexingError::GetPageError);
                            },
                            Ok(v) => v
                        };
                        if let Err(e) = match self.insert_into_bucket(&bucket_ph, rid) {
                            return Err(e);
                        }
                    }
                }
            }
        } else {//if it's an internal node.\
            let mut next_node: u32;//next level node to call this method.
            let (prev_index, is_dup) = match self.find_node_insert_index(key_val, node.get_data()) {
                Err(e) => {
                    return Err(e);
                },
                Ok(v) => v
            };
            let node_header = utils::get_header_mut::<InternalHeader>(node.get_data());
            if prev_index == BEGINNING_OF_SLOT {
                //connect to the first child node.
                next_node = node_header.first_child;
            } else {
                next_node = entries[prev_index].page_num;//page number of internal node entry stores the page number of the node it points to.
            }

            let next_node_ph = match self.pfh.get_page(next_node) {
                Err(e) => {
                    dbg!(&e);
                    return Err(IndexingError::GetPageError);
                },
                Ok(v) => v
            };
            let next_node_header = utils::get_header::<EntryHeader>(next_node_ph.get_data());
            
            if next_node_header.num_keys == self.header.max_keys {
                //if the next node is full, we need to split the next node.
                
            }
        }
    }

    /*
     * Insert a rid into a bucket, entries related to a same index value have
     * no relations.
     */
    fn insert_into_bucket(&mut self, mut ph: &PageHandle, rid: RID) -> Result<(), IndexingError> {
        let mut flag = true;
        while flag {
            /*
             * TODO
             * In original code, here's the part that traverses all buckets just to 
             * make sure no entry with a same rid is already inserted.
             * I think it's a little unnessary, so I just leave it aside for now.
             */
            let mut bucket_entries = self.get_bucket_entries(ph.get_data());
            let mut bucket_header = utils::get_header_mut::<BucketHeader>(ph.get_data());
            if bucket_header.next_bucket == NO_MORE_PAGES && bucket_header.num_keys == self.header.max_bucket_entries {
                flag = false;
                let new_ph = match self.create_new_bucket() {
                    Err(e) => {
                        return Err(e);
                    },
                    Ok(v) => v
                };
                bucket_header.next_bucket = new_ph.get_page_num();
                if let Err(e) = match self.pfh.unpin_dirty_page(ph.get_page_num()) {
                    return Err(e);
                }

                bucket_entries = self.get_bucket_entries(new_ph.get_data());
                bucket_header = utils::get_header_mut::<BucketHeader>(new_ph.get_data());
                ph = new_ph;
            }

            if bucket_header.next_bucket == NO_MORE_PAGES {
                let loc = bucket_header.free_slot;
                bucket_entries[loc].page_num = rid.get_page_num();
                bucket_entries[loc].slot_num = rid.get_slot_num();
                bucket_header.free_slot = bucket_entries[loc].next_slot;
                bucket_entries[loc].next_slot = bucket_header.first_slot;
                bucket_header.first_slot = loc;
                bucket_header.num_keys += 1;
                if let Err(e) = self.pfh.unpin_dirty_page(ph.get_page_num()) {
                    return Err(e);
                }
            }

            ph = match self.pfh.get_page(bucket_header.next_bucket) {
                Err(e) => {
                    return Err(e);
                },
                Ok(v) => v
            };
        }
        Ok(())
    }

    /*
     * split a node, what we need:
     *   1. parent_ph: parent node PageHandle
     *   2. full_ph: PageHandle of the full node to split.
     *   3. is_leaf: is the full node a leaf node.
     *   4. parent_prev_index: previous index of new key, acquired by the method 
     *      find_node_insert_index.
     * returns:
     *   1. page number of the new node.
     *   2. the index at which a new key is inserted.
     */
    fn split_node(&mut self, parent_ph: PageHandle, full_ph: PageHandle, is_leaf: bool, parent_prev_index: usize) -> Result<(usize), IndexingError> {
        let parent_header = utils::get_header_mut::<InternalHeader>();
        let parent_entries = self.get_node_entries(parent_ph.get_data());
        
        let new_ph = match self.create_new_node(&is_leaf) {
            Err(e) => {
                return  Err(e);
            },
            Ok(v) => v
        };

        let full_header = utils::get_header_mut::<EntryHeader>(full_ph.get_data());
        let new_header = utils::get_header_mut::<EntryHeader>(new_ph.get_data());
        
        let new_entries = self.get_node_entries(new_ph.get_data());
        let full_entries = self.get_node_entries(full_ph.get_data());
        let new_keys = unsafe {
            new_ph.get_data().offset(self.header.keys_offset as isize)
        };
        let full_keys = unsafe {
            full_ph.get_data().offset(self.header.keys_offset as isize)
        };
        let parent_keys = unsafe {
            parent_ph.get_data().offset(self.header.keys_offset as isize)
        };

        /*
         * move self.header.max_keys/2 number of entries and keys to the new node.
         */
        let mut prev_index: usize = BEGINNING_OF_SLOT as usize;
        let mut curr_index: usize = full_header.first_slot as usize;
        for i in 0..(self.header.max_keys/2) {
            prev_index = curr_index;
            curr_index = full_entries[curr_index].next_slot as usize;
        }
        full_entries[prev_index].next_slot = NO_MORE_SLOTS;

        //find the key to insert into the parent node.
        let parent_key = unsafe {
            full_keys.offset((curr_index * self.header.attr_length) as isize)
        };

        /*
         * now, the node that curr_index points to is an edge node.
         * we need to insert it into the new node and the parent node, 
         * then remove it from the full node or I could say old node.
         * 
         * Above actions is only taken when it's an internal node.
         */
        if !is_leaf {
            let new_header = utils::get_header_mut::<InternalHeader>(new_ph.get_data());
            new_header.first_child = full_entries[curr_index].page_num;
            new_header.is_empty = false;
            //unlink curr_index from the old node
            prev_index = curr_index;
            curr_index = full_entries[prev_index].next_slot;
            full_entries[prev_index].next_slot = full_header.free_slot;
            full_header.free_slot = prev_index as u32;
            full_header.num_keys -= 1;
        }

        //now we remove all the remaining entries to the new node.
        //prev_index2 and curr_index2 gonna be used in the new node.
        let mut prev_index2 = BEGINNING_OF_SLOT as usize;
        let mut curr_index2 = new_header.free_slot as usize;
        while curr_index != NO_MORE_SLOTS {
            new_entries[curr_index2] = full_entries[curr_index];//NodeEntry implemented Copy trait.
            unsafe {
                std::ptr::copy(full_keys.offset((curr_index * self.header.attr_length) as isize), new_keys.offset((curr_index2 * self.header.attr_length)), self.header.attr_length);
            }

            if prev_index2 == BEGINNING_OF_SLOT {//as for the first slot.
                new_header.free_slot = new_entries[curr_index2].next_slot;
                new_entries[curr_index2].next_slot = new_header.first_slot;
                new_header.first_slot = curr_index2;
            } else {
                new_header.free_slot = new_entries[curr_index2].next_slot;
                new_entries[curr_index2].next_slot = new_entries[prev_index2].next_slot;
                new_entries[prev_index2].next_slot = curr_index2;
            }

            prev_index2 = curr_index2;
            curr_index2 = new_entries[curr_index2].next_slot;

            prev_index = curr_index;
            curr_index = full_entries[curr_index].next_slot;
            full_entries[prev_index].next_slot = full_header.free_slot;
            full_header.free_slot = prev_index;

            full_header.num_keys -= 1;
            new_header.num_keys += 1;
        }
        
        //insert the parent_key into the parent node at the index specified in parameters.
        let loc = parent_header.free_slot as usize;
        let slot = parent_header.free_slot;
        unsafe {
            std::ptr::copy(parent_key, parent_keys.offset((loc * self.header.attr_length) as isize), self.header.attr_length);
        }
        if parent_prev_index == BEGINNING_OF_SLOT as usize {
            parent_header.free_slot = parent_entries[loc].next_slot;
            parent_entries[loc].next_slot = parent_header.first_slot;
            parent_header.first_slot = slot;
        } else {
            parent_header.free_slot = parent_entries[loc].next_slot;
            parent_entries[loc].next_slot = parent_entries[parent_prev_index].next_slot;
            parent_entries[parent_prev_index].next_slot = slot;
        }
        parent_header.num_keys += 1;

        /*
         * As all leaf nodes are linked together, so we need to link the new node if it's
         * a leaf node.
         */
        if is_leaf {
            let new_header = utils::get_header_mut::<LeafHeader>(new_ph.get_data());
            let full_header = utils::get_header_mut::<LeafHeader>(full_ph.get_data());
            let next_page = full_header.next_page;
            
            new_header.prev_page = full_ph.get_page_num();
            new_header.next_page = full_header.next_page;
            full_header.next_page = new_ph.get_page_num();
            if next_page != NO_MORE_PAGES {
                let full_next_ph = match self.pfh.get_page(next_page) {
                    Err(e) => {
                        dbg!(&e);
                        return Err(IndexingError::GetPageError);
                    },
                    Ok(v) => v
                };
                let full_next_header = utils::get_header_mut::<LeafHeader>(full_next_ph.get_data());
                full_next_header.prev_page = new_ph.get_page_num();
                if let Err(e) = self.pfh.unpin_dirty_page(next_page) {
                    dbg!(&e);
                    return Err(IndexingError::UnpinPageError);
                }
            }
        }

        if let Err(e) = self.pfh.unpin_dirty_page(new_ph.get_page_num()) {
            dbg!(&e);
            return Err(IndexingError::UnpinPageError);
        }
        Ok(loc)
    }
    
    fn create_new_node(&mut self, is_leaf: &bool) -> Result<PageHandle, IndexingError> {
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

    /*
     * Every time a duplicate entry appears, a new page is allocated.
     * And all rids associated with these duplicate entries are stored in this page.
     * If one page is full, allocate another one.
     */
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
