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

 struct Node 
