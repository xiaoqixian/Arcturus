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
 *     1. header: store data of the page
 *     2. entries: array of all entries metadata.
 *     3. attributes data
 *
 * Node Insertion:
 * 
 * Insert Into a Non Full Node:
 *  check if it's a leaf node or an internal node:
 *      1. If it's a leaf node, directly intert into it.
 */
