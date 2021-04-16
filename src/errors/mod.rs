/**********************************************
  > File Name		: errors.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Tue 02 Mar 2021 11:05:17 AM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

/*
 * Define some erros enum for global usage.
 * Some errors are passed between modules, so they are normally returned by the 
 * public interfaces of modules. And the true error is get debuged by dbg!.
 * And the enums inside Error are errors passed inside a module. Like the 
 * PageFileError enum are only used in the page_file module.
 *
 * And all constructions and associated functions don't return errors,
 * just panic.
 */
#[derive(Debug)]
pub enum Error {
    //public
    IncompleteWrite,
    FileOpenError,


    //page_file module
    UnpinPageError,
    MarkDirtyError,
    AllocatePageError,
    CreatePageFileError,
    GetPageError,
    PageDisposed,

    //record_management module
    SetBitmapError,
    FindFreeSlotError,

    //indexing module
    CreateNewNodeError,
}

#[derive(Debug)]
pub enum PageFileError {
    Okay,
    NoMemory,
    NoBuffer,//no buffer space
    NoPage, //no page in the buffer, returns when intended to free a used page.
    AllPagesPinned, //all pages pinned, that means we have to resize the buffer.
    NoFilePointer,
    DestShort, //the dest is short for space.
    IncompleteRead, //Incomplete from file.
    IncompleteWrite,
    ReadAtError, //error of read_at method
    WriteAtError, 
    LostFilePointer, //returns when we need to use the file pointer of a page, but find it without any.
    DataUnintialized, //returns when data field of BufferPage is null.
    OutOfIndex,
    //Internal errors.
    PageInBuf, //the new page is already in buf.
    PageNotInBuf, //the page is not in buffer when we expect it is in the buffer.
    PageUnpinned, //returns when we expect the page to be pinned, but find opposite.
    PagePinned, //opposite to the PageUnpinned.
    PageFreed, //returns when free_page function tries to free a page but find it already freed.
    LocationError, //returns when we calculate a location but it's too ridiculous.
    HashNotFound, //returns when we insert a new page but can't find it in the hashtable.
    HashPageExist, //the new page is already in hashtable.
    InvalidName, //invalid file name
    Unix, //error in Unix system call or library routine.

}

#[derive(Debug)]
pub enum RecordError {
    BitSet,//returns when the bit is set but expects not set.
    BitUnset,//returns when the bit is unset but expects set.


    NoFilePointer,
    InvalidPageNumber,
    InvalidSlotNumber,
    OffsetError,
    NullPointerError,
    MismatchRecordOffset,//returns when offset is not integer multiple of page size.
    FullPage,//returns when there is no free slot in the page, not an error actually. Just tell the caller that the page is full.
    RecordDeleted,//returns when the record is already deleted. Usually detected when the bitmap is unset.
    IncompleteWrite,
    FileExist,
    FileOpenError,//may because file does not exist.
}

#[derive(Debug)]
pub enum IndexingError {
    CreateFileError,
    InvalidAttr,
    IncompleteWrite,
    IncompleteRead,
    FileExist,
    AllocatePageError,
    CreateNewNodeError,
    UnpinPageError,
}
