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
 */

mod Errors {

    enum PageFileError {
        Okay,
        NoMemory,
        NoBuffer,//no buffer space
        NoPage, //no page in the buffer, returns when intended to free a used page.
        AllPagesPinned, //all pages pinned, that means we have to resize the buffer.
        NoFilePointer,
        DestShort, //the dest is short for space.
        IncompleteRead, //Incomplete from file.
        IncompleteWrite,
        //Internal errors.
        PageInBuf, //the new page is already in buf.
        LocationError, //returns when we calculate a location but it's too ridiculous.
        HashNotFound, //returns when we insert a new page but can't find it in the hashtable.
        HashPageExist, //the new page is already in hashtable.
        InvalidName, //invalid file name
        Unix, //error in Unix system call or library routine.
    }

}
