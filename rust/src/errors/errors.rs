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
        NoMemory,
        NoBuffer,//no buffer space
        IncompleteRead, //Incomplete from file.
        IncompleteWrite,
        //Internal errors.
        PageInBuf, //the new page is already in buf.
        HashNotFound, //occurs when we insert a new page but not found in the hashtable.
        HashPageExist, //the new page is already in hashtable.
        InvalidName, //invalid file name
        Unix, //error in Unix system call or library routine.
    }

}
