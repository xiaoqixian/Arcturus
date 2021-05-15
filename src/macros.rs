/**********************************************
  > File Name		: macros.rs
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Fri 14 May 2021 10:34:16 AM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

#[macro_export]
macro_rules! ok_or_return {
    ($func: expr, $Error: ident::$err: ident) => {{
        match $func {
            Ok(v) => v,
            Err(e) => {
                dbg!(e);
                return Err($Error::$err);
            }
        }
    }};
}

#[macro_export]
macro_rules! error_return {
    ($func: expr, $Error: ident::$err: ident) => {
        if let Err(e) = $func {
            dbg!(e);
            return Err($Error::$err);
        }
    }
}
