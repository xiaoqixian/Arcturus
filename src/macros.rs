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
    //when calls on methods
    ($self: ident, $method: ident($($args: tt)*), $Error: ident::$err: ident) => {{
        match $self.$method($($args)*) {
            Ok(v) => v,
            Err(e) => {
                dbg!(e);
                return Err($Error::$err);
            }
        }
    }};
    ($self: ident, $method: ident($($args: tt)*)) => {{
        match $self.$method($($args)*) {
            Ok(v) => v,
            Err(e) => {
                return Err(e);
            }
        }
    }}
}

#[macro_export]
macro_rules! error_return {
    ($self: ident, $method: ident($($args: tt)*), $Error: ident::$err: ident) => {{
        if let Err(e) = $self.$method($($args)*) {
            dbg!(e);
            return Err($Error::$err);
        }
    }};
    ($self: ident, $method: ident($($args: tt)*)) => {{
        if let Err(e) = $self.$method($($args)*) {
            return Err(e);
        }
    }}
}
