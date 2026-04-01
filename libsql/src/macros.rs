#![allow(unused_macros)]

macro_rules! cfg_core {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "core")]
            #[cfg_attr(docsrs, doc(cfg(feature = "core")))]
            $item
        )*
    }
}
