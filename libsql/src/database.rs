#![allow(deprecated)]

mod builder;

pub use builder::Builder;

#[cfg(feature = "core")]
pub use libsql_sys::{Cipher, EncryptionConfig};

use crate::{Connection, Result};
use std::fmt;
use std::sync::atomic::AtomicU64;

cfg_core! {
    bitflags::bitflags! {
        /// Flags that can be passed to libsql to open a database in specific
        /// modes.
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
        #[repr(C)]
        pub struct OpenFlags: ::std::os::raw::c_int {
            const SQLITE_OPEN_READ_ONLY = libsql_sys::ffi::SQLITE_OPEN_READONLY;
            const SQLITE_OPEN_READ_WRITE = libsql_sys::ffi::SQLITE_OPEN_READWRITE;
            const SQLITE_OPEN_CREATE = libsql_sys::ffi::SQLITE_OPEN_CREATE;
        }
    }

    impl Default for OpenFlags {
        #[inline]
        fn default() -> OpenFlags {
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
        }
    }
}

enum DbType {
    #[cfg(feature = "core")]
    Memory { db: crate::local::Database },
    #[cfg(feature = "core")]
    File {
        path: String,
        flags: OpenFlags,
        encryption_config: Option<EncryptionConfig>,
        skip_safety_assert: bool,
    },
}

impl fmt::Debug for DbType {
    #[allow(unreachable_patterns)]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(feature = "core")]
            Self::Memory { .. } => write!(f, "Memory"),
            #[cfg(feature = "core")]
            Self::File { .. } => write!(f, "File"),

            _ => write!(f, "no database type set"),
        }
    }
}

/// A struct that knows how to build [`Connection`]'s, this type does
/// not do much work until the [`Database::connect`] fn is called.
pub struct Database {
    db_type: DbType,
    /// The maximum replication index returned from a write performed using any connection created using this Database object.
    #[allow(dead_code)]
    max_write_replication_index: std::sync::Arc<AtomicU64>,
}

cfg_core! {
    impl Database {
        /// Open an in-memory libsql database.
        #[deprecated = "Use the new `Builder` to construct `Database`"]
        pub fn open_in_memory() -> Result<Self> {
            let db = crate::local::Database::open(":memory:", OpenFlags::default())?;

            Ok(Database {
                db_type: DbType::Memory { db },
                max_write_replication_index: Default::default(),
            })
        }

        /// Open a file backed libsql database.
        #[deprecated = "Use the new `Builder` to construct `Database`"]
        pub fn open(db_path: impl Into<String>) -> Result<Database> {
            Database::open_with_flags(db_path, OpenFlags::default())
        }

        /// Open a file backed libsql database with flags.
        #[deprecated = "Use the new `Builder` to construct `Database`"]
        pub fn open_with_flags(db_path: impl Into<String>, flags: OpenFlags) -> Result<Database> {
            Ok(Database {
                db_type: DbType::File {
                    path: db_path.into(),
                    flags,
                    encryption_config: None,
                    skip_safety_assert: false,
                },
                max_write_replication_index: Default::default(),
            })
        }
    }
}

impl Database {
    /// Connect to the database this can mean a few things depending on how it was constructed:
    ///
    /// - When constructed with `open`/`open_with_flags`/`open_in_memory` this will call into the
    ///   libsql C ffi and create a connection to the libsql database.
    /// - When constructed with `open_remote` and friends it will not call any C ffi and will
    ///   lazily create a HTTP connection to the provided endpoint.
    /// - When constructed with `open_with_remote_sync_` and friends it will attempt to perform a
    ///   handshake with the remote server and will attempt to replicate the remote database
    ///   locally.
    #[allow(unreachable_patterns)]
    pub fn connect(&self) -> Result<Connection> {
        match &self.db_type {
            #[cfg(feature = "core")]
            DbType::Memory { db } => {
                use crate::local::impls::LibsqlConnection;

                let conn = db.connect()?;

                let conn = LibsqlConnection { conn };

                Ok(Connection { conn })
            }

            #[cfg(feature = "core")]
            DbType::File {
                path,
                flags,
                encryption_config,
                skip_safety_assert,
            } => {
                use crate::local::impls::LibsqlConnection;

                let db = if !skip_safety_assert {
                    crate::local::Database::open(path, *flags)?
                } else {
                    unsafe { crate::local::Database::open_raw(path, *flags)? }
                };

                let conn = db.connect()?;

                if !cfg!(feature = "encryption") && encryption_config.is_some() {
                    return Err(crate::Error::Misuse(
                        "Encryption is not enabled: enable the `encryption` feature in order to enable encryption-at-rest".to_string(),
                    ));
                }

                #[cfg(feature = "encryption")]
                if let Some(cfg) = encryption_config {
                    if unsafe {
                        libsql_sys::connection::set_encryption_cipher(conn.raw, cfg.cipher_id())
                    } == -1
                    {
                        return Err(crate::Error::Misuse(
                            "failed to set encryption cipher".to_string(),
                        ));
                    }
                    if unsafe {
                        libsql_sys::connection::set_encryption_key(conn.raw, &cfg.encryption_key)
                    } != crate::ffi::SQLITE_OK
                    {
                        return Err(crate::Error::Misuse(
                            "failed to set encryption key".to_string(),
                        ));
                    }
                }

                let conn = LibsqlConnection { conn };

                Ok(Connection { conn })
            }

            _ => unreachable!("no database type set"),
        }
    }
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Database").finish()
    }
}
