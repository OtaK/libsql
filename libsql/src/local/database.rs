use std::sync::Once;

use crate::{database::OpenFlags, local::connection::Connection, Error::ConnectionFailed, Result};
use libsql_sys::ffi;

// A libSQL database.
pub struct Database {
    pub db_path: String,
    pub flags: OpenFlags,
}

impl Database {
    /// Open a local database file.
    pub fn open<S: Into<String>>(db_path: S, flags: OpenFlags) -> Result<Database> {
        let db_path = db_path.into();

        if db_path.starts_with("libsql:")
            || db_path.starts_with("http:")
            || db_path.starts_with("https:")
        {
            Err(ConnectionFailed(format!(
                "Unable to open local database {db_path} with Database::open()"
            )))
        } else {
            Ok(Database::new(db_path, flags))
        }
    }

    /// Safety: this is like `open` but does not enfoce that sqlite_config has THREADSAFE set to
    /// `SQLITE_CONFIG_SERIALIZED`, calling
    pub unsafe fn open_raw<S: Into<String>>(db_path: S, flags: OpenFlags) -> Result<Database> {
        let db_path = db_path.into();

        if db_path.starts_with("libsql:")
            || db_path.starts_with("http:")
            || db_path.starts_with("https:")
        {
            Err(ConnectionFailed(format!(
                "Unable to open local database {db_path} with Database::open()"
            )))
        } else {
            Ok(Database { db_path, flags })
        }
    }

    pub fn new(db_path: String, flags: OpenFlags) -> Database {
        static LIBSQL_INIT: Once = Once::new();

        LIBSQL_INIT.call_once(|| {
            // Ensure that we are configured with the correct threading model
            // if this config is not set correctly the entire api is unsafe.
            unsafe {
                assert_eq!(
                    ffi::sqlite3_config(ffi::SQLITE_CONFIG_SERIALIZED),
                    ffi::SQLITE_OK,
                    "libsql was configured with an incorrect threading configuration and
                    the api is not safe to use. Please check that no multi-thread options have
                    been set. If nothing was configured then please open an issue at:
                    https://github.com/libsql/libsql"
                );

                assert_eq!(
                    ffi::sqlite3_initialize(),
                    ffi::SQLITE_OK,
                    "libsql failed to initialize"
                );
            }
        });

        Database { db_path, flags }
    }

    pub fn connect(&self) -> Result<Connection> {
        Connection::connect(self)
    }

    pub(crate) fn path(&self) -> &str {
        &self.db_path
    }
}
