cfg_core! {
    use crate::EncryptionConfig;
}

use super::DbType;
use crate::{Database, Result};

/// A builder for [`Database`]. This struct can be used to build
/// all variants of [`Database`]. These variants include:
///
/// - `new_local`/`Local` which means a `Database` that is just a local libsql database
///   it does no networking and does not connect to any remote database.
/// - `new_remote_replica`/`RemoteReplica` creates an embedded replica database that will be able
///   to sync from the remote url and delegate writes to the remote primary.
/// - `new_synced_database`/`SyncedDatabase` creates a database that can be written offline and
///   synced to a remote server.
/// - `new_local_replica`/`LocalReplica` creates an embedded replica similar to the remote version
///   except you must use `Database::sync_frames` to sync with the remote. This version also
///   includes the ability to delegate writes to a remote primary.
/// - new_remote`/`Remote` creates a database that does not create anything locally but will
///   instead run all queries on the remote database. This is essentially the pure HTTP api.
///
/// # Note
///
/// Embedded replicas require a clean database (no database file) or a previously synced database or else it will
/// throw an error to prevent any misuse. To work around this error a user can delete the database
/// and let it resync and create the wal_index metadata file.
pub struct Builder<T = ()> {
    inner: T,
}

impl Builder<()> {
    cfg_core! {
        /// Create a new local database.
        pub fn new_local(path: impl AsRef<std::path::Path>) -> Builder<Local> {
            Builder {
                inner: Local {
                    path: path.as_ref().to_path_buf(),
                    flags: crate::OpenFlags::default(),
                    encryption_config: None,
                    skip_safety_assert: false,
                },
            }
        }
    }
}

cfg_core! {
    /// Local database configuration type in [`Builder`].
    pub struct Local {
        path: std::path::PathBuf,
        flags: crate::OpenFlags,
        encryption_config: Option<EncryptionConfig>,
        skip_safety_assert: bool,
    }

    impl Builder<Local> {
        /// Set [`OpenFlags`] for this database.
        pub fn flags(mut self, flags: crate::OpenFlags) -> Builder<Local> {
            self.inner.flags = flags;
            self
        }

        /// Set an encryption config that will encrypt the local database.
        pub fn encryption_config(
            mut self,
            encryption_config: EncryptionConfig,
        ) -> Builder<Local> {
            self.inner.encryption_config = Some(encryption_config);
            self
        }

        /// Skip the saftey assert used to ensure that sqlite3 is configured correctly for the way
        /// that libsql uses the ffi code. By default, libsql will try to use the SERIALIZED
        /// threadsafe mode for sqlite3. This allows us to implement Send/Sync for all the types to
        /// allow them to move between threads safely. Due to the fact that sqlite3 has a global
        /// config this may conflict with other sqlite3 connections in the same process.
        ///
        /// Using this setting is very UNSAFE and you are expected to use the libsql in adherence
        /// with the sqlite3 threadsafe rules or else you WILL create undefined behavior. Use at
        /// your own risk.
        pub unsafe fn skip_safety_assert(mut self, skip: bool) -> Builder<Local> {
            self.inner.skip_safety_assert = skip;
            self
        }

        /// Build the local database.
        pub async fn build(self) -> Result<Database> {
            let db = if self.inner.path == std::path::Path::new(":memory:") {
                let db = if !self.inner.skip_safety_assert {
                    crate::local::Database::open(":memory:", crate::OpenFlags::default())?
                } else {
                    unsafe { crate::local::Database::open_raw(":memory:", crate::OpenFlags::default())? }
                };

                Database {
                    db_type: DbType::Memory { db } ,
                    max_write_replication_index: Default::default(),
                }
            } else {
                let path = self
                    .inner
                    .path
                    .to_str()
                    .ok_or(crate::Error::InvalidUTF8Path)?
                    .to_owned();

                Database {
                    db_type: DbType::File {
                        path,
                        flags: self.inner.flags,
                        encryption_config: self.inner.encryption_config,
                        skip_safety_assert: self.inner.skip_safety_assert
                    },
                    max_write_replication_index: Default::default(),
                }
            };

            Ok(db)
        }
    }
}
