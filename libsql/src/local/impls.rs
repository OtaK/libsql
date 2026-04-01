use std::time::Duration;
use std::{fmt, path::Path};

use blocking::unblock;

use crate::{
    connection::{AuthHook, BatchRows, UpdateHook},
    params::Params,
    rows::{ColumnsInner, RowInner, RowsInner},
    statement::Stmt,
    transaction::Tx,
    Column, Connection, Result, Row, Rows, Statement, Transaction, TransactionBehavior, Value,
    ValueType,
};

#[derive(Clone)]
pub(crate) struct LibsqlConnection {
    pub(crate) conn: super::Connection,
}

impl LibsqlConnection {
    pub async fn execute<S: Into<String>>(&self, sql: S, params: Params) -> Result<u64> {
        let this = self.clone();
        let sql = sql.into();
        unblock(move || this.conn.execute(sql, params)).await
    }

    pub async fn execute_batch<S: Into<String>>(&self, sql: S) -> Result<BatchRows> {
        let this = self.clone();
        let sql = sql.into();
        unblock(move || this.conn.execute_batch(sql)).await
    }

    pub async fn execute_transactional_batch<S: Into<String>>(&self, sql: S) -> Result<BatchRows> {
        let this = self.clone();
        let sql = sql.into();
        unblock(move || this.conn.execute_transactional_batch(sql)).await?;
        Ok(BatchRows::empty())
    }

    pub async fn prepare<S: Into<String>>(&self, sql: S) -> Result<Statement> {
        let this = self.clone();
        let sql = sql.into();
        let stmt = unblock(move || this.conn.prepare(sql)).await?;
        Ok(Statement {
            inner: Box::new(LibsqlStmt(stmt)),
        })
    }

    pub async fn transaction(&self, tx_behavior: TransactionBehavior) -> Result<Transaction> {
        let conn = self.conn.clone();
        let tx = unblock(move || crate::local::Transaction::begin(conn, tx_behavior)).await?;
        // TODO(lucio): Can we just use the conn passed to the transaction?
        Ok(Transaction {
            inner: Box::new(LibsqlTx(Some(tx))),
            conn: Connection { conn: self.clone() },
            close: None,
        })
    }

    pub fn interrupt(&self) -> Result<()> {
        self.conn.interrupt()
    }

    pub fn busy_timeout(&self, timeout: Duration) -> Result<()> {
        self.conn.busy_timeout(timeout)
    }

    pub fn is_autocommit(&self) -> bool {
        self.conn.is_autocommit()
    }

    pub fn changes(&self) -> u64 {
        self.conn.changes()
    }

    pub fn total_changes(&self) -> u64 {
        self.conn.total_changes()
    }

    pub fn last_insert_rowid(&self) -> i64 {
        self.conn.last_insert_rowid()
    }

    pub fn set_reserved_bytes(&self, reserved_bytes: i32) -> Result<()> {
        self.conn.set_reserved_bytes(reserved_bytes)
    }

    pub fn get_reserved_bytes(&self) -> Result<i32> {
        self.conn.get_reserved_bytes()
    }

    pub fn enable_load_extension(&self, onoff: bool) -> Result<()> {
        self.conn.enable_load_extension(onoff)
    }

    pub fn load_extension(&self, dylib_path: &Path, entry_point: Option<&str>) -> Result<()> {
        self.conn.load_extension(dylib_path, entry_point)
    }

    pub fn authorizer(&self, hook: Option<AuthHook>) -> Result<()> {
        self.conn.authorizer(hook)
    }

    pub fn add_update_hook(&self, cb: Box<UpdateHook>) {
        self.conn.add_update_hook(cb)
    }
}

impl Drop for LibsqlConnection {
    fn drop(&mut self) {
        self.conn.disconnect()
    }
}

pub(crate) struct LibsqlStmt(pub crate::local::Statement);

#[async_trait::async_trait]
impl Stmt for LibsqlStmt {
    fn finalize(&mut self) {
        self.0.finalize();
    }

    async fn execute(&self, params: &Params) -> Result<usize> {
        let params = params.clone();
        let stmt = self.0.clone();

        unblock(move || stmt.execute(&params))
            .await
            .map(|i| i as usize)
    }

    async fn query(&self, params: &Params) -> Rows {
        let params = params.clone();
        let stmt = self.0.clone();

        let rows = unblock(move || stmt.query(&params)).await;
        Rows::new(LibsqlRows(rows))
    }

    async fn run(&self, params: &Params) -> Result<()> {
        let params = params.clone();
        let stmt = self.0.clone();

        unblock(move || stmt.run(&params)).await
    }

    fn reset(&self) {
        self.0.reset();
    }

    fn parameter_count(&self) -> usize {
        self.0.parameter_count()
    }

    fn parameter_name(&self, idx: i32) -> Option<&str> {
        self.0.parameter_name(idx)
    }

    fn column_count(&self) -> usize {
        self.0.column_count()
    }

    fn columns(&self) -> Vec<Column<'_>> {
        self.0.columns()
    }
}

pub(super) struct LibsqlTx(pub(super) Option<crate::local::Transaction>);

#[async_trait::async_trait]
impl Tx for LibsqlTx {
    async fn commit(&mut self) -> Result<()> {
        let tx = self.0.take().expect("Tx already dropped");
        unblock(|| tx.commit()).await
    }

    async fn rollback(&mut self) -> Result<()> {
        let tx = self.0.take().expect("Tx already dropped");
        unblock(|| tx.rollback()).await
    }
}

pub(crate) struct LibsqlRows(pub(crate) crate::local::Rows);

#[async_trait::async_trait]
impl RowsInner for LibsqlRows {
    async fn next(&mut self) -> Result<Option<Row>> {
        let rows = self.0.clone();
        self.0 = crate::local::Rows::new(rows.stmt.clone());
        let row = unblock(move || rows.next()).await?.map(|r| Row {
            inner: Box::new(LibsqlRow(r)),
        });

        Ok(row)
    }
}

impl ColumnsInner for LibsqlRows {
    fn column_count(&self) -> i32 {
        self.0.column_count()
    }

    fn column_name(&self, idx: i32) -> Option<&str> {
        self.0.column_name(idx)
    }

    fn column_type(&self, idx: i32) -> Result<ValueType> {
        self.0.column_type(idx).map(ValueType::from)
    }
}

struct LibsqlRow(crate::local::Row);

impl RowInner for LibsqlRow {
    fn column_value(&self, idx: i32) -> Result<Value> {
        self.0.get_value(idx)
    }

    fn column_str(&self, idx: i32) -> Result<&str> {
        self.0.get::<&str>(idx)
    }
}

impl ColumnsInner for LibsqlRow {
    fn column_name(&self, idx: i32) -> Option<&str> {
        self.0.column_name(idx)
    }

    fn column_type(&self, idx: i32) -> Result<ValueType> {
        self.0.column_type(idx).map(ValueType::from)
    }

    fn column_count(&self) -> i32 {
        self.0.stmt.column_count() as i32
    }
}

impl fmt::Debug for LibsqlRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}
