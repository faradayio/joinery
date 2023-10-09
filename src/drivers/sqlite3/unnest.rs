//! Simple SQLite3 extension which implements a dummy `UNNEST` function.
//!
//! References:
//! - [SQLite3 virtual tables](https://www.sqlite.org/vtab.html)
//! - [`rusqlite` virtual table
//!   example](https://github.com/rusqlite/rusqlite/blob/master/tests/vtab.rs)
//! - [`rusqlite` virtual table
//!   documentation](https://docs.rs/rusqlite/latest/rusqlite/vtab/index.html)
//!
//! This is taken almost verbatim from the `rusqlite` example linked above.
//!
//! This needs to use unsafe code because it's implementing a C callback API
//! provided by SQLite3. The `rusqlite` crate provides a safe wrapper around
//! many SQLite3 APIs, but the data structure layout needed for virtual tables
//! needs to match the C structs defined by SQLite3. When we write `unsafe
//! impl`, we're promising that we'd laid things out correctly.

use std::{ffi::c_int, marker::PhantomData};

use rusqlite::vtab::{
    eponymous_only_module, sqlite3_vtab, sqlite3_vtab_cursor, Context, IndexInfo, VTab,
    VTabConnection, VTabCursor, Values,
};

#[repr(C)]
struct UnnestTab {
    /// Base class. Must be first.
    base: sqlite3_vtab,
}

unsafe impl<'vtab> VTab<'vtab> for UnnestTab {
    type Aux = ();
    type Cursor = UnnestTabCursor<'vtab>;

    fn connect(
        _: &mut VTabConnection,
        _aux: Option<&()>,
        _args: &[&[u8]],
    ) -> rusqlite::Result<(String, UnnestTab)> {
        let vtab = UnnestTab {
            base: sqlite3_vtab::default(),
        };
        Ok(("CREATE TABLE x(value, array HIDDEN)".to_owned(), vtab))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        info.set_estimated_cost(1.);
        Ok(())
    }

    fn open(&'vtab mut self) -> rusqlite::Result<UnnestTabCursor<'vtab>> {
        Ok(UnnestTabCursor::default())
    }
}

#[derive(Default)]
#[repr(C)]
struct UnnestTabCursor<'vtab> {
    /// Base class. Must be first.
    base: sqlite3_vtab_cursor,
    /// The rowid of the cursor's current row.
    row_id: i64,
    phantom: PhantomData<&'vtab UnnestTab>,
}

unsafe impl VTabCursor for UnnestTabCursor<'_> {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        _args: &Values<'_>,
    ) -> rusqlite::Result<()> {
        self.row_id = 1;
        Ok(())
    }

    fn next(&mut self) -> rusqlite::Result<()> {
        self.row_id += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.row_id > 1
    }

    fn column(&self, ctx: &mut Context, _: c_int) -> rusqlite::Result<()> {
        ctx.set_result(&self.row_id)
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(self.row_id)
    }
}

/// Register the `UNNEST` module with the given SQLite3 database connection.
#[allow(dead_code)]
pub fn register_unnest(db: &rusqlite::Connection) -> rusqlite::Result<()> {
    let module = eponymous_only_module::<UnnestTab>();
    db.create_module("unnest", module, None)
}
