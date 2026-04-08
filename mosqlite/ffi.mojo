"""Low-level FFI wrappers for the SQLite C library.

All sqlite3 handles (``sqlite3*`` and ``sqlite3_stmt*``) are stored as ``Int``
(pointer address).  String arguments are passed via ``.unsafe_ptr()`` following
the standard ``std.ffi`` pattern.  C strings returned from SQLite are received
as ``CStringSlice[StaticConstantOrigin]`` (or ``Optional[...]`` for nullable
pointers) and immediately copied into owned ``String`` values.

Constants follow the SQLite C API convention (``SQLITE_OK``, ``SQLITE_ROW``,
``SQLITE_DONE``, column types).

Do not call these functions from user code -- use ``db.mojo`` instead.
"""

from std.ffi import external_call, CStringSlice


# -----------------------------------------------------------------------
# Result codes
# -----------------------------------------------------------------------

comptime SQLITE_OK   = 0
comptime SQLITE_ROW  = 100
comptime SQLITE_DONE = 101

# -----------------------------------------------------------------------
# Column type codes (from sqlite3.h)
# -----------------------------------------------------------------------

comptime SQLITE_INTEGER = 1
comptime SQLITE_FLOAT   = 2
comptime SQLITE_TEXT    = 3
comptime SQLITE_BLOB    = 4
comptime SQLITE_NULL    = 5

# -----------------------------------------------------------------------
# Internal helpers
# -----------------------------------------------------------------------


def _check(rc: Int32, msg: String) raises:
    """Raise if rc is not SQLITE_OK.

    Args:
        rc:  Return code from a SQLite function.
        msg: Context message prepended to the error description.

    Raises:
        Error: When rc != SQLITE_OK.
    """
    if rc != SQLITE_OK:
        raise Error(msg + " (sqlite3 rc=" + String(Int(rc)) + ")")


def _cstring_to_string(s: CStringSlice) -> String:
    """Convert a ``CStringSlice`` to an owned ``String``.

    Args:
        s: A non-owning C string slice.

    Returns:
        A new ``String`` containing the same bytes.
    """
    return String(StringSlice(unsafe_from_utf8=s.as_bytes()))


# -----------------------------------------------------------------------
# Connection management
# -----------------------------------------------------------------------


def _sqlite3_open(filename: String) raises -> Int:
    """Open or create a SQLite database file.

    Wraps ``sqlite3_open(filename, &db)``.

    Args:
        filename: File path, or ``:memory:`` for an in-memory database.

    Returns:
        The sqlite3 handle as an integer address.

    Raises:
        Error: If ``sqlite3_open`` returns a non-zero code.
    """
    var db_ptr: Int = 0
    var filename_ = filename
    var rc = external_call["sqlite3_open", Int32](
        filename_.as_c_string_slice().unsafe_ptr(),
        UnsafePointer(to=db_ptr),
    )
    _check(rc, "sqlite3_open('" + filename + "') failed")
    return db_ptr


def _sqlite3_close(db: Int) -> Int32:
    """Close a database connection.

    Wraps ``sqlite3_close(db)``.

    Args:
        db: sqlite3 handle returned by ``_sqlite3_open``.

    Returns:
        SQLite result code.
    """
    return external_call["sqlite3_close", Int32](db)


def _sqlite3_errmsg(db: Int) -> String:
    """Return the most recent error message for a connection.

    Wraps ``sqlite3_errmsg(db)``.

    Args:
        db: sqlite3 handle.

    Returns:
        A copy of the error string (empty string if db is 0).
    """
    if db == 0:
        return String("")
    var s = external_call["sqlite3_errmsg", CStringSlice[StaticConstantOrigin]](db)
    return _cstring_to_string(s)


# -----------------------------------------------------------------------
# Statement execution -- no result set
# -----------------------------------------------------------------------


def _sqlite3_exec(db: Int, sql: String) raises:
    """Execute one or more SQL statements that produce no result rows.

    Wraps ``sqlite3_exec(db, sql, NULL, NULL, NULL)``.

    Args:
        db:  sqlite3 handle.
        sql: Semicolon-separated SQL text (DDL, DML, PRAGMA, etc.).

    Raises:
        Error: If ``sqlite3_exec`` returns a non-zero code.
    """
    var sql_ = sql
    var rc = external_call["sqlite3_exec", Int32](
        db,
        sql_.as_c_string_slice().unsafe_ptr(),
        Int(0),   # no callback
        Int(0),   # no callback arg
        Int(0),   # no errmsg out-param
    )
    _check(rc, "sqlite3_exec failed: " + sql)


# -----------------------------------------------------------------------
# Prepared statements
# -----------------------------------------------------------------------


def _sqlite3_prepare_v2(db: Int, sql: String) raises -> Int:
    """Compile SQL text into a prepared statement.

    Wraps ``sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)``.

    Args:
        db:  sqlite3 handle.
        sql: A single SQL statement (without trailing semicolon).

    Returns:
        The sqlite3_stmt handle as an integer address.

    Raises:
        Error: If compilation fails.
    """
    var stmt: Int = 0
    var sql_ = sql
    var rc = external_call["sqlite3_prepare_v2", Int32](
        db,
        sql_.as_c_string_slice().unsafe_ptr(),
        Int32(-1),
        UnsafePointer(to=stmt),
        Int(0),   # unused tail pointer
    )
    _check(rc, "sqlite3_prepare_v2 failed: " + sql)
    return stmt


def _sqlite3_step(stmt: Int) -> Int32:
    """Advance a prepared statement by one row.

    Wraps ``sqlite3_step(stmt)``.

    Args:
        stmt: sqlite3_stmt handle.

    Returns:
        ``SQLITE_ROW`` if a row is available, ``SQLITE_DONE`` if finished,
        or another SQLite error code.
    """
    return external_call["sqlite3_step", Int32](stmt)


def _sqlite3_reset(stmt: Int) -> Int32:
    """Reset a prepared statement to be re-executed.

    Wraps ``sqlite3_reset(stmt)``.

    Args:
        stmt: sqlite3_stmt handle.

    Returns:
        SQLite result code.
    """
    return external_call["sqlite3_reset", Int32](stmt)


def _sqlite3_finalize(stmt: Int):
    """Destroy a prepared statement.

    Wraps ``sqlite3_finalize(stmt)``.

    Args:
        stmt: sqlite3_stmt handle.
    """
    _ = external_call["sqlite3_finalize", Int32](stmt)


# -----------------------------------------------------------------------
# Parameter binding  (1-based index)
# -----------------------------------------------------------------------


def _sqlite3_bind_int(stmt: Int, idx: Int, val: Int) raises:
    """Bind an integer value to a statement parameter.

    Wraps ``sqlite3_bind_int64(stmt, idx, val)``.

    Args:
        stmt: sqlite3_stmt handle.
        idx:  Parameter index (1-based).
        val:  Integer value to bind.

    Raises:
        Error: On binding failure.
    """
    var rc = external_call["sqlite3_bind_int64", Int32](stmt, Int32(idx), val)
    _check(rc, "sqlite3_bind_int64 (idx=" + String(idx) + ") failed")


def _sqlite3_bind_double(stmt: Int, idx: Int, val: Float64) raises:
    """Bind a floating-point value to a statement parameter.

    Wraps ``sqlite3_bind_double(stmt, idx, val)``.

    Args:
        stmt: sqlite3_stmt handle.
        idx:  Parameter index (1-based).
        val:  Float64 value to bind.

    Raises:
        Error: On binding failure.
    """
    var rc = external_call["sqlite3_bind_double", Int32](stmt, Int32(idx), val)
    _check(rc, "sqlite3_bind_double (idx=" + String(idx) + ") failed")


def _sqlite3_bind_text(stmt: Int, idx: Int, val: String) raises:
    """Bind a text value to a statement parameter.

    Wraps ``sqlite3_bind_text(stmt, idx, val, -1, SQLITE_TRANSIENT)``.

    Args:
        stmt: sqlite3_stmt handle.
        idx:  Parameter index (1-based).
        val:  String value to bind.

    Raises:
        Error: On binding failure.
    """
    # SQLITE_TRANSIENT = -1 tells SQLite to copy the string immediately.
    var val_ = val
    var rc = external_call["sqlite3_bind_text", Int32](
        stmt, Int32(idx), val_.as_c_string_slice().unsafe_ptr(), Int32(-1), Int(-1)
    )
    _check(rc, "sqlite3_bind_text (idx=" + String(idx) + ") failed")


def _sqlite3_bind_null(stmt: Int, idx: Int):
    """Bind SQL NULL to a statement parameter.

    Wraps ``sqlite3_bind_null(stmt, idx)``.

    Args:
        stmt: sqlite3_stmt handle.
        idx:  Parameter index (1-based).
    """
    _ = external_call["sqlite3_bind_null", Int32](stmt, Int32(idx))


# -----------------------------------------------------------------------
# Column reading  (0-based index)
# -----------------------------------------------------------------------


def _sqlite3_column_count(stmt: Int) -> Int:
    """Return the number of columns in a result row.

    Wraps ``sqlite3_column_count(stmt)``.

    Args:
        stmt: sqlite3_stmt handle.

    Returns:
        Number of result columns.
    """
    return Int(external_call["sqlite3_column_count", Int32](stmt))


def _sqlite3_column_type(stmt: Int, col: Int) -> Int32:
    """Return the SQLite type code of a column value.

    Wraps ``sqlite3_column_type(stmt, col)``.

    Args:
        stmt: sqlite3_stmt handle.
        col:  Column index (0-based).

    Returns:
        One of ``SQLITE_INTEGER``, ``SQLITE_FLOAT``, ``SQLITE_TEXT``,
        ``SQLITE_BLOB``, or ``SQLITE_NULL``.
    """
    return external_call["sqlite3_column_type", Int32](stmt, Int32(col))


def _sqlite3_column_int(stmt: Int, col: Int) -> Int:
    """Read an integer column value.

    Wraps ``sqlite3_column_int64(stmt, col)``.

    Args:
        stmt: sqlite3_stmt handle.
        col:  Column index (0-based).

    Returns:
        Integer value.
    """
    return Int(external_call["sqlite3_column_int64", Int64](stmt, Int32(col)))


def _sqlite3_column_double(stmt: Int, col: Int) -> Float64:
    """Read a floating-point column value.

    Wraps ``sqlite3_column_double(stmt, col)``.

    Args:
        stmt: sqlite3_stmt handle.
        col:  Column index (0-based).

    Returns:
        Float64 value.
    """
    return external_call["sqlite3_column_double", Float64](stmt, Int32(col))


def _sqlite3_column_text(stmt: Int, col: Int) -> String:
    """Read a text column value.

    Wraps ``sqlite3_column_text(stmt, col)``.

    Args:
        stmt: sqlite3_stmt handle.
        col:  Column index (0-based).

    Returns:
        A copy of the column's text as a ``String``, or an empty string for
        SQL NULL.
    """
    var maybe = external_call[
        "sqlite3_column_text", Optional[CStringSlice[StaticConstantOrigin]]
    ](stmt, Int32(col))
    if not maybe:
        return String("")
    return _cstring_to_string(maybe.value())
