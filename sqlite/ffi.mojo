"""Low-level FFI wrappers for the SQLite C library.

All sqlite3 handles (``sqlite3*`` and ``sqlite3_stmt*``) are stored as
``Int`` (pointer address).  Input C strings are passed as ``Int`` via
``unsafe_ptr() → Int`` cast.  Output C-string return values are received
as ``UnsafePointer[UInt8, MutExternalOrigin]`` and immediately copied
into owned ``String`` values via ``StringSlice``.

The library is loaded at runtime via ``OwnedDLHandle`` so Mojo's JIT
never needs to resolve SQLite symbols at compile time, eliminating the
``JIT session error: Symbols not found`` failure on Linux.

Do not call ``Sqlite3FFI`` methods from user code -- use ``db.mojo``.
"""

from std.ffi import OwnedDLHandle, RTLD
from std.os import getenv
from std.sys.info import CompilationTarget
from std.memory import UnsafePointer


# -----------------------------------------------------------------------
# Result codes
# -----------------------------------------------------------------------

comptime SQLITE_OK   = 0
comptime SQLITE_ROW  = 100
comptime SQLITE_DONE = 101

# -----------------------------------------------------------------------
# Column type codes
# -----------------------------------------------------------------------

comptime SQLITE_INTEGER = 1
comptime SQLITE_FLOAT   = 2
comptime SQLITE_TEXT    = 3
comptime SQLITE_BLOB    = 4
comptime SQLITE_NULL    = 5


# -----------------------------------------------------------------------
# Internal helpers
# -----------------------------------------------------------------------


def _ptr_to_string(p: UnsafePointer[UInt8, MutExternalOrigin]) -> String:
    """Copy a C string at ``p`` into an owned Mojo ``String``.

    Args:
        p: Pointer to a null-terminated UTF-8 string returned by SQLite.
           Null pointer returns an empty string.

    Returns:
        Owned ``String`` copy, or empty string for null pointers.
    """
    if not p:
        return String("")
    return String(StringSlice(unsafe_from_utf8_ptr=p))


def _find_sqlite3_library() -> String:
    """Locate ``libsqlite3`` via ``$CONDA_PREFIX`` (pixi) or bare soname.

    Search order:
    1. ``$CONDA_PREFIX/lib/libsqlite3.so.0`` (Linux) or
       ``$CONDA_PREFIX/lib/libsqlite3.dylib`` (macOS) when set.
    2. Bare soname, relying on ``LD_LIBRARY_PATH`` / dyld path.

    Returns:
        Library path string for ``OwnedDLHandle``.
    """
    var prefix = getenv("CONDA_PREFIX", "")
    if prefix:
        comptime if CompilationTarget.is_linux():
            return prefix + "/lib/libsqlite3.so.0"
        else:
            return prefix + "/lib/libsqlite3.dylib"
    comptime if CompilationTarget.is_linux():
        return "libsqlite3.so.0"
    else:
        return "libsqlite3.dylib"


def _check(rc: Int32, msg: String) raises:
    """Raise if ``rc`` is not ``SQLITE_OK``.

    Args:
        rc:  Return code from a SQLite function.
        msg: Context message prepended to the error.

    Raises:
        Error: When ``rc != SQLITE_OK``.
    """
    if rc != SQLITE_OK:
        raise Error(msg + " (sqlite3 rc=" + String(Int(rc)) + ")")


# -----------------------------------------------------------------------
# Sqlite3FFI
# -----------------------------------------------------------------------


struct Sqlite3FFI(Movable):
    """Runtime-loaded SQLite FFI: ``dlopen`` + ``dlsym`` for all C entry-points.

    Loads ``libsqlite3`` at construction via ``OwnedDLHandle`` and resolves
    every function pointer via ``get_function``.  All opaque pointer arguments
    (``sqlite3*``, ``sqlite3_stmt*``) are represented as ``Int`` (64-bit on
    all supported platforms), matching the C ABI on x86-64 and arm64 without
    requiring ``UnsafePointer`` type annotations.

    Each ``Database``, ``Statement``, and ``Transaction`` owns one instance.
    The OS reference-counts the underlying shared library, so multiple
    concurrent ``OwnedDLHandle`` objects map to a single loaded image.
    ``RTLD.NODELETE`` ensures ``dlclose`` is a no-op: the library stays
    resident for the process lifetime even as ``Sqlite3FFI`` instances
    are created and destroyed per-request.

    Example::

        var ffi = Sqlite3FFI()
        var db = ffi.open(":memory:")
        ffi.exec(db, "CREATE TABLE t (x INTEGER)")
        _ = ffi.close(db)
    """

    var _lib: OwnedDLHandle

    # -- connection functions ------------------------------------------------
    var _fn_open:   def(Int, Int) abi("C") -> Int32
    var _fn_close:  def(Int) abi("C") -> Int32
    var _fn_errmsg: def(Int) abi("C") -> UnsafePointer[UInt8, MutExternalOrigin]
    var _fn_exec:   def(Int, Int, Int, Int, Int) abi("C") -> Int32

    # -- prepared statement functions ----------------------------------------
    var _fn_prepare:  def(Int, Int, Int32, Int, Int) abi("C") -> Int32
    var _fn_step:     def(Int) abi("C") -> Int32
    var _fn_reset:    def(Int) abi("C") -> Int32
    var _fn_finalize: def(Int) abi("C") -> Int32

    # -- parameter binding (1-based index) -----------------------------------
    var _fn_bind_int:    def(Int, Int32, Int) abi("C") -> Int32
    var _fn_bind_double: def(Int, Int32, Float64) abi("C") -> Int32
    var _fn_bind_text:   def(Int, Int32, Int, Int32, Int) abi("C") -> Int32
    var _fn_bind_null:   def(Int, Int32) abi("C") -> Int32

    # -- column reading (0-based index) --------------------------------------
    var _fn_col_count:  def(Int) abi("C") -> Int32
    var _fn_col_type:   def(Int, Int32) abi("C") -> Int32
    var _fn_col_int64:  def(Int, Int32) abi("C") -> Int
    var _fn_col_double: def(Int, Int32) abi("C") -> Float64
    var _fn_col_text:   def(Int, Int32) abi("C") -> UnsafePointer[UInt8, MutExternalOrigin]

    def __init__(out self, lib_path: String = "") raises:
        """Load ``libsqlite3`` and resolve all function pointers.

        Args:
            lib_path: Explicit path to the library.  If empty,
                      ``_find_sqlite3_library()`` is used (honours
                      ``$CONDA_PREFIX``).

        Raises:
            Error: If the library cannot be opened or a symbol is missing.
        """
        var path = lib_path if lib_path else _find_sqlite3_library()
        # RTLD.NODELETE: dlclose() becomes a no-op for this handle.
        # Without it, each Database destruction calls dlclose() which on
        # some platforms fully unloads libsqlite3, wiping all VFS state,
        # WAL locks, and shared-memory mappings.  This causes "no such
        # table" and SQLITE_CANTOPEN errors on subsequent open() calls.
        # The library stays resident until process exit regardless.
        self._lib = OwnedDLHandle(path, RTLD.NOW | RTLD.GLOBAL | RTLD.NODELETE)

        self._fn_open = self._lib.get_function[def(Int, Int) abi("C") -> Int32](
            "sqlite3_open"
        )
        self._fn_close = self._lib.get_function[def(Int) abi("C") -> Int32](
            "sqlite3_close"
        )
        self._fn_errmsg = self._lib.get_function[
            def(Int) abi("C") -> UnsafePointer[UInt8, MutExternalOrigin]
        ]("sqlite3_errmsg")
        self._fn_exec = self._lib.get_function[
            def(Int, Int, Int, Int, Int) abi("C") -> Int32
        ]("sqlite3_exec")
        self._fn_prepare = self._lib.get_function[
            def(Int, Int, Int32, Int, Int) abi("C") -> Int32
        ]("sqlite3_prepare_v2")
        self._fn_step = self._lib.get_function[def(Int) abi("C") -> Int32](
            "sqlite3_step"
        )
        self._fn_reset = self._lib.get_function[def(Int) abi("C") -> Int32](
            "sqlite3_reset"
        )
        self._fn_finalize = self._lib.get_function[def(Int) abi("C") -> Int32](
            "sqlite3_finalize"
        )
        self._fn_bind_int = self._lib.get_function[
            def(Int, Int32, Int) abi("C") -> Int32
        ]("sqlite3_bind_int64")
        self._fn_bind_double = self._lib.get_function[
            def(Int, Int32, Float64) abi("C") -> Int32
        ]("sqlite3_bind_double")
        self._fn_bind_text = self._lib.get_function[
            def(Int, Int32, Int, Int32, Int) abi("C") -> Int32
        ]("sqlite3_bind_text")
        self._fn_bind_null = self._lib.get_function[def(Int, Int32) abi("C") -> Int32](
            "sqlite3_bind_null"
        )
        self._fn_col_count = self._lib.get_function[def(Int) abi("C") -> Int32](
            "sqlite3_column_count"
        )
        self._fn_col_type = self._lib.get_function[def(Int, Int32) abi("C") -> Int32](
            "sqlite3_column_type"
        )
        self._fn_col_int64 = self._lib.get_function[def(Int, Int32) abi("C") -> Int](
            "sqlite3_column_int64"
        )
        self._fn_col_double = self._lib.get_function[
            def(Int, Int32) abi("C") -> Float64
        ]("sqlite3_column_double")
        self._fn_col_text = self._lib.get_function[
            def(Int, Int32) abi("C") -> UnsafePointer[UInt8, MutExternalOrigin]
        ]("sqlite3_column_text")

    # -- connection ----------------------------------------------------------

    def open(self, filename: String) raises -> Int:
        """Open or create a SQLite database file.

        Uses a ``List[Int]`` as the output buffer for the ``sqlite3**``
        argument (same pattern as simdjson FFI for output pointer args).

        Passes an explicit null-terminated copy of ``filename`` (same
        pattern as ``exec``) to avoid the Mojo ``String`` quirk where
        reused heap buffers may contain stale bytes after the logical
        string end.

        Args:
            filename: File path or ``:memory:`` for an in-memory database.

        Returns:
            sqlite3 connection handle as ``Int``.

        Raises:
            Error: If ``sqlite3_open`` returns a non-zero code.
        """
        var n = filename.byte_length()
        var src = filename.unsafe_ptr()
        var buf = List[UInt8](capacity=n + 1)
        for i in range(n):
            buf.append(src[i])
        buf.append(0)  # explicit null terminator
        var db_out = List[Int](capacity=1)
        db_out.append(0)
        var rc = self._fn_open(
            Int(buf.unsafe_ptr()),
            Int(db_out.unsafe_ptr()),
        )
        _ = buf^  # keep buf alive past the FFI call
        if Int(rc) != SQLITE_OK:
            raise Error("sqlite3_open('" + filename + "') failed (sqlite3 rc=" + String(Int(rc)) + ")")
        return db_out[0]

    def close(self, db: Int) abi("C") -> Int32:
        """Close the database connection.

        Args:
            db: sqlite3 handle.

        Returns:
            SQLite result code.
        """
        return self._fn_close(db)

    def errmsg(self, db: Int) abi("C") -> String:
        """Return the most recent error message for the connection.

        Args:
            db: sqlite3 handle.

        Returns:
            Human-readable error string (empty if ``db`` is 0).
        """
        if db == 0:
            return String("")
        return _ptr_to_string(self._fn_errmsg(db))

    def exec(self, db: Int, sql: String) raises:
        """Execute one or more SQL statements with no result rows.

        ``sqlite3_exec`` reads until a null byte, so we build an explicit
        ``List[UInt8]`` copy of *sql* and append a ``0`` byte before calling
        into C.  ``String.unsafe_ptr()`` returns a read-only pointer in this
        Mojo version, so we cannot patch the null byte in-place.  The copy
        eliminates the Mojo ``String`` quirk where reused heap buffers may
        contain stale bytes after the logical string end.

        Args:
            db:  sqlite3 handle.
            sql: Semicolon-separated SQL text (DDL, DML, PRAGMA, etc.).

        Raises:
            Error: If ``sqlite3_exec`` returns a non-zero code.
        """
        var n = sql.byte_length()
        var src = sql.unsafe_ptr()
        var buf = List[UInt8](capacity=n + 1)
        for i in range(n):
            buf.append(src[i])
        buf.append(0)  # explicit null terminator
        var rc = self._fn_exec(
            db, Int(buf.unsafe_ptr()), Int(0), Int(0), Int(0)
        )
        _ = buf^  # keep buf alive past the FFI call
        if rc != SQLITE_OK:
            var db_err = self.errmsg(db)
            raise Error(
                "sqlite3_exec failed: " + sql
                + " -- " + db_err
                + " (sqlite3 rc=" + String(Int(rc)) + ")"
            )

    # -- prepared statements -------------------------------------------------

    def prepare_v2(self, db: Int, sql: String) raises -> Int:
        """Compile SQL text into a prepared statement.

        Uses a ``List[Int]`` as the output buffer for the ``sqlite3_stmt**``
        argument.

        Passes ``len(sql)`` as the explicit byte count to
        ``sqlite3_prepare_v2`` (instead of ``-1``) so SQLite never reads
        beyond the valid bytes.  This avoids a Mojo ``String`` quirk where
        reused heap buffers may contain stale bytes after the logical
        string end, which with ``nByte = -1`` causes spurious parse errors
        (e.g. ``near "NTEGER": syntax error``).

        Args:
            db:  sqlite3 handle.
            sql: A single SQL statement (without trailing semicolon).

        Returns:
            sqlite3_stmt handle as ``Int``.

        Raises:
            Error: If compilation fails.
        """
        var s = sql
        var sql_len = Int32(s.byte_length())
        var stmt_out = List[Int](capacity=1)
        stmt_out.append(0)
        var rc = self._fn_prepare(
            db,
            Int(s.unsafe_ptr()),
            sql_len,           # explicit byte count — do not use -1
            Int(stmt_out.unsafe_ptr()),
            Int(0),
        )
        _ = s^                 # keep s alive until after the FFI call
        if Int(rc) != SQLITE_OK:
            var db_err = self.errmsg(db)
            raise Error(
                "sqlite3_prepare_v2 failed: " + sql
                + " -- " + db_err
                + " (sqlite3 rc=" + String(Int(rc)) + ")"
            )
        return stmt_out[0]

    def step(self, stmt: Int) abi("C") -> Int32:
        """Advance a prepared statement by one step.

        Args:
            stmt: sqlite3_stmt handle.

        Returns:
            ``SQLITE_ROW``, ``SQLITE_DONE``, or an error code.
        """
        return self._fn_step(stmt)

    def reset(self, stmt: Int) abi("C") -> Int32:
        """Reset a prepared statement for re-execution.

        Args:
            stmt: sqlite3_stmt handle.

        Returns:
            SQLite result code.
        """
        return self._fn_reset(stmt)

    def finalize(self, stmt: Int):
        """Destroy a prepared statement and free its resources.

        Args:
            stmt: sqlite3_stmt handle.
        """
        _ = self._fn_finalize(stmt)

    # -- parameter binding ---------------------------------------------------

    def bind_int(self, stmt: Int, idx: Int, val: Int) raises:
        """Bind an integer value to a statement parameter (1-based index).

        Wraps ``sqlite3_bind_int64``.

        Args:
            stmt: sqlite3_stmt handle.
            idx:  Parameter index (1-based).
            val:  Integer value.

        Raises:
            Error: On binding failure.
        """
        var rc = self._fn_bind_int(stmt, Int32(idx), val)
        _check(rc, "sqlite3_bind_int64 (idx=" + String(idx) + ") failed")

    def bind_double(self, stmt: Int, idx: Int, val: Float64) raises:
        """Bind a floating-point value to a statement parameter.

        Args:
            stmt: sqlite3_stmt handle.
            idx:  Parameter index (1-based).
            val:  Float64 value.

        Raises:
            Error: On binding failure.
        """
        var rc = self._fn_bind_double(stmt, Int32(idx), val)
        _check(rc, "sqlite3_bind_double (idx=" + String(idx) + ") failed")

    def bind_text(self, stmt: Int, idx: Int, val: String) raises:
        """Bind a text value to a statement parameter (SQLITE_TRANSIENT copy).

        Passes the explicit byte length (``len(v)``) to ``sqlite3_bind_text``
        instead of ``-1`` to avoid the Mojo ``String`` null-termination quirk
        where reused buffers may contain stale bytes after the logical end.

        Args:
            stmt: sqlite3_stmt handle.
            idx:  Parameter index (1-based).
            val:  String value (copied by SQLite).

        Raises:
            Error: On binding failure.
        """
        var v = val
        var v_len = Int32(v.byte_length())
        # SQLITE_TRANSIENT = -1: SQLite copies the string immediately.
        var rc = self._fn_bind_text(
            stmt, Int32(idx), Int(v.unsafe_ptr()), v_len, Int(-1)
        )
        _ = v^  # keep v alive past the FFI call
        _check(rc, "sqlite3_bind_text (idx=" + String(idx) + ") failed")

    def bind_null(self, stmt: Int, idx: Int):
        """Bind SQL NULL to a statement parameter.

        Args:
            stmt: sqlite3_stmt handle.
            idx:  Parameter index (1-based).
        """
        _ = self._fn_bind_null(stmt, Int32(idx))

    # -- column reading ------------------------------------------------------

    def column_count(self, stmt: Int) abi("C") -> Int:
        """Return the number of columns in the current result row.

        Args:
            stmt: sqlite3_stmt handle.

        Returns:
            Column count.
        """
        return Int(self._fn_col_count(stmt))

    def column_type(self, stmt: Int, col: Int) abi("C") -> Int:
        """Return the SQLite type code of a column value (0-based index).

        Args:
            stmt: sqlite3_stmt handle.
            col:  Column index (0-based).

        Returns:
            One of ``SQLITE_INTEGER``, ``SQLITE_FLOAT``, ``SQLITE_TEXT``,
            ``SQLITE_BLOB``, or ``SQLITE_NULL``.
        """
        return Int(self._fn_col_type(stmt, Int32(col)))

    def column_int(self, stmt: Int, col: Int) abi("C") -> Int:
        """Read an integer column value (0-based index).

        Args:
            stmt: sqlite3_stmt handle.
            col:  Column index (0-based).

        Returns:
            Integer value (via ``sqlite3_column_int64``).
        """
        return self._fn_col_int64(stmt, Int32(col))

    def column_double(self, stmt: Int, col: Int) abi("C") -> Float64:
        """Read a floating-point column value (0-based index).

        Args:
            stmt: sqlite3_stmt handle.
            col:  Column index (0-based).

        Returns:
            Float64 value.
        """
        return self._fn_col_double(stmt, Int32(col))

    def column_text(self, stmt: Int, col: Int) abi("C") -> String:
        """Read a text column value (0-based index).

        Args:
            stmt: sqlite3_stmt handle.
            col:  Column index (0-based).

        Returns:
            A copy of the column text as an owned ``String``, or an empty
            string for SQL NULL.
        """
        return _ptr_to_string(self._fn_col_text(stmt, Int32(col)))
