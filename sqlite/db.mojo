"""Safe Mojo API for SQLite -- Layer 2.

Provides four structs that wrap the raw FFI handles from ``ffi.mojo``:

- ``Database``    -- owns a SQLite connection; closes it on destruction.
- ``Statement``   -- owns a prepared statement; finalizes it on destruction.
- ``Row``         -- a snapshot of a single result row (value types, not a borrow).
- ``Transaction`` -- context-manager / RAII transaction guard; commits on clean
                     exit, rolls back automatically on exception.

**Basic usage**::

    var db = Database(":memory:")
    db.execute("CREATE TABLE t (id INTEGER, name TEXT)")
    var stmt = db.prepare("INSERT INTO t VALUES (?, ?)")
    stmt.bind_int(1, 1)
    stmt.bind_text(2, "Alice")
    _ = stmt.step()

    var q = db.prepare("SELECT id, name FROM t")
    while True:
        var row = q.step()
        if not row:
            break
        print(row.value().int_val(0), row.value().text_val(1))

**Transaction usage -- context manager (recommended)**::

    with db.transaction():
        db.execute("INSERT INTO orders VALUES (1, 'Alice')")
        db.execute("INSERT INTO line_items VALUES (1, 42, 3)")
    # -> COMMIT on clean exit; ROLLBACK if any statement raises

**Transaction usage -- manual control**::

    var tx = db.transaction()       # issues BEGIN
    db.execute("INSERT ...")
    db.execute("INSERT ...")
    tx.commit()                     # issues COMMIT; destructor becomes a no-op
"""

from .ffi import (
    Sqlite3FFI,
    SQLITE_ROW,
    SQLITE_INTEGER,
    SQLITE_FLOAT,
    SQLITE_TEXT,
    SQLITE_NULL,
)


# -----------------------------------------------------------------------
# Transaction
# -----------------------------------------------------------------------


struct Transaction(Movable):
    """RAII transaction guard -- ``with`` block commits on success, rolls back on error.

    Obtain a ``Transaction`` via ``Database.transaction()``, which issues
    ``BEGIN`` immediately.

    **Context-manager usage (recommended)** -- identical to Python's
    ``with conn:`` pattern::

        with db.transaction():
            db.execute("INSERT INTO orders VALUES (1, 'Alice')")
            db.execute("INSERT INTO line_items VALUES (1, 42, 3)")
        # -> COMMIT issued automatically; both rows written atomically

    If any statement inside raises, ``ROLLBACK`` is issued and the original
    exception propagates::

        with db.transaction():
            db.execute("INSERT INTO t VALUES (1)")
            raise Error("oops")     # -> __exit__(err) -> ROLLBACK, re-raised

    **Manual usage (fine-grained control)** -- use ``var tx`` when you need
    explicit guard access (conditional rollback, multiple commit points).
    Note: Mojo's ``with``/``__exit__`` protocol requires a non-consuming
    ``__enter__``, so ``with ... as tx:`` binds ``tx`` to ``None`` -- use
    ``var tx`` instead::

        var tx = db.transaction()   # BEGIN
        db.execute("INSERT ...")
        if some_condition:
            tx.rollback()           # abort without raising
            return
        tx.commit()                 # explicit COMMIT; destructor becomes no-op

    To abandon without raising, consume the guard immediately::

        var tx = db.transaction()
        _ = tx^                     # guard destroyed -> immediate ROLLBACK

    Note:
        Nested transactions are not supported via ``BEGIN``/``COMMIT``.
        Use ``SAVEPOINT`` directly if you need nesting.
    """

    var _ffi:    Sqlite3FFI
    var _handle: Int   # sqlite3 connection handle (non-owning borrow)
    var _done:   Bool  # True after commit() or rollback(); silences __del__

    def __init__(out self, handle: Int) raises:
        """Begin a new transaction on ``handle``.

        Args:
            handle: Open ``sqlite3*`` connection handle.

        Raises:
            Error: If ``BEGIN`` fails (e.g. a transaction is already active).
        """
        self._ffi    = Sqlite3FFI()
        self._handle = handle
        self._done   = False
        self._ffi.exec(handle, "BEGIN")

    def __del__(deinit self):
        """Issue ``ROLLBACK`` if the transaction was neither committed nor rolled back.

        Errors from the implicit ``ROLLBACK`` are swallowed so destructors
        never raise.
        """
        if not self._done:
            try:
                self._ffi.exec(self._handle, "ROLLBACK")
            except:
                pass

    def commit(mut self) raises:
        """Commit the transaction and make all changes permanent.

        After this call the destructor becomes a no-op.

        Raises:
            Error: If ``COMMIT`` fails.
        """
        if not self._done:
            self._ffi.exec(self._handle, "COMMIT")
            self._done = True

    def rollback(mut self) raises:
        """Explicitly roll back the transaction.

        Useful when you detect a logical error and want to abort early
        without raising an exception.  After this call the destructor
        becomes a no-op.

        Raises:
            Error: If ``ROLLBACK`` fails.
        """
        if not self._done:
            self._ffi.exec(self._handle, "ROLLBACK")
            self._done = True

    # ------------------------------------------------------------------
    # Context-manager protocol
    # ------------------------------------------------------------------

    def __enter__(mut self):
        """Enter the transaction context manager.

        ``BEGIN`` was already issued in ``__init__``, so nothing extra is
        needed here.  Mojo's ``with`` statement calls ``__exit__`` on this
        same object when the block finishes.

        Note:
            Because ``__exit__`` is defined, Mojo requires a non-consuming
            ``__enter__``.  Use ``with db.transaction():`` (without ``as``);
            for explicit guard access, use ``var tx = db.transaction()``.
        """
        pass

    def __exit__(mut self) raises:
        """Commit on clean exit.

        Called automatically when the ``with`` block finishes without raising.
        Equivalent to calling ``commit()`` explicitly.

        Raises:
            Error: If ``COMMIT`` fails (e.g., disk full, constraint violation).
        """
        self.commit()

    def __exit__(mut self, err: Error) -> Bool:
        """Roll back on exception and re-raise.

        Called automatically when an exception escapes the ``with`` block.
        Issues ``ROLLBACK`` to discard all pending changes, swallows any
        rollback error (so the original exception is not replaced), then
        returns ``False`` so the original exception continues to propagate.

        Args:
            err: The exception raised inside the ``with`` block.

        Returns:
            ``False`` -- always re-raises the caller's exception.
        """
        try:
            self.rollback()
        except:
            pass
        return False


# -----------------------------------------------------------------------
# Row
# -----------------------------------------------------------------------


struct Row(Movable):
    """An immutable snapshot of a single result row.

    All column values are copied out of the statement at construction time,
    so the ``Row`` outlives the ``Statement`` that produced it.

    Column indices are 0-based throughout.
    """

    var _ncols:  Int
    var _types:  List[Int]
    var _ints:   List[Int]
    var _floats: List[Float64]
    var _texts:  List[String]

    def __init__(
        out self,
        ncols: Int,
        types: List[Int],
        ints: List[Int],
        floats: List[Float64],
        texts: List[String],
    ):
        """Construct a ``Row`` from pre-read column snapshots.

        Args:
            ncols:  Number of columns.
            types:  Per-column SQLite type codes.
            ints:   Per-column integer values (0 for non-integer columns).
            floats: Per-column float values (0.0 for non-float columns).
            texts:  Per-column text values (empty for non-text columns).
        """
        self._ncols  = ncols
        self._types  = types.copy()
        self._ints   = ints.copy()
        self._floats = floats.copy()
        self._texts  = texts.copy()

    def num_cols(self) -> Int:
        """Return the number of columns in this row.

        Returns:
            Column count.
        """
        return self._ncols

    def int_val(self, col: Int) -> Int:
        """Return the integer value of column ``col``.

        Args:
            col: 0-based column index.

        Returns:
            Integer value (0 if the column is not ``SQLITE_INTEGER``).
        """
        return self._ints[col]

    def float_val(self, col: Int) -> Float64:
        """Return the floating-point value of column ``col``.

        Args:
            col: 0-based column index.

        Returns:
            Float64 value (0.0 if the column is not ``SQLITE_FLOAT``).
        """
        return self._floats[col]

    def text_val(self, col: Int) -> String:
        """Return the text value of column ``col``.

        Args:
            col: 0-based column index.

        Returns:
            String value (empty string if the column is not ``SQLITE_TEXT``).
        """
        return self._texts[col]

    def is_null(self, col: Int) -> Bool:
        """Check whether column ``col`` contains SQL NULL.

        Args:
            col: 0-based column index.

        Returns:
            ``True`` if the stored type is ``SQLITE_NULL``.
        """
        return self._types[col] == SQLITE_NULL


# -----------------------------------------------------------------------
# Statement
# -----------------------------------------------------------------------


struct Statement(Movable):
    """A compiled, prepared SQLite statement.

    Finalizes the underlying ``sqlite3_stmt`` on destruction.
    Parameters use 1-based indexing (as in the SQLite C API).
    """

    var _ffi:    Sqlite3FFI
    var _db:     Int
    var _handle: Int

    def __init__(out self, db: Int, sql: String) raises:
        """Compile a SQL statement.

        Args:
            db:  sqlite3 handle of the owning connection.
            sql: A single SQL statement (no trailing semicolon needed).

        Raises:
            Error: If the SQL fails to compile.
        """
        self._ffi    = Sqlite3FFI()
        self._db     = db
        self._handle = self._ffi.prepare_v2(db, sql)

    def __del__(deinit self):
        """Finalize the statement and release its resources."""
        self._ffi.finalize(self._handle)

    def reset(self) raises:
        """Reset the statement so it can be re-executed.

        Raises:
            Error: If the reset call fails.
        """
        var rc = self._ffi.reset(self._handle)
        if rc != 0:
            raise Error("sqlite3_reset failed (rc=" + String(Int(rc)) + ")")

    # -- parameter binding ---------------------------------------------------

    def bind_int(self, idx: Int, val: Int) raises:
        """Bind an integer value to parameter ``idx``.

        Args:
            idx: 1-based parameter index.
            val: Integer value.

        Raises:
            Error: On binding failure.
        """
        self._ffi.bind_int(self._handle, idx, val)

    def bind_float(self, idx: Int, val: Float64) raises:
        """Bind a floating-point value to parameter ``idx``.

        Args:
            idx: 1-based parameter index.
            val: Float64 value.

        Raises:
            Error: On binding failure.
        """
        self._ffi.bind_double(self._handle, idx, val)

    def bind_text(self, idx: Int, val: String) raises:
        """Bind a text value to parameter ``idx``.

        Args:
            idx: 1-based parameter index.
            val: String value (copied by SQLite).

        Raises:
            Error: On binding failure.
        """
        self._ffi.bind_text(self._handle, idx, val)

    def bind_null(self, idx: Int):
        """Bind SQL NULL to parameter ``idx``.

        Args:
            idx: 1-based parameter index.
        """
        self._ffi.bind_null(self._handle, idx)

    # -- execution -----------------------------------------------------------

    def step(self) raises -> Optional[Row]:
        """Advance the statement by one step.

        Reads all column values into a ``Row`` snapshot when a row is
        available, so the caller does not need to keep the statement alive.

        Returns:
            ``Some(Row)`` when a result row is available,
            ``None`` when the statement has finished (``SQLITE_DONE``).

        Raises:
            Error: If ``sqlite3_step`` returns an error code.
        """
        var rc = self._ffi.step(self._handle)
        if rc == SQLITE_ROW:
            var ncols  = self._ffi.column_count(self._handle)
            var types  = List[Int]()
            var ints   = List[Int]()
            var floats = List[Float64]()
            var texts  = List[String]()
            for col in range(ncols):
                var t = self._ffi.column_type(self._handle, col)
                types.append(t)
                if t == SQLITE_INTEGER:
                    ints.append(self._ffi.column_int(self._handle, col))
                    floats.append(Float64(0))
                    texts.append(String(""))
                elif t == SQLITE_FLOAT:
                    ints.append(0)
                    floats.append(self._ffi.column_double(self._handle, col))
                    texts.append(String(""))
                elif t == SQLITE_TEXT:
                    ints.append(0)
                    floats.append(Float64(0))
                    texts.append(self._ffi.column_text(self._handle, col))
                else:  # NULL or BLOB
                    ints.append(0)
                    floats.append(Float64(0))
                    texts.append(String(""))
            return Row(ncols, types, ints, floats, texts)
        if Int(rc) == 101:  # SQLITE_DONE
            return None
        raise Error(
            "sqlite3_step failed (rc=" + String(Int(rc)) + "): "
            + self._ffi.errmsg(self._db)
        )


# -----------------------------------------------------------------------
# Database
# -----------------------------------------------------------------------


struct Database(Movable):
    """A SQLite database connection.

    Opens (or creates) the database file on construction and closes the
    connection on destruction.  Use ``:memory:`` for a transient in-memory
    database.

    Example::

        var db = Database(":memory:")
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    """

    var _ffi:    Sqlite3FFI
    var _handle: Int

    def __init__(out self, path: String) raises:
        """Open or create a SQLite database.

        Args:
            path: File-system path to the database, or ``:memory:``.

        Raises:
            Error: If ``sqlite3_open`` fails.
        """
        self._ffi    = Sqlite3FFI()
        self._handle = self._ffi.open(path)

    def __del__(deinit self):
        """Close the database connection."""
        _ = self._ffi.close(self._handle)

    def execute(self, sql: String) raises:
        """Execute one or more SQL statements with no result rows.

        Suitable for DDL (``CREATE TABLE``, ``DROP TABLE``) and DML without
        a return value (``INSERT ... VALUES ...``, ``DELETE ...``).

        Args:
            sql: Semicolon-separated SQL text.

        Raises:
            Error: If ``sqlite3_exec`` fails.
        """
        self._ffi.exec(self._handle, sql)

    def prepare(self, sql: String) raises -> Statement:
        """Compile a SQL statement for repeated execution.

        Args:
            sql: A single SQL statement.

        Returns:
            A ready-to-bind ``Statement``.

        Raises:
            Error: If the SQL fails to compile.
        """
        return Statement(self._handle, sql)

    def transaction(self) raises -> Transaction:
        """Begin a new transaction and return an RAII guard.

        The guard issues ``BEGIN`` immediately.  Call ``commit()`` on the
        returned ``Transaction`` to persist changes.  If the guard is
        destroyed before ``commit()`` (exception, early return, or explicit
        ``rollback()``), a ``ROLLBACK`` is issued automatically.

        Returns:
            A new ``Transaction`` with ``BEGIN`` already executed.

        Raises:
            Error: If ``BEGIN`` fails (e.g. a transaction is already open).

        Example::

            var tx = db.transaction()
            db.execute("INSERT INTO orders VALUES (1, 'Alice')")
            db.execute("INSERT INTO line_items VALUES (1, 42, 3)")
            tx.commit()   # both rows committed atomically
        """
        return Transaction(self._handle)

    def last_error(self) -> String:
        """Return the most recent error message for this connection.

        Returns:
            Human-readable error string from ``sqlite3_errmsg``.
        """
        return self._ffi.errmsg(self._handle)
