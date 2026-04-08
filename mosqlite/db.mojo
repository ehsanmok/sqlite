"""Safe Mojo API for SQLite -- Layer 2.

Provides four structs that wrap the raw FFI handles from ``ffi.mojo``:

- ``Database``    -- owns a SQLite connection; closes it on destruction.
- ``Statement``   -- owns a prepared statement; finalizes it on destruction.
- ``Row``         -- a snapshot of a single result row (value types, not a borrow).
- ``Transaction`` -- RAII transaction guard; rolls back automatically unless
                     ``commit()`` is called.

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

**Transaction usage (auto-rollback on error)**::

    var tx = db.transaction()       # issues BEGIN
    db.execute("INSERT ...")
    db.execute("INSERT ...")        # if this raises, tx is destroyed → ROLLBACK
    tx.commit()                     # issues COMMIT; destructor becomes a no-op
"""

from .ffi import (
    SQLITE_ROW,
    _sqlite3_open,
    _sqlite3_close,
    _sqlite3_errmsg,
    _sqlite3_exec,
    _sqlite3_prepare_v2,
    _sqlite3_step,
    _sqlite3_reset,
    _sqlite3_finalize,
    _sqlite3_bind_int,
    _sqlite3_bind_double,
    _sqlite3_bind_text,
    _sqlite3_bind_null,
    _sqlite3_column_count,
    _sqlite3_column_type,
    _sqlite3_column_int,
    _sqlite3_column_double,
    _sqlite3_column_text,
    SQLITE_INTEGER,
    SQLITE_FLOAT,
    SQLITE_TEXT,
    SQLITE_NULL,
)


# -----------------------------------------------------------------------
# Transaction
# -----------------------------------------------------------------------


struct Transaction(Movable):
    """RAII transaction guard — automatically rolls back unless committed.

    Obtain a ``Transaction`` via ``Database.transaction()``, which issues a
    ``BEGIN`` immediately.  If ``commit()`` is called the changes are
    persisted.  If the ``Transaction`` is destroyed before ``commit()`` —
    whether by an exception unwinding the stack, an early ``return``, or
    explicit ``rollback()`` — a ``ROLLBACK`` is issued automatically.

    **Idiomatic Mojo usage pattern** — call ``rollback()`` explicitly in the
    ``except`` handler (Mojo's ``def`` scoping keeps a ``var`` alive until the
    end of the enclosing function, so the destructor fires too late to serve
    as a Python-style ``with`` statement replacement):

    .. code-block:: mojo

        def transfer(db: Database, from_id: Int, to_id: Int, amount: Int) raises:
            var tx = db.transaction()           # BEGIN
            try:
                db.execute("UPDATE accounts SET balance = balance - "
                           + String(amount) + " WHERE id = " + String(from_id))
                db.execute("UPDATE accounts SET balance = balance + "
                           + String(amount) + " WHERE id = " + String(to_id))
                tx.commit()                     # COMMIT on success
            except e:
                tx.rollback()                   # ROLLBACK on any error
                raise e                         # re-raise to caller

    Alternatively, to abandon a transaction at a known point without raising,
    use ``_ = tx^`` to consume (and immediately destroy) the guard:

    .. code-block:: mojo

        var tx = db.transaction()
        db.execute("INSERT ...")
        _ = tx^                                 # guard destroyed → ROLLBACK

    Note:
        Nested transactions are not supported by SQLite's
        ``BEGIN``/``COMMIT`` protocol.  Use ``SAVEPOINT`` directly if you
        need nesting.
    """

    var _handle: Int   # sqlite3 connection handle (non-owning borrow)
    var _done: Bool    # True after commit() or rollback(); silences __del__

    def __init__(out self, handle: Int) raises:
        """Begin a new transaction on ``handle``.

        Args:
            handle: Open ``sqlite3*`` connection handle.

        Raises:
            Error: If ``BEGIN`` fails (e.g. a transaction is already active).
        """
        self._handle = handle
        self._done   = False
        _sqlite3_exec(handle, "BEGIN")

    def __del__(deinit self):
        """Issue ``ROLLBACK`` if the transaction was neither committed nor rolled back.

        Errors from the implicit ``ROLLBACK`` are swallowed so destructors
        never raise.
        """
        if not self._done:
            try:
                _sqlite3_exec(self._handle, "ROLLBACK")
            except:
                pass

    def commit(mut self) raises:
        """Commit the transaction and make all changes permanent.

        After this call the destructor becomes a no-op.

        Raises:
            Error: If ``COMMIT`` fails.
        """
        if not self._done:
            _sqlite3_exec(self._handle, "COMMIT")
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
            _sqlite3_exec(self._handle, "ROLLBACK")
            self._done = True


# -----------------------------------------------------------------------
# Row
# -----------------------------------------------------------------------


struct Row(Movable):
    """An immutable snapshot of a single result row.

    All column values are copied out of the statement at construction time,
    so the ``Row`` outlives the ``Statement`` that produced it.

    Column indices are 0-based throughout.
    """

    var _ncols: Int
    var _types: List[Int]
    var _ints:  List[Int]
    var _floats: List[Float64]
    var _texts:  List[String]

    def __init__(out self, stmt: Int):
        """Snapshot all column values from a statement at its current position.

        Args:
            stmt: sqlite3_stmt handle pointing at a ``SQLITE_ROW``.
        """
        self._ncols = _sqlite3_column_count(stmt)
        self._types  = List[Int]()
        self._ints   = List[Int]()
        self._floats = List[Float64]()
        self._texts  = List[String]()

        for col in range(self._ncols):
            var t = Int(_sqlite3_column_type(stmt, col))
            self._types.append(t)
            if t == SQLITE_INTEGER:
                self._ints.append(_sqlite3_column_int(stmt, col))
                self._floats.append(Float64(0))
                self._texts.append(String(""))
            elif t == SQLITE_FLOAT:
                self._ints.append(0)
                self._floats.append(_sqlite3_column_double(stmt, col))
                self._texts.append(String(""))
            elif t == SQLITE_TEXT:
                self._ints.append(0)
                self._floats.append(Float64(0))
                self._texts.append(_sqlite3_column_text(stmt, col))
            else:  # NULL or BLOB
                self._ints.append(0)
                self._floats.append(Float64(0))
                self._texts.append(String(""))

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
        self._db     = db
        self._handle = _sqlite3_prepare_v2(db, sql)

    def __del__(deinit self):
        """Finalize the statement and release its resources."""
        _sqlite3_finalize(self._handle)

    def reset(self) raises:
        """Reset the statement so it can be re-executed.

        Raises:
            Error: If the reset call fails.
        """
        var rc = _sqlite3_reset(self._handle)
        if rc != 0:
            raise Error("sqlite3_reset failed (rc=" + String(Int(rc)) + ")")

    # --- parameter binding -----------------------------------------------

    def bind_int(self, idx: Int, val: Int) raises:
        """Bind an integer value to parameter ``idx``.

        Args:
            idx: 1-based parameter index.
            val: Integer value.

        Raises:
            Error: On binding failure.
        """
        _sqlite3_bind_int(self._handle, idx, val)

    def bind_float(self, idx: Int, val: Float64) raises:
        """Bind a floating-point value to parameter ``idx``.

        Args:
            idx: 1-based parameter index.
            val: Float64 value.

        Raises:
            Error: On binding failure.
        """
        _sqlite3_bind_double(self._handle, idx, val)

    def bind_text(self, idx: Int, val: String) raises:
        """Bind a text value to parameter ``idx``.

        Args:
            idx: 1-based parameter index.
            val: String value (copied by SQLite).

        Raises:
            Error: On binding failure.
        """
        _sqlite3_bind_text(self._handle, idx, val)

    def bind_null(self, idx: Int):
        """Bind SQL NULL to parameter ``idx``.

        Args:
            idx: 1-based parameter index.
        """
        _sqlite3_bind_null(self._handle, idx)

    # --- execution -------------------------------------------------------

    def step(self) raises -> Optional[Row]:
        """Advance the statement by one step.

        Returns:
            ``Some(Row)`` when a result row is available,
            ``None`` when the statement has finished (``SQLITE_DONE``).

        Raises:
            Error: If ``sqlite3_step`` returns an error code.
        """
        var rc = _sqlite3_step(self._handle)
        if rc == SQLITE_ROW:
            return Row(self._handle)
        if Int(rc) == 101:  # SQLITE_DONE
            return None
        raise Error(
            "sqlite3_step failed (rc=" + String(Int(rc)) + "): "
            + _sqlite3_errmsg(self._db)
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

    var _handle: Int

    def __init__(out self, path: String) raises:
        """Open or create a SQLite database.

        Args:
            path: File-system path to the database, or ``:memory:``.

        Raises:
            Error: If ``sqlite3_open`` fails.
        """
        self._handle = _sqlite3_open(path)

    def __del__(deinit self):
        """Close the database connection."""
        _ = _sqlite3_close(self._handle)

    def execute(self, sql: String) raises:
        """Execute one or more SQL statements with no result rows.

        Suitable for DDL (``CREATE TABLE``, ``DROP TABLE``) and DML without
        a return value (``INSERT ... VALUES ...``, ``DELETE ...``).

        Args:
            sql: Semicolon-separated SQL text.

        Raises:
            Error: If ``sqlite3_exec`` fails.
        """
        _sqlite3_exec(self._handle, sql)

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
        return _sqlite3_errmsg(self._handle)
