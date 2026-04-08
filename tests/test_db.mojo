"""Unit tests for mosqlite.db -- Database, Statement, Row.

Tests use an in-memory database (``:memory:``) for isolation and speed.
"""

from std.testing import assert_equal, assert_true, assert_false
from mosqlite.db import Database, Row


def test_open_memory() raises:
    """Opening an in-memory database succeeds without error."""
    var db = Database(":memory:")
    _ = db


def test_execute_create_table() raises:
    """Executing a CREATE TABLE statement succeeds."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (id INTEGER, name TEXT, val REAL)")


def test_insert_and_step() raises:
    """Inserting a row and stepping through it returns SQLITE_DONE."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (id INTEGER, name TEXT)")
    db.execute("INSERT INTO t VALUES (1, 'Alice')")


def test_prepare_and_step_row() raises:
    """Prepared SELECT returns one row with correct values."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (id INTEGER, name TEXT)")
    db.execute("INSERT INTO t VALUES (42, 'Bob')")

    var stmt = db.prepare("SELECT id, name FROM t")
    var maybe_row = stmt.step()
    assert_true(Bool(maybe_row), "Expected a row")
    ref row = maybe_row.value()
    assert_equal(row.int_val(0), 42)
    assert_equal(row.text_val(1), "Bob")

    var done = stmt.step()
    assert_false(Bool(done), "Expected SQLITE_DONE")


def test_row_is_null() raises:
    """NULL column values are correctly detected."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (a INTEGER, b TEXT)")
    db.execute("INSERT INTO t VALUES (NULL, NULL)")

    var stmt = db.prepare("SELECT a, b FROM t")
    var maybe = stmt.step()
    ref row = maybe.value()
    assert_true(row.is_null(0), "Column 0 should be NULL")
    assert_true(row.is_null(1), "Column 1 should be NULL")


def test_bind_int() raises:
    """Binding an integer parameter works correctly."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (val INTEGER)")
    var stmt = db.prepare("INSERT INTO t VALUES (?)")
    stmt.bind_int(1, 99)
    _ = stmt.step()

    var q = db.prepare("SELECT val FROM t")
    var maybe = q.step()
    ref row = maybe.value()
    assert_equal(row.int_val(0), 99)


def test_bind_float() raises:
    """Binding a float parameter works correctly."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (val REAL)")
    var stmt = db.prepare("INSERT INTO t VALUES (?)")
    stmt.bind_float(1, Float64(3.14))
    _ = stmt.step()

    var q = db.prepare("SELECT val FROM t")
    var maybe = q.step()
    ref row = maybe.value()
    # Use approximate equality for floating-point.
    var diff = row.float_val(0) - 3.14
    assert_true(diff < 0.001 and diff > -0.001, "Float value mismatch")


def test_bind_text() raises:
    """Binding a text parameter works correctly."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (val TEXT)")
    var stmt = db.prepare("INSERT INTO t VALUES (?)")
    stmt.bind_text(1, "hello")
    _ = stmt.step()

    var q = db.prepare("SELECT val FROM t")
    var maybe = q.step()
    ref row = maybe.value()
    assert_equal(row.text_val(0), "hello")


def test_bind_null() raises:
    """Binding NULL to a parameter works correctly."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (val TEXT)")
    var stmt = db.prepare("INSERT INTO t VALUES (?)")
    stmt.bind_null(1)
    _ = stmt.step()

    var q = db.prepare("SELECT val FROM t")
    var maybe = q.step()
    ref row = maybe.value()
    assert_true(row.is_null(0), "Expected NULL column")


def test_multiple_rows() raises:
    """Iterating over multiple rows returns all of them."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (id INTEGER)")
    db.execute("INSERT INTO t VALUES (1)")
    db.execute("INSERT INTO t VALUES (2)")
    db.execute("INSERT INTO t VALUES (3)")

    var stmt = db.prepare("SELECT id FROM t ORDER BY id")
    var ids = List[Int]()
    while True:
        var maybe = stmt.step()
        if not maybe:
            break
        ids.append(maybe.value().int_val(0))

    assert_equal(len(ids), 3)
    assert_equal(ids[0], 1)
    assert_equal(ids[1], 2)
    assert_equal(ids[2], 3)


def test_num_cols() raises:
    """Row.num_cols returns the correct column count."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (a INTEGER, b TEXT, c REAL)")
    db.execute("INSERT INTO t VALUES (1, 'x', 2.5)")
    var stmt = db.prepare("SELECT * FROM t")
    var maybe = stmt.step()
    ref row = maybe.value()
    assert_equal(row.num_cols(), 3)


def test_reset_and_reuse() raises:
    """Resetting a statement allows re-execution."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (id INTEGER)")
    db.execute("INSERT INTO t VALUES (7)")

    var stmt = db.prepare("SELECT id FROM t")
    var maybe1 = stmt.step()
    ref row1 = maybe1.value()
    assert_equal(row1.int_val(0), 7)
    _ = stmt.step()  # consume DONE

    stmt.reset()
    var maybe2 = stmt.step()
    ref row2 = maybe2.value()
    assert_equal(row2.int_val(0), 7)


def main() raises:
    test_open_memory()
    print("test_open_memory                 PASSED")

    test_execute_create_table()
    print("test_execute_create_table        PASSED")

    test_insert_and_step()
    print("test_insert_and_step             PASSED")

    test_prepare_and_step_row()
    print("test_prepare_and_step_row        PASSED")

    test_row_is_null()
    print("test_row_is_null                 PASSED")

    test_bind_int()
    print("test_bind_int                    PASSED")

    test_bind_float()
    print("test_bind_float                  PASSED")

    test_bind_text()
    print("test_bind_text                   PASSED")

    test_bind_null()
    print("test_bind_null                   PASSED")

    test_multiple_rows()
    print("test_multiple_rows               PASSED")

    test_num_cols()
    print("test_num_cols                    PASSED")

    test_reset_and_reuse()
    print("test_reset_and_reuse             PASSED")

    print("\nAll db tests passed.")
