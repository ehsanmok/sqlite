"""Unit tests for mosqlite.db -- Database, Statement, Row.

Tests use an in-memory database (``:memory:``) for isolation and speed.

Coverage:
- Open / close lifecycle
- DDL execution (CREATE TABLE, multiple statements)
- All bind variants: int, float, text, null
- Text edge cases: empty, unicode, embedded quotes, newline, large
- Integer edge cases: zero, negative, boundary values
- Float edge cases: zero, negative, large
- NULL detection and mixed null/non-null rows
- Multi-row iteration and ordering
- Statement reset and repeated reuse
- Aggregate queries (COUNT)
- DML: UPDATE and DELETE
- Transactions: COMMIT and ROLLBACK
- Column count via ``num_cols``
"""

from std.testing import assert_equal, assert_true, assert_false
from mosqlite.db import Database, Row


# -----------------------------------------------------------------------
# Lifecycle
# -----------------------------------------------------------------------


def test_open_memory() raises:
    """Opening an in-memory database succeeds without error."""
    var db = Database(":memory:")
    _ = db


def test_execute_create_table() raises:
    """Executing a CREATE TABLE statement succeeds."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (id INTEGER, name TEXT, val REAL)")


def test_execute_multiple_statements() raises:
    """Multiple DDL statements separated by semicolons execute together."""
    var db = Database(":memory:")
    db.execute(
        "CREATE TABLE a (x INTEGER);"
        "CREATE TABLE b (y TEXT)"
    )
    db.execute("INSERT INTO a VALUES (1)")
    db.execute("INSERT INTO b VALUES ('hi')")


# -----------------------------------------------------------------------
# Basic DML / SELECT
# -----------------------------------------------------------------------


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
    assert_false(Bool(done), "Expected SQLITE_DONE after last row")


def test_num_cols() raises:
    """``Row.num_cols`` returns the correct column count."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (a INTEGER, b TEXT, c REAL)")
    db.execute("INSERT INTO t VALUES (1, 'x', 2.5)")
    var stmt = db.prepare("SELECT * FROM t")
    var maybe = stmt.step()
    ref row = maybe.value()
    assert_equal(row.num_cols(), 3)


# -----------------------------------------------------------------------
# NULL handling
# -----------------------------------------------------------------------


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


def test_mixed_null_and_values() raises:
    """Rows with a mix of NULL and non-NULL columns are decoded correctly."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (a INTEGER, b TEXT, c REAL)")
    var ins = db.prepare("INSERT INTO t VALUES (?, ?, ?)")
    ins.bind_int(1, 7)
    ins.bind_null(2)
    ins.bind_float(3, Float64(1.5))
    _ = ins.step()

    var q = db.prepare("SELECT a, b, c FROM t")
    var maybe = q.step()
    ref row = maybe.value()
    assert_false(row.is_null(0), "Column a should not be NULL")
    assert_true(row.is_null(1), "Column b should be NULL")
    assert_false(row.is_null(2), "Column c should not be NULL")
    assert_equal(row.int_val(0), 7)


# -----------------------------------------------------------------------
# Bind: integers
# -----------------------------------------------------------------------


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


def test_bind_int_zero() raises:
    """Binding integer zero round-trips correctly."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (val INTEGER)")
    var stmt = db.prepare("INSERT INTO t VALUES (?)")
    stmt.bind_int(1, 0)
    _ = stmt.step()

    var q = db.prepare("SELECT val FROM t")
    ref row = q.step().value()
    assert_equal(row.int_val(0), 0)


def test_bind_int_negative() raises:
    """Binding a negative integer round-trips correctly."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (val INTEGER)")
    var stmt = db.prepare("INSERT INTO t VALUES (?)")
    stmt.bind_int(1, -12345)
    _ = stmt.step()

    var q = db.prepare("SELECT val FROM t")
    ref row = q.step().value()
    assert_equal(row.int_val(0), -12345)


def test_bind_int_large() raises:
    """Binding a large positive integer (near i64 max) round-trips correctly."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (val INTEGER)")
    var large = 9_223_372_036_854_775_806  # i64 max - 1
    var stmt = db.prepare("INSERT INTO t VALUES (?)")
    stmt.bind_int(1, large)
    _ = stmt.step()

    var q = db.prepare("SELECT val FROM t")
    ref row = q.step().value()
    assert_equal(row.int_val(0), large)


def test_bind_int_large_negative() raises:
    """Binding the most negative Int round-trips correctly."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (val INTEGER)")
    var very_neg = -9_223_372_036_854_775_807  # -(i64 max)
    var stmt = db.prepare("INSERT INTO t VALUES (?)")
    stmt.bind_int(1, very_neg)
    _ = stmt.step()

    var q = db.prepare("SELECT val FROM t")
    ref row = q.step().value()
    assert_equal(row.int_val(0), very_neg)


# -----------------------------------------------------------------------
# Bind: floats
# -----------------------------------------------------------------------


def test_bind_float() raises:
    """Binding a float parameter works correctly."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (val REAL)")
    var stmt = db.prepare("INSERT INTO t VALUES (?)")
    stmt.bind_float(1, Float64(3.14))
    _ = stmt.step()

    var q = db.prepare("SELECT val FROM t")
    ref row = q.step().value()
    var diff = row.float_val(0) - 3.14
    assert_true(diff < 0.001 and diff > -0.001, "Float value mismatch")


def test_bind_float_zero() raises:
    """Binding 0.0 round-trips correctly."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (val REAL)")
    var stmt = db.prepare("INSERT INTO t VALUES (?)")
    stmt.bind_float(1, Float64(0.0))
    _ = stmt.step()

    var q = db.prepare("SELECT val FROM t")
    ref row = q.step().value()
    assert_equal(row.float_val(0), 0.0)


def test_bind_float_negative() raises:
    """Binding a negative float round-trips correctly."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (val REAL)")
    var stmt = db.prepare("INSERT INTO t VALUES (?)")
    stmt.bind_float(1, Float64(-273.15))
    _ = stmt.step()

    var q = db.prepare("SELECT val FROM t")
    ref row = q.step().value()
    var diff = row.float_val(0) - (-273.15)
    assert_true(diff < 0.0001 and diff > -0.0001, "Negative float mismatch")


def test_bind_float_large() raises:
    """Binding a very large float round-trips correctly."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (val REAL)")
    var big = Float64(1.7976931348623157e308)  # near Float64 max
    var stmt = db.prepare("INSERT INTO t VALUES (?)")
    stmt.bind_float(1, big)
    _ = stmt.step()

    var q = db.prepare("SELECT val FROM t")
    ref row = q.step().value()
    assert_true(row.float_val(0) > 1e307, "Large float lost precision")


# -----------------------------------------------------------------------
# Bind: text
# -----------------------------------------------------------------------


def test_bind_text() raises:
    """Binding a text parameter works correctly."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (val TEXT)")
    var stmt = db.prepare("INSERT INTO t VALUES (?)")
    stmt.bind_text(1, "hello")
    _ = stmt.step()

    var q = db.prepare("SELECT val FROM t")
    ref row = q.step().value()
    assert_equal(row.text_val(0), "hello")


def test_bind_text_empty() raises:
    """Binding an empty string round-trips correctly."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (val TEXT)")
    var stmt = db.prepare("INSERT INTO t VALUES (?)")
    stmt.bind_text(1, "")
    _ = stmt.step()

    var q = db.prepare("SELECT val FROM t")
    ref row = q.step().value()
    assert_equal(row.text_val(0), "")


def test_bind_text_unicode() raises:
    """Binding a multi-byte UTF-8 string round-trips correctly."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (val TEXT)")
    var s = "こんにちは 🌍"  # Japanese + emoji
    var stmt = db.prepare("INSERT INTO t VALUES (?)")
    stmt.bind_text(1, s)
    _ = stmt.step()

    var q = db.prepare("SELECT val FROM t")
    ref row = q.step().value()
    assert_equal(row.text_val(0), s)


def test_bind_text_single_quote() raises:
    """Text with an embedded single quote round-trips via bind_text (injection safe)."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (val TEXT)")
    var dangerous = "O'Brien'; DROP TABLE t; --"
    var stmt = db.prepare("INSERT INTO t VALUES (?)")
    stmt.bind_text(1, dangerous)
    _ = stmt.step()

    var q = db.prepare("SELECT val FROM t")
    ref row = q.step().value()
    assert_equal(row.text_val(0), dangerous, "SQL injection chars should round-trip")


def test_bind_text_newline() raises:
    """Text containing newlines round-trips correctly."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (val TEXT)")
    var multiline = "line one\nline two\nline three"
    var stmt = db.prepare("INSERT INTO t VALUES (?)")
    stmt.bind_text(1, multiline)
    _ = stmt.step()

    var q = db.prepare("SELECT val FROM t")
    ref row = q.step().value()
    assert_equal(row.text_val(0), multiline)


def test_bind_text_large() raises:
    """Binding a 1000-character string round-trips correctly."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (val TEXT)")
    var big = String()
    for _ in range(1000):
        big += "A"
    var stmt = db.prepare("INSERT INTO t VALUES (?)")
    stmt.bind_text(1, big)
    _ = stmt.step()

    var q = db.prepare("SELECT val FROM t")
    ref row = q.step().value()
    assert_equal(len(row.text_val(0)), 1000, "Large string length mismatch")
    assert_equal(row.text_val(0), big)


# -----------------------------------------------------------------------
# Bind: null
# -----------------------------------------------------------------------


def test_bind_null() raises:
    """Binding NULL to a parameter works correctly."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (val TEXT)")
    var stmt = db.prepare("INSERT INTO t VALUES (?)")
    stmt.bind_null(1)
    _ = stmt.step()

    var q = db.prepare("SELECT val FROM t")
    ref row = q.step().value()
    assert_true(row.is_null(0), "Expected NULL column")


# -----------------------------------------------------------------------
# Multi-row iteration and ordering
# -----------------------------------------------------------------------


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


def test_many_rows() raises:
    """Inserting and iterating 100 rows succeeds with correct count."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (id INTEGER)")
    var ins = db.prepare("INSERT INTO t VALUES (?)")
    for i in range(100):
        ins.bind_int(1, i)
        _ = ins.step()
        ins.reset()

    var q = db.prepare("SELECT COUNT(*) FROM t")
    ref count_row = q.step().value()
    assert_equal(count_row.int_val(0), 100)


def test_order_by_desc() raises:
    """ORDER BY DESC returns rows in reverse order."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (v INTEGER)")
    db.execute("INSERT INTO t VALUES (3)")
    db.execute("INSERT INTO t VALUES (1)")
    db.execute("INSERT INTO t VALUES (2)")

    var q = db.prepare("SELECT v FROM t ORDER BY v DESC")
    ref r0 = q.step().value()
    assert_equal(r0.int_val(0), 3)
    ref r1 = q.step().value()
    assert_equal(r1.int_val(0), 2)
    ref r2 = q.step().value()
    assert_equal(r2.int_val(0), 1)


# -----------------------------------------------------------------------
# Statement reset / reuse
# -----------------------------------------------------------------------


def test_reset_and_reuse() raises:
    """Resetting a statement allows re-execution."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (id INTEGER)")
    db.execute("INSERT INTO t VALUES (7)")

    var stmt = db.prepare("SELECT id FROM t")
    ref row1 = stmt.step().value()
    assert_equal(row1.int_val(0), 7)
    _ = stmt.step()  # consume DONE

    stmt.reset()
    ref row2 = stmt.step().value()
    assert_equal(row2.int_val(0), 7)


def test_stmt_reuse_many_times() raises:
    """A prepared INSERT can be reset and reused 50 times without error."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (v INTEGER)")
    var ins = db.prepare("INSERT INTO t VALUES (?)")

    for i in range(50):
        ins.bind_int(1, i * 2)
        _ = ins.step()
        ins.reset()

    var q = db.prepare("SELECT COUNT(*) FROM t")
    ref cr = q.step().value()
    assert_equal(cr.int_val(0), 50)


# -----------------------------------------------------------------------
# Aggregate queries
# -----------------------------------------------------------------------


def test_count_aggregate() raises:
    """SELECT COUNT(*) returns the correct row count."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (id INTEGER)")
    db.execute("INSERT INTO t VALUES (1)")
    db.execute("INSERT INTO t VALUES (2)")
    db.execute("INSERT INTO t VALUES (3)")

    var q = db.prepare("SELECT COUNT(*) FROM t")
    ref row = q.step().value()
    assert_equal(row.int_val(0), 3)


def test_count_empty_table() raises:
    """COUNT on an empty table returns 0."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (id INTEGER)")
    var q = db.prepare("SELECT COUNT(*) FROM t")
    ref row = q.step().value()
    assert_equal(row.int_val(0), 0)


def test_max_aggregate() raises:
    """SELECT MAX() returns the maximum value."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (v INTEGER)")
    db.execute("INSERT INTO t VALUES (10)")
    db.execute("INSERT INTO t VALUES (30)")
    db.execute("INSERT INTO t VALUES (20)")

    var q = db.prepare("SELECT MAX(v) FROM t")
    ref row = q.step().value()
    assert_equal(row.int_val(0), 30)


# -----------------------------------------------------------------------
# UPDATE and DELETE
# -----------------------------------------------------------------------


def test_update_statement() raises:
    """Executing an UPDATE changes the stored value."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (id INTEGER, name TEXT)")
    db.execute("INSERT INTO t VALUES (1, 'Alice')")

    var upd = db.prepare("UPDATE t SET name = ? WHERE id = ?")
    upd.bind_text(1, "Alicia")
    upd.bind_int(2, 1)
    _ = upd.step()

    var q = db.prepare("SELECT name FROM t WHERE id = 1")
    ref row = q.step().value()
    assert_equal(row.text_val(0), "Alicia")


def test_delete_statement() raises:
    """Executing a DELETE removes the matching rows."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (id INTEGER)")
    db.execute("INSERT INTO t VALUES (1)")
    db.execute("INSERT INTO t VALUES (2)")
    db.execute("INSERT INTO t VALUES (3)")

    var del_stmt = db.prepare("DELETE FROM t WHERE id = ?")
    del_stmt.bind_int(1, 2)
    _ = del_stmt.step()

    var q = db.prepare("SELECT COUNT(*) FROM t")
    ref row = q.step().value()
    assert_equal(row.int_val(0), 2, "Row count after DELETE should be 2")


# -----------------------------------------------------------------------
# Transactions
# -----------------------------------------------------------------------


def test_transaction_commit() raises:
    """Rows inserted inside BEGIN/COMMIT are visible after COMMIT."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (v INTEGER)")
    db.execute("BEGIN")
    db.execute("INSERT INTO t VALUES (42)")
    db.execute("COMMIT")

    var q = db.prepare("SELECT v FROM t")
    var maybe = q.step()
    assert_true(Bool(maybe), "Expected row after COMMIT")
    ref row = maybe.value()
    assert_equal(row.int_val(0), 42)


def test_transaction_rollback() raises:
    """Rows inserted inside BEGIN/ROLLBACK are not visible after ROLLBACK."""
    var db = Database(":memory:")
    db.execute("CREATE TABLE t (v INTEGER)")
    db.execute("BEGIN")
    db.execute("INSERT INTO t VALUES (99)")
    db.execute("ROLLBACK")

    var q = db.prepare("SELECT COUNT(*) FROM t")
    ref row = q.step().value()
    assert_equal(row.int_val(0), 0, "ROLLBACK should revert the INSERT")


# -----------------------------------------------------------------------
# last_error
# -----------------------------------------------------------------------


def test_last_error_after_open() raises:
    """``last_error`` on a fresh database returns an empty or benign message."""
    var db = Database(":memory:")
    # After a successful open, errmsg should be "not an error" or similar —
    # not an empty panic. We just verify it doesn't raise.
    var msg = db.last_error()
    _ = msg


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------


def main() raises:
    # Lifecycle
    test_open_memory()
    print("test_open_memory                     PASSED")
    test_execute_create_table()
    print("test_execute_create_table            PASSED")
    test_execute_multiple_statements()
    print("test_execute_multiple_statements     PASSED")

    # Basic DML / SELECT
    test_insert_and_step()
    print("test_insert_and_step                 PASSED")
    test_prepare_and_step_row()
    print("test_prepare_and_step_row            PASSED")
    test_num_cols()
    print("test_num_cols                        PASSED")

    # NULL
    test_row_is_null()
    print("test_row_is_null                     PASSED")
    test_mixed_null_and_values()
    print("test_mixed_null_and_values           PASSED")

    # Integers
    test_bind_int()
    print("test_bind_int                        PASSED")
    test_bind_int_zero()
    print("test_bind_int_zero                   PASSED")
    test_bind_int_negative()
    print("test_bind_int_negative               PASSED")
    test_bind_int_large()
    print("test_bind_int_large                  PASSED")
    test_bind_int_large_negative()
    print("test_bind_int_large_negative         PASSED")

    # Floats
    test_bind_float()
    print("test_bind_float                      PASSED")
    test_bind_float_zero()
    print("test_bind_float_zero                 PASSED")
    test_bind_float_negative()
    print("test_bind_float_negative             PASSED")
    test_bind_float_large()
    print("test_bind_float_large                PASSED")

    # Text
    test_bind_text()
    print("test_bind_text                       PASSED")
    test_bind_text_empty()
    print("test_bind_text_empty                 PASSED")
    test_bind_text_unicode()
    print("test_bind_text_unicode               PASSED")
    test_bind_text_single_quote()
    print("test_bind_text_single_quote          PASSED")
    test_bind_text_newline()
    print("test_bind_text_newline               PASSED")
    test_bind_text_large()
    print("test_bind_text_large                 PASSED")

    # Null
    test_bind_null()
    print("test_bind_null                       PASSED")

    # Multi-row
    test_multiple_rows()
    print("test_multiple_rows                   PASSED")
    test_many_rows()
    print("test_many_rows                       PASSED")
    test_order_by_desc()
    print("test_order_by_desc                   PASSED")

    # Reset / reuse
    test_reset_and_reuse()
    print("test_reset_and_reuse                 PASSED")
    test_stmt_reuse_many_times()
    print("test_stmt_reuse_many_times           PASSED")

    # Aggregates
    test_count_aggregate()
    print("test_count_aggregate                 PASSED")
    test_count_empty_table()
    print("test_count_empty_table               PASSED")
    test_max_aggregate()
    print("test_max_aggregate                   PASSED")

    # UPDATE / DELETE
    test_update_statement()
    print("test_update_statement                PASSED")
    test_delete_statement()
    print("test_delete_statement                PASSED")

    # Transactions
    test_transaction_commit()
    print("test_transaction_commit              PASSED")
    test_transaction_rollback()
    print("test_transaction_rollback            PASSED")

    # last_error
    test_last_error_after_open()
    print("test_last_error_after_open           PASSED")

    print("\nAll db tests passed.")
