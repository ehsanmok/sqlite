"""Example 3 — All SQLite storage classes.

SQLite has five storage classes: NULL, INTEGER, REAL, TEXT, BLOB.
sqlite exposes four of them (BLOB support is future work).

This example:

- Creates a table with one column of each supported type.
- Inserts a row with values and a row with all NULLs.
- Reads both rows back and verifies types via ``Row.is_null``.

``Row`` column accessors always return a safe default (0, 0.0, or "")
for columns that are NULL or of a different type; use ``is_null`` to
distinguish genuine NULL from a zero value.
"""

from sqlite.db import Database, Row


def _print_row(row_num: Int, row: Row) raises:
    """Print one row; show NULL explicitly for null columns."""
    var id_s    = "NULL" if row.is_null(0) else String(row.int_val(0))
    var flag_s  = "NULL" if row.is_null(1) else ("true" if row.int_val(1) != 0 else "false")
    var real_s  = "NULL" if row.is_null(2) else String(row.float_val(2))
    var text_s  = "NULL" if row.is_null(3) else row.text_val(3)
    print(
        "row", row_num, "->",
        "id=" + id_s,
        "flag=" + flag_s,
        "value=" + real_s,
        "label=" + text_s,
    )


def main() raises:
    var db = Database(":memory:")

    # One column per storage class (BLOB omitted).
    db.execute(
        "CREATE TABLE samples ("
        "  id    INTEGER,"   # maps to Int in Mojo
        "  flag  INTEGER,"   # Bool stored as 0/1
        "  value REAL,"      # maps to Float64
        "  label TEXT"       # maps to String
        ")"
    )

    # -----------------------------------------------------------------------
    # Row with concrete values.
    # -----------------------------------------------------------------------
    var ins = db.prepare("INSERT INTO samples VALUES (?, ?, ?, ?)")

    ins.bind_int(1, 42)
    ins.bind_int(2, 1)                # True -> 1
    ins.bind_float(3, Float64(2.718281828))
    ins.bind_text(4, "Euler's number")
    _ = ins.step()
    ins.reset()

    # -----------------------------------------------------------------------
    # Row with all NULLs.
    # -----------------------------------------------------------------------
    ins.bind_null(1)
    ins.bind_null(2)
    ins.bind_null(3)
    ins.bind_null(4)
    _ = ins.step()

    # -----------------------------------------------------------------------
    # Read back both rows.
    # -----------------------------------------------------------------------
    var q = db.prepare("SELECT id, flag, value, label FROM samples")

    var row_num = 1
    while True:
        var maybe_row = q.step()
        if not maybe_row:
            break
        ref row = maybe_row.value()
        _print_row(row_num, row)
        row_num += 1

    print("Done.")
