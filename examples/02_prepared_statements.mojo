"""Example 2 — Prepared statements and parameter binding.

Demonstrates:

- ``db.prepare`` to compile a statement once and run it many times.
- ``bind_int``, ``bind_float``, ``bind_text``, ``bind_null`` (1-based indices).
- ``stmt.reset()`` to re-execute the same statement with new values.
- Why prepared statements are safer than string interpolation (SQL injection).

The key rule: **parameters are 1-based** (matching the SQLite C API).
"""

from mosqlite.db import Database


def main() raises:
    var db = Database(":memory:")
    db.execute(
        "CREATE TABLE products ("
        "  id    INTEGER,"
        "  name  TEXT,"
        "  price REAL,"
        "  sku   TEXT"   # nullable
        ")"
    )

    # -----------------------------------------------------------------------
    # Compile the INSERT once; bind and step for each row.
    # -----------------------------------------------------------------------
    var ins = db.prepare("INSERT INTO products VALUES (?, ?, ?, ?)")

    # Row 1: all fields present.
    ins.bind_int(1, 1)
    ins.bind_text(2, "Widget")
    ins.bind_float(3, Float64(9.99))
    ins.bind_text(4, "WDG-001")
    _ = ins.step()

    # Reset clears bindings and rewinds the statement for re-use.
    ins.reset()

    # Row 2: sku is unknown — bind NULL explicitly.
    ins.bind_int(1, 2)
    ins.bind_text(2, "Gadget")
    ins.bind_float(3, Float64(24.50))
    ins.bind_null(4)
    _ = ins.step()

    ins.reset()

    # Row 3: loop insertion to show the pattern at scale.
    var names = List[String]()
    names.append("Alpha")
    names.append("Beta")
    names.append("Gamma")

    for i in range(len(names)):
        ins.bind_int(1, i + 3)
        ins.bind_text(2, names[i])
        ins.bind_float(3, Float64(i + 1) * 5.0)
        ins.bind_null(4)   # no SKU for these
        _ = ins.step()
        ins.reset()

    # -----------------------------------------------------------------------
    # Query all rows.
    # -----------------------------------------------------------------------
    var q = db.prepare(
        "SELECT id, name, price, sku FROM products ORDER BY id"
    )

    print("id | name   | price  | sku")
    print("---+--------+--------+--------")

    while True:
        var maybe_row = q.step()
        if not maybe_row:
            break
        ref row = maybe_row.value()

        var sku = "NULL" if row.is_null(3) else row.text_val(3)
        print(
            row.int_val(0), "|",
            row.text_val(1), "|",
            row.float_val(2), "|",
            sku,
        )

    print("Done.")
