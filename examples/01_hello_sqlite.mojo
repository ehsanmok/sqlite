"""Example 1 — Hello, SQLite.

The simplest possible mosqlite program:

1. Open an in-memory database.
2. Create a table.
3. Insert a row using ``execute`` (no parameter binding).
4. Read the row back with a prepared ``SELECT``.

No ORM, no binding — just raw SQL strings.  This is the right starting point
when you want to understand what mosqlite wraps.
"""

from mosqlite.db import Database


def main() raises:
    # Open (or create) an in-memory database.
    # Use a file path like "/tmp/hello.db" for a persistent database.
    var db = Database(":memory:")

    # DDL: create the table.  ``execute`` is fire-and-forget for statements
    # that return no rows (CREATE, DROP, INSERT without RETURNING, etc.).
    db.execute("CREATE TABLE greetings (id INTEGER, message TEXT)")

    # DML: insert a row using a plain string (fine for constant data).
    db.execute("INSERT INTO greetings VALUES (1, 'Hello, SQLite!')")

    # Query: prepare a SELECT, then step through results.
    var stmt = db.prepare("SELECT id, message FROM greetings")

    while True:
        var maybe_row = stmt.step()
        if not maybe_row:
            break          # SQLITE_DONE — no more rows

        # Row borrows from the statement; take a reference to avoid a copy.
        ref row = maybe_row.value()
        print("id:", row.int_val(0), "| message:", row.text_val(1))

    # db and stmt go out of scope here; their destructors close the connection
    # and finalize the prepared statement automatically.
    print("Done.")
