"""SQLite bindings with morph ORM integration for Mojo.

``sqlite`` provides three layers of abstraction over the SQLite C library:

**Layer 1, FFI** (``sqlite.ffi``): raw ``sqlite3_*`` wrappers with
handles stored as ``Int``.  Not intended for direct use.

**Layer 2, Safe API** (``sqlite.db``): ``Database``, ``Statement``,
``Row``, and ``Transaction`` structs that own their handles and clean up on
destruction.

**Layer 3, ORM** (``sqlite.orm``): ``create_table``, ``insert``, and
``query`` generic functions that use compile-time reflection (via
`morph <https://github.com/ehsanmok/morph>`_) to map Mojo structs directly to
SQLite tables.

## Quick Start

```mojo
from sqlite import Database, create_table, insert, query

@fieldwise_init
struct Person(Defaultable, Movable):
    var name: String
    var age: Int
    var score: Float64

    def __init__(out self):
        self.name = ""
        self.age = 0
        self.score = 0.0

def main() raises:
    var db = Database(":memory:")
    create_table[Person](db, "people")
    insert[Person](db, "people", Person(name="Alice", age=30, score=9.5))
    insert[Person](db, "people", Person(name="Bob",   age=25, score=7.2))

    var rows = query[Person](db, "people")
    for i in range(len(rows)):
        print(rows[i].name, rows[i].age, rows[i].score)
```

## Transaction API: context manager (recommended)

``db.transaction()`` supports Mojo's ``with`` statement.  The ``with`` block
auto-commits on clean exit and auto-rolls back (re-raising) if any statement
raises, identical to Python's ``with conn:`` pattern.

```mojo
from sqlite import Database

def transfer(db: Database, from_id: Int, to_id: Int, amount: Int) raises:
    with db.transaction():
        db.execute(
            "UPDATE accounts SET balance = balance - "
            + String(amount) + " WHERE id = " + String(from_id)
        )
        db.execute(
            "UPDATE accounts SET balance = balance + "
            + String(amount) + " WHERE id = " + String(to_id)
        )
    # -> COMMIT on success; ROLLBACK + re-raise if either UPDATE failed
```

For fine-grained control (conditional rollback without raising, multiple
commit points), use the ``var tx`` manual form.  Note: Mojo's
``with``/``__exit__`` protocol requires a non-consuming ``__enter__``, so
``with ... as tx:`` would bind ``tx`` to ``None``, so use ``var tx`` instead:

```mojo
var tx = db.transaction()   # BEGIN
db.execute("INSERT ...")
if some_condition:
    tx.rollback()           # abort without raising
    return
tx.commit()                 # explicit COMMIT
```

## Raw statement API

```mojo
from sqlite import Database

var db = Database(":memory:")
db.execute("CREATE TABLE t (id INTEGER, label TEXT)")
var stmt = db.prepare("INSERT INTO t VALUES (?, ?)")
stmt.bind_int(1, 42)
stmt.bind_text(2, "hello")
_ = stmt.step()

var q = db.prepare("SELECT id, label FROM t")
while True:
    var row = q.step()
    if not row:
        break
    print(row.value().int_val(0), row.value().text_val(1))
```
"""

from .db import Database, Statement, Row, Transaction
from .orm import create_table, insert, query
