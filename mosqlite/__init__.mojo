"""SQLite bindings with morph ORM integration for Mojo.

``mosqlite`` provides three layers of abstraction over the SQLite C library:

**Layer 1 -- FFI** (``mosqlite.ffi``): raw ``sqlite3_*`` wrappers with
handles stored as ``Int``.  Not intended for direct use.

**Layer 2 -- Safe API** (``mosqlite.db``): ``Database``, ``Statement``, and
``Row`` structs that own their handles and clean up on destruction.

**Layer 3 -- ORM** (``mosqlite.orm``): ``create_table``, ``insert``, and
``query`` generic functions that use compile-time reflection (via
`morph <https://github.com/ehsanmok/morph>`_) to map Mojo structs directly to
SQLite tables.

## Quick Start

```mojo
from mosqlite import Database, create_table, insert, query

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

## Raw statement API

```mojo
from mosqlite import Database

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

from .db import Database, Statement, Row
from .orm import create_table, insert, query
