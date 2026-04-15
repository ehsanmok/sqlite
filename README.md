# sqlite

[![CI](https://github.com/ehsanmok/sqlite/actions/workflows/ci.yml/badge.svg)](https://github.com/ehsanmok/sqlite/actions)
[![Docs](https://github.com/ehsanmok/sqlite/actions/workflows/docs.yaml/badge.svg)](https://ehsanmok.github.io/sqlite)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

SQLite bindings for Mojo with a safe API, Pythonic context-manager
transactions, and an ORM layer powered by compile-time reflection via
[morph](https://github.com/ehsanmok/morph).

## Features

- **Three-layer design**: raw FFI, safe `Database`/`Statement`/`Row` API, and
  `morph`-based ORM
- **Pythonic transactions**: `with db.transaction():` commits on success and
  rolls back automatically on exception (identical to Python's `with conn:`)
- **ORM**: `create_table`, `insert`, `query` driven by compile-time struct
  reflection; no SQL to write for basic CRUD
- **Rich type support**: `String`, `Int`, `Int64`, `Float64`, `Float32`,
  `Bool`, `Optional[T]`
- **Thoroughly tested**: unit, edge-case, and property-based fuzz tests via
  [mozz](https://github.com/ehsanmok/mozz)

## Quick Start

### ORM

```mojo
from sqlite import Database, create_table, insert, query

@fieldwise_init
struct Person(Defaultable, Movable):
    var name: String
    var age:  Int
    var score: Float64

    def __init__(out self):
        self.name  = ""
        self.age   = 0
        self.score = 0.0

def main() raises:
    var db = Database(":memory:")
    create_table[Person](db, "people")
    insert[Person](db, "people", Person(name="Alice", age=30, score=9.5))
    insert[Person](db, "people", Person(name="Bob",   age=25, score=7.2))

    var rows = query[Person](db, "people")
    for i in range(len(rows)):
        print(rows[i].name, rows[i].age, rows[i].score)
    # Alice 30 9.5
    # Bob   25 7.2
```

### Transactions

`db.transaction()` supports Mojo's `with` statement, giving you the same
auto-commit / auto-rollback semantics as Python's `with conn:`.

#### Context-manager pattern (recommended)

```mojo
from sqlite import Database

def transfer(mut db: Database, from_id: Int, to_id: Int, amount: Int) raises:
    with db.transaction():
        db.execute(
            "UPDATE accounts SET balance = balance - "
            + String(amount) + " WHERE id = " + String(from_id)
        )
        db.execute(
            "UPDATE accounts SET balance = balance + "
            + String(amount) + " WHERE id = " + String(to_id)
        )
    # -> COMMIT on success; ROLLBACK + re-raise if either UPDATE raised
```

For explicit guard access (e.g., conditional rollback without raising), use
the `var tx` form. Mojo's `with/__exit__` protocol requires a non-consuming
`__enter__`, so `with ... as tx:` would bind `tx` to `None`:

```mojo
var tx = db.transaction()   # BEGIN
db.execute("INSERT ...")
if some_condition:
    tx.rollback()           # abort without raising
    return
tx.commit()
```

#### Manual pattern (fine-grained control)

```mojo
var tx = db.transaction()   # BEGIN
db.execute("INSERT ...")
tx.commit()                 # explicit COMMIT

# Abandon without raising: immediate ROLLBACK
var tx2 = db.transaction()
db.execute("INSERT ...")
_ = tx2^                    # consume guard -> ROLLBACK right here
```

### Raw prepared statements

```mojo
from sqlite import Database

def main() raises:
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
        # 42  hello
```

## Installation

Add sqlite to your project's `pixi.toml`:

```toml
[workspace]
channels = ["https://conda.modular.com/max-nightly", "conda-forge"]
preview = ["pixi-build"]

[dependencies]
sqlite = { git = "https://github.com/ehsanmok/sqlite.git", branch = "main" }
```

Then run:

```bash
pixi install
```

## Examples

Progressive examples live in [`examples/`](examples/):

| File | What it shows |
|---|---|
| `01_hello_sqlite.mojo` | Open a database, `CREATE TABLE`, `INSERT`, `SELECT` |
| `02_prepared_statements.mojo` | Bind parameters, iterate rows, reuse statements |
| `03_all_types.mojo` | Every supported column type round-trip |
| `04_orm_basics.mojo` | ORM `create_table` / `insert` / `query` |
| `05_orm_optional.mojo` | `Optional` fields, `WHERE` / `ORDER BY` |
| `06_contacts_app.mojo` | Realistic CRUD mini-app with transactions |
| `07_transactions.mojo` | Bank-transfer demo: `with`, `as tx`, manual, `_ = tx^` |

Full API reference: [ehsanmok.github.io/sqlite](https://ehsanmok.github.io/sqlite)

## Development

```bash
pixi run tests          # run all tests (db + ORM + fuzz)
pixi run test-db        # db layer only
pixi run test-orm       # ORM layer only
pixi run test-fuzz      # property-based fuzz tests
pixi run examples       # run all examples
pixi run example-07     # transactions example
pixi run bench          # micro-benchmarks
pixi run -e dev docs    # build and open API docs
pixi run format         # auto-format source
```

## License

[MIT](LICENSE)
