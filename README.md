# mosqlite

[![CI](https://github.com/ehsanmok/mosqlite/actions/workflows/ci.yml/badge.svg)](https://github.com/ehsanmok/mosqlite/actions)
[![Docs](https://github.com/ehsanmok/mosqlite/actions/workflows/docs.yaml/badge.svg)](https://ehsanmok.github.io/mosqlite)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

SQLite bindings for Mojo with a safe API, Pythonic context-manager
transactions, and an ORM layer powered by compile-time reflection via
[morph](https://github.com/ehsanmok/morph).

## Features

- **Three-layer design** — raw FFI → safe `Database`/`Statement`/`Row` API →
  `morph`-based ORM
- **Pythonic transactions** — `with db.transaction():` commits on success,
  rolls back automatically on exception (identical to Python's `with conn:`)
- **ORM** — `create_table`, `insert`, `query` driven by compile-time struct
  reflection; no SQL to write for basic CRUD
- **Rich type support** — `String`, `Int`, `Int64`, `Float64`, `Float32`,
  `Bool`, `Optional[T]`
- **98 tests** — unit, edge-case, and property-based fuzz tests via
  [mozz](https://github.com/ehsanmok/mozz)

## Quick start

### ORM

```mojo
from mosqlite import Database, create_table, insert, query

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
from mosqlite import Database

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
    # → COMMIT on success; ROLLBACK + re-raise if either UPDATE raised
```

For explicit guard access (e.g., conditional rollback without raising), use
the `var tx` form — Mojo's `with/__exit__` protocol requires a non-consuming
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

# Abandon without raising — immediate ROLLBACK:
var tx2 = db.transaction()
db.execute("INSERT ...")
_ = tx2^                    # consume guard → ROLLBACK right here
```

### Raw prepared statements

```mojo
from mosqlite import Database

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

## API reference

### `Database`

| Method | Description |
|---|---|
| `Database(path)` | Open or create a database file; `":memory:"` for in-memory |
| `db.execute(sql)` | Run one or more statements with no result rows |
| `db.prepare(sql)` | Compile a statement for repeated execution |
| `db.transaction()` | Begin a transaction, return an RAII `Transaction` guard |
| `db.last_error()` | Most recent error message from SQLite |

### `Statement`

| Method | Description |
|---|---|
| `stmt.bind_int(idx, val)` | Bind `Int` to parameter `idx` (1-based) |
| `stmt.bind_float(idx, val)` | Bind `Float64` |
| `stmt.bind_text(idx, val)` | Bind `String` |
| `stmt.bind_null(idx)` | Bind SQL `NULL` |
| `stmt.step()` | Advance one row; returns `Optional[Row]` |
| `stmt.reset()` | Reset for re-execution |

### `Row`

| Method | Description |
|---|---|
| `row.int_val(col)` | Read integer column (0-based) |
| `row.float_val(col)` | Read float column |
| `row.text_val(col)` | Read text column |
| `row.is_null(col)` | Check for SQL `NULL` |
| `row.num_cols()` | Number of columns |

### `Transaction`

`Transaction` implements `__enter__`/`__exit__` so it works directly with
Mojo's `with` statement.  It can also be used manually.

| Usage | Description |
|---|---|
| `with db.transaction():` | Auto-commit on clean exit; auto-rollback on exception |
| `with db.transaction() as tx:` | Same, plus `tx` is accessible inside the block |
| `tx.commit()` | `COMMIT`; subsequent `__exit__` / destructor is a no-op |
| `tx.rollback()` | `ROLLBACK`; subsequent `__exit__` / destructor is a no-op |
| `_ = tx^` | Consume guard → immediate `ROLLBACK` |

### ORM functions

```mojo
from mosqlite import create_table, insert, query
```

| Function | Description |
|---|---|
| `create_table[T](db, table)` | `CREATE TABLE IF NOT EXISTS` from struct fields |
| `insert[T](db, table, value)` | `INSERT` a struct instance |
| `query[T](db, table, where?)` | `SELECT *` into a `List[T]`; optional `WHERE` / `ORDER BY` clause |

**Supported field types:** `String`, `Int`, `Int64`, `Float64`, `Float32`,
`Bool`, `Optional[String]`, `Optional[Int]`, `Optional[Int64]`,
`Optional[Float64]`, `Optional[Float32]`, `Optional[Bool]`.

## Supported column type mapping

| Mojo type | SQLite type |
|---|---|
| `String` | `TEXT` |
| `Int`, `Int64`, `Bool` | `INTEGER` |
| `Float64`, `Float32` | `REAL` |
| `Optional[T]` | nullable column of the inner type |

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
| `07_transactions.mojo` | Bank-transfer demo — `with`, `as tx`, manual, `_ = tx^` |

## Installation

Add to your `pixi.toml`:

```toml
[dependencies]
mosqlite = { git = "https://github.com/ehsanmok/mosqlite.git", branch = "main" }
```

`sqlite` is automatically resolved as a transitive dependency.

## Development

```bash
pixi run tests          # run all 93 tests (db + ORM + fuzz)
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
