"""Unit tests for mosqlite.orm -- create_table, insert, query.

Uses in-memory databases (``:memory:``) and simple ``@fieldwise_init`` structs.
"""

from std.testing import assert_equal, assert_true, assert_false
from mosqlite.db import Database
from mosqlite.orm import create_table, insert, query


# ---------------------------------------------------------------------------
# Test structs
# ---------------------------------------------------------------------------


@fieldwise_init
struct Person(Defaultable, Movable, Copyable):
    """Simple test struct with scalar fields."""

    var name: String
    var age: Int
    var score: Float64
    var active: Bool

    def __init__(out self):
        self.name = ""
        self.age = 0
        self.score = 0.0
        self.active = False


@fieldwise_init
struct Record(Defaultable, Movable, Copyable):
    """Test struct with Optional fields."""

    var label: String
    var value: Optional[Int]
    var note: Optional[String]

    def __init__(out self):
        self.label = ""
        self.value = None
        self.note = None


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_create_table_no_error() raises:
    """create_table executes without error."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")


def test_create_table_idempotent() raises:
    """create_table is idempotent (uses IF NOT EXISTS)."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    create_table[Person](db, "persons")  # second call must not fail


def test_insert_one_row() raises:
    """Inserting one row succeeds without error."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    insert[Person](
        db, "persons", Person(name="Alice", age=30, score=9.5, active=True)
    )


def test_query_empty_table() raises:
    """Querying an empty table returns an empty list."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    var rows = query[Person](db, "persons")
    assert_equal(len(rows), 0)


def test_insert_query_roundtrip() raises:
    """Round-trip: inserted values are returned unchanged by query."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    insert[Person](
        db, "persons", Person(name="Alice", age=30, score=9.5, active=True)
    )

    var rows = query[Person](db, "persons")
    assert_equal(len(rows), 1)
    assert_equal(rows[0].name, "Alice")
    assert_equal(rows[0].age, 30)
    assert_true(rows[0].active, "active should be True")

    # Approximate float comparison.
    var diff = rows[0].score - 9.5
    assert_true(diff < 0.001 and diff > -0.001, "score mismatch")


def test_insert_multiple_rows() raises:
    """Multiple inserts are all retrievable."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    insert[Person](
        db, "persons", Person(name="Alice", age=30, score=9.5, active=True)
    )
    insert[Person](
        db, "persons", Person(name="Bob", age=25, score=7.2, active=False)
    )
    insert[Person](
        db, "persons", Person(name="Carol", age=40, score=8.8, active=True)
    )

    var rows = query[Person](db, "persons")
    assert_equal(len(rows), 3)


def test_bool_false_roundtrip() raises:
    """Bool=False is stored as 0 and restored as False."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    insert[Person](
        db, "persons", Person(name="Bob", age=25, score=7.2, active=False)
    )

    var rows = query[Person](db, "persons")
    assert_false(rows[0].active, "active should be False")


def test_optional_fields_none() raises:
    """Optional fields default to None are stored as NULL and restored."""
    var db = Database(":memory:")
    create_table[Record](db, "records")
    insert[Record](db, "records", Record(label="test", value=None, note=None))

    var rows = query[Record](db, "records")
    assert_equal(len(rows), 1)
    assert_equal(rows[0].label, "test")
    assert_false(Bool(rows[0].value), "value should be None")
    assert_false(Bool(rows[0].note), "note should be None")


def test_optional_fields_some() raises:
    """Optional fields with values round-trip correctly."""
    var db = Database(":memory:")
    create_table[Record](db, "records")
    insert[Record](
        db, "records", Record(label="r", value=Optional[Int](42), note=Optional[String]("hi"))
    )

    var rows = query[Record](db, "records")
    assert_equal(len(rows), 1)
    assert_equal(rows[0].label, "r")
    assert_true(Bool(rows[0].value), "value should be Some")
    assert_equal(rows[0].value.value(), 42)
    assert_true(Bool(rows[0].note), "note should be Some")
    assert_equal(rows[0].note.value(), "hi")


def test_query_where_clause() raises:
    """WHERE clause filters results correctly."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    insert[Person](
        db, "persons", Person(name="Alice", age=30, score=9.5, active=True)
    )
    insert[Person](
        db, "persons", Person(name="Bob", age=25, score=7.2, active=False)
    )

    var rows = query[Person](db, "persons", where="age > 28")
    assert_equal(len(rows), 1)
    assert_equal(rows[0].name, "Alice")


def test_multiple_tables() raises:
    """Multiple distinct tables can coexist in the same connection."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    create_table[Record](db, "records")

    insert[Person](
        db, "persons", Person(name="Alice", age=30, score=9.5, active=True)
    )
    insert[Record](
        db,
        "records",
        Record(label="r1", value=Optional[Int](1), note=None),
    )

    var persons = query[Person](db, "persons")
    var records = query[Record](db, "records")
    assert_equal(len(persons), 1)
    assert_equal(len(records), 1)
    assert_equal(persons[0].name, "Alice")
    assert_equal(records[0].label, "r1")


def main() raises:
    test_create_table_no_error()
    print("test_create_table_no_error       PASSED")

    test_create_table_idempotent()
    print("test_create_table_idempotent     PASSED")

    test_insert_one_row()
    print("test_insert_one_row              PASSED")

    test_query_empty_table()
    print("test_query_empty_table           PASSED")

    test_insert_query_roundtrip()
    print("test_insert_query_roundtrip      PASSED")

    test_insert_multiple_rows()
    print("test_insert_multiple_rows        PASSED")

    test_bool_false_roundtrip()
    print("test_bool_false_roundtrip        PASSED")

    test_optional_fields_none()
    print("test_optional_fields_none        PASSED")

    test_optional_fields_some()
    print("test_optional_fields_some        PASSED")

    test_query_where_clause()
    print("test_query_where_clause          PASSED")

    test_multiple_tables()
    print("test_multiple_tables             PASSED")

    print("\nAll ORM tests passed.")
