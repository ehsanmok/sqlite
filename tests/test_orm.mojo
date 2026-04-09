"""Unit tests for sqlite.orm -- create_table, insert, query.

Uses in-memory databases (``:memory:``) and simple ``@fieldwise_init`` structs.

Coverage:
- ``create_table``: basic, idempotent, multiple tables
- ``insert`` / ``query`` round-trips for all supported field types
- String edge cases: empty, unicode, embedded quotes, newline, large
- Integer edge cases: zero, negative, boundary values
- Float edge cases: 0.0, negative, large; Float32 field
- Bool: True and False round-trips
- Optional fields: None (NULL) and Some for Int, String, Float64, Bool
- WHERE clause filtering and ORDER BY
- Bulk insert / count invariant (50 rows)
"""

from std.testing import assert_equal, assert_true, assert_false
from sqlite.db import Database
from sqlite.orm import create_table, insert, query


# ---------------------------------------------------------------------------
# Test structs
# ---------------------------------------------------------------------------


@fieldwise_init
struct Person(Defaultable, Movable, Copyable):
    """Struct with all primitive field types."""

    var name: String
    var age: Int
    var score: Float64
    var active: Bool

    def __init__(out self):
        self.name = ""
        self.age = 0
        self.score = 0.0
        self.active = False

    def __init__(out self, *, copy: Self):
        self.name = copy.name
        self.age = copy.age
        self.score = copy.score
        self.active = copy.active


@fieldwise_init
struct Record(Defaultable, Movable, Copyable):
    """Struct with Optional fields (Integer and String)."""

    var label: String
    var value: Optional[Int]
    var note: Optional[String]

    def __init__(out self):
        self.label = ""
        self.value = None
        self.note = None

    def __init__(out self, *, copy: Self):
        self.label = copy.label
        self.value = copy.value
        self.note = copy.note


@fieldwise_init
struct Metrics(Defaultable, Movable, Copyable):
    """Struct with Optional Float64 and Optional Bool fields."""

    var tag: String
    var rating: Optional[Float64]
    var enabled: Optional[Bool]

    def __init__(out self):
        self.tag = ""
        self.rating = None
        self.enabled = None

    def __init__(out self, *, copy: Self):
        self.tag = copy.tag
        self.rating = copy.rating
        self.enabled = copy.enabled


@fieldwise_init
struct Sensor(Defaultable, Movable, Copyable):
    """Struct with a Float32 field to verify REAL column mapping."""

    var name: String
    var reading: Float32

    def __init__(out self):
        self.name = ""
        self.reading = Float32(0.0)

    def __init__(out self, *, copy: Self):
        self.name = copy.name
        self.reading = copy.reading


# ---------------------------------------------------------------------------
# create_table tests
# ---------------------------------------------------------------------------


def test_create_table_no_error() raises:
    """``create_table`` executes without error."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")


def test_create_table_idempotent() raises:
    """``create_table`` is idempotent (uses IF NOT EXISTS)."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    create_table[Person](db, "persons")  # second call must not fail


def test_multiple_tables() raises:
    """Multiple distinct tables can coexist in the same connection."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    create_table[Record](db, "records")
    create_table[Metrics](db, "metrics")

    insert[Person](db, "persons", Person(name="Alice", age=30, score=9.5, active=True))
    insert[Record](db, "records", Record(label="r1", value=Optional[Int](1), note=None))
    insert[Metrics](db, "metrics", Metrics(tag="x", rating=None, enabled=Optional[Bool](True)))

    assert_equal(len(query[Person](db, "persons")), 1)
    assert_equal(len(query[Record](db, "records")), 1)
    assert_equal(len(query[Metrics](db, "metrics")), 1)


# ---------------------------------------------------------------------------
# Basic insert / query
# ---------------------------------------------------------------------------


def test_insert_one_row() raises:
    """Inserting one row succeeds without error."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    insert[Person](db, "persons", Person(name="Alice", age=30, score=9.5, active=True))


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
    insert[Person](db, "persons", Person(name="Alice", age=30, score=9.5, active=True))

    var rows = query[Person](db, "persons")
    assert_equal(len(rows), 1)
    assert_equal(rows[0].name, "Alice")
    assert_equal(rows[0].age, 30)
    assert_true(rows[0].active, "active should be True")
    var diff = rows[0].score - 9.5
    assert_true(diff < 0.001 and diff > -0.001, "score mismatch")


def test_insert_multiple_rows() raises:
    """Multiple inserts are all retrievable."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    insert[Person](db, "persons", Person(name="Alice", age=30, score=9.5, active=True))
    insert[Person](db, "persons", Person(name="Bob", age=25, score=7.2, active=False))
    insert[Person](db, "persons", Person(name="Carol", age=40, score=8.8, active=True))

    var rows = query[Person](db, "persons")
    assert_equal(len(rows), 3)


# ---------------------------------------------------------------------------
# String edge cases
# ---------------------------------------------------------------------------


def test_empty_string_field() raises:
    """An empty String field round-trips as empty string."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    insert[Person](db, "persons", Person(name="", age=0, score=0.0, active=False))

    var rows = query[Person](db, "persons")
    assert_equal(rows[0].name, "")


def test_unicode_string_field() raises:
    """A multi-byte UTF-8 String field round-trips correctly."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    var unicode_name = "Ångström 日本語 🔥"
    insert[Person](db, "persons", Person(name=unicode_name, age=1, score=0.0, active=False))

    var rows = query[Person](db, "persons")
    assert_equal(rows[0].name, unicode_name)


def test_string_with_single_quote() raises:
    """A String containing single-quote characters round-trips (injection safe)."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    var tricky = "O'Brien'; DROP TABLE persons; --"
    insert[Person](db, "persons", Person(name=tricky, age=0, score=0.0, active=False))

    var rows = query[Person](db, "persons")
    assert_equal(rows[0].name, tricky, "SQL injection chars must round-trip")


def test_string_with_newline() raises:
    """A String field containing newlines round-trips correctly."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    var multiline = "first line\nsecond line\nthird line"
    insert[Person](db, "persons", Person(name=multiline, age=0, score=0.0, active=False))

    var rows = query[Person](db, "persons")
    assert_equal(rows[0].name, multiline)


def test_large_string_field() raises:
    """A 2000-character String field round-trips without truncation."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    var big = String()
    for _ in range(2000):
        big += "Z"
    insert[Person](db, "persons", Person(name=big, age=0, score=0.0, active=False))

    var rows = query[Person](db, "persons")
    assert_equal(len(rows[0].name), 2000)
    assert_equal(rows[0].name, big)


# ---------------------------------------------------------------------------
# Integer edge cases
# ---------------------------------------------------------------------------


def test_zero_int_field() raises:
    """Integer field value 0 round-trips correctly."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    insert[Person](db, "persons", Person(name="zero", age=0, score=0.0, active=False))

    var rows = query[Person](db, "persons")
    assert_equal(rows[0].age, 0)


def test_negative_int_field() raises:
    """Negative integer field round-trips correctly."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    insert[Person](db, "persons", Person(name="neg", age=-42, score=0.0, active=False))

    var rows = query[Person](db, "persons")
    assert_equal(rows[0].age, -42)


def test_large_positive_int_field() raises:
    """Large positive integer near i64 max round-trips correctly."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    var big_age = 9_223_372_036_854_775_806
    insert[Person](db, "persons", Person(name="bigint", age=big_age, score=0.0, active=False))

    var rows = query[Person](db, "persons")
    assert_equal(rows[0].age, big_age)


def test_large_negative_int_field() raises:
    """Large negative integer round-trips correctly."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    var neg_big = -9_223_372_036_854_775_807
    insert[Person](db, "persons", Person(name="neglarge", age=neg_big, score=0.0, active=False))

    var rows = query[Person](db, "persons")
    assert_equal(rows[0].age, neg_big)


# ---------------------------------------------------------------------------
# Float edge cases
# ---------------------------------------------------------------------------


def test_float_zero_roundtrip() raises:
    """Float64 field value 0.0 round-trips correctly."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    insert[Person](db, "persons", Person(name="fzero", age=0, score=0.0, active=False))

    var rows = query[Person](db, "persons")
    assert_equal(rows[0].score, 0.0)


def test_float_negative_roundtrip() raises:
    """Negative Float64 field round-trips correctly."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    insert[Person](db, "persons", Person(name="fneg", age=0, score=-273.15, active=False))

    var rows = query[Person](db, "persons")
    var diff = rows[0].score - (-273.15)
    assert_true(diff < 0.0001 and diff > -0.0001, "Negative float mismatch")


def test_float32_field() raises:
    """Float32 field round-trips correctly through a REAL column."""
    var db = Database(":memory:")
    create_table[Sensor](db, "sensors")
    insert[Sensor](db, "sensors", Sensor(name="s1", reading=Float32(3.14)))

    var rows = query[Sensor](db, "sensors")
    assert_equal(len(rows), 1)
    assert_equal(rows[0].name, "s1")
    # Float32 → Float64 → Float32 loses some precision; use approximate comparison.
    var diff = Float64(rows[0].reading) - Float64(Float32(3.14))
    assert_true(diff < 0.001 and diff > -0.001, "Float32 round-trip precision lost")


# ---------------------------------------------------------------------------
# Bool edge cases
# ---------------------------------------------------------------------------


def test_bool_true_roundtrip() raises:
    """Bool=True is stored as 1 and restored as True."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    insert[Person](db, "persons", Person(name="tr", age=0, score=0.0, active=True))

    var rows = query[Person](db, "persons")
    assert_true(rows[0].active, "active should be True")


def test_bool_false_roundtrip() raises:
    """Bool=False is stored as 0 and restored as False."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    insert[Person](db, "persons", Person(name="fa", age=0, score=0.0, active=False))

    var rows = query[Person](db, "persons")
    assert_false(rows[0].active, "active should be False")


def test_bool_both_values() raises:
    """Inserting rows with True and False active preserves both values."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    insert[Person](db, "persons", Person(name="t", age=1, score=0.0, active=True))
    insert[Person](db, "persons", Person(name="f", age=2, score=0.0, active=False))

    var rows = query[Person](db, "persons")
    assert_equal(len(rows), 2)
    # Row order matches insert order via rowid.
    assert_true(rows[0].active, "First row should be active=True")
    assert_false(rows[1].active, "Second row should be active=False")


# ---------------------------------------------------------------------------
# Optional field edge cases
# ---------------------------------------------------------------------------


def test_optional_fields_none() raises:
    """Optional fields set to None are stored as NULL and restored as None."""
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
        db, "records",
        Record(label="r", value=Optional[Int](42), note=Optional[String]("hi"))
    )

    var rows = query[Record](db, "records")
    assert_true(Bool(rows[0].value), "value should be Some")
    assert_equal(rows[0].value.value(), 42)
    assert_true(Bool(rows[0].note), "note should be Some")
    assert_equal(rows[0].note.value(), "hi")


def test_optional_int_zero() raises:
    """Optional[Int](0) round-trips as Some(0), not None."""
    var db = Database(":memory:")
    create_table[Record](db, "records")
    insert[Record](db, "records", Record(label="z", value=Optional[Int](0), note=None))

    var rows = query[Record](db, "records")
    assert_true(Bool(rows[0].value), "Optional[Int](0) should be Some")
    assert_equal(rows[0].value.value(), 0)


def test_optional_int_negative() raises:
    """Optional[Int](-7) round-trips correctly."""
    var db = Database(":memory:")
    create_table[Record](db, "records")
    insert[Record](db, "records", Record(label="n", value=Optional[Int](-7), note=None))

    var rows = query[Record](db, "records")
    assert_equal(rows[0].value.value(), -7)


def test_optional_string_empty() raises:
    """Optional[String]("") round-trips as Some("")."""
    var db = Database(":memory:")
    create_table[Record](db, "records")
    insert[Record](
        db, "records",
        Record(label="e", value=None, note=Optional[String](""))
    )

    var rows = query[Record](db, "records")
    assert_true(Bool(rows[0].note), "Optional[String]('') should be Some")
    assert_equal(rows[0].note.value(), "")


def test_optional_float_some() raises:
    """Optional[Float64] with a value round-trips correctly."""
    var db = Database(":memory:")
    create_table[Metrics](db, "metrics")
    insert[Metrics](
        db, "metrics",
        Metrics(tag="pi", rating=Optional[Float64](3.14159), enabled=None)
    )

    var rows = query[Metrics](db, "metrics")
    assert_true(Bool(rows[0].rating), "rating should be Some")
    var diff = rows[0].rating.value() - 3.14159
    assert_true(diff < 0.00001 and diff > -0.00001, "Float64 optional precision lost")


def test_optional_float_none() raises:
    """Optional[Float64] set to None is stored as NULL."""
    var db = Database(":memory:")
    create_table[Metrics](db, "metrics")
    insert[Metrics](db, "metrics", Metrics(tag="none", rating=None, enabled=None))

    var rows = query[Metrics](db, "metrics")
    assert_false(Bool(rows[0].rating), "rating should be None")


def test_optional_bool_true() raises:
    """Optional[Bool](True) round-trips as Some(True)."""
    var db = Database(":memory:")
    create_table[Metrics](db, "metrics")
    insert[Metrics](
        db, "metrics",
        Metrics(tag="bt", rating=None, enabled=Optional[Bool](True))
    )

    var rows = query[Metrics](db, "metrics")
    assert_true(Bool(rows[0].enabled), "enabled should be Some")
    assert_true(rows[0].enabled.value(), "enabled value should be True")


def test_optional_bool_false() raises:
    """Optional[Bool](False) round-trips as Some(False)."""
    var db = Database(":memory:")
    create_table[Metrics](db, "metrics")
    insert[Metrics](
        db, "metrics",
        Metrics(tag="bf", rating=None, enabled=Optional[Bool](False))
    )

    var rows = query[Metrics](db, "metrics")
    assert_true(Bool(rows[0].enabled), "enabled should be Some")
    assert_false(rows[0].enabled.value(), "enabled value should be False")


def test_all_optionals_none() raises:
    """A row with every Optional field set to None is retrieved correctly."""
    var db = Database(":memory:")
    create_table[Metrics](db, "metrics")
    insert[Metrics](db, "metrics", Metrics(tag="all-none", rating=None, enabled=None))

    var rows = query[Metrics](db, "metrics")
    assert_equal(rows[0].tag, "all-none")
    assert_false(Bool(rows[0].rating))
    assert_false(Bool(rows[0].enabled))


def test_all_optionals_some() raises:
    """A row with every Optional field set to Some is retrieved correctly."""
    var db = Database(":memory:")
    create_table[Metrics](db, "metrics")
    insert[Metrics](
        db, "metrics",
        Metrics(tag="all-some", rating=Optional[Float64](1.0), enabled=Optional[Bool](True))
    )

    var rows = query[Metrics](db, "metrics")
    assert_true(Bool(rows[0].rating))
    assert_true(Bool(rows[0].enabled))
    assert_equal(rows[0].rating.value(), 1.0)
    assert_true(rows[0].enabled.value())


# ---------------------------------------------------------------------------
# WHERE clause and ordering
# ---------------------------------------------------------------------------


def test_query_where_clause() raises:
    """WHERE clause filters results correctly."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    insert[Person](db, "persons", Person(name="Alice", age=30, score=9.5, active=True))
    insert[Person](db, "persons", Person(name="Bob", age=25, score=7.2, active=False))

    var rows = query[Person](db, "persons", where="age > 28")
    assert_equal(len(rows), 1)
    assert_equal(rows[0].name, "Alice")


def test_query_where_filters_all() raises:
    """WHERE that matches nothing returns an empty list."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    insert[Person](db, "persons", Person(name="Alice", age=30, score=9.5, active=True))

    var rows = query[Person](db, "persons", where="age > 100")
    assert_equal(len(rows), 0)


def test_query_where_matches_all() raises:
    """WHERE that matches all rows returns the full list."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    insert[Person](db, "persons", Person(name="Alice", age=30, score=9.5, active=True))
    insert[Person](db, "persons", Person(name="Bob", age=25, score=7.2, active=False))

    var rows = query[Person](db, "persons", where="age >= 0")
    assert_equal(len(rows), 2)


def test_query_order_by_name() raises:
    """ORDER BY in the WHERE parameter sorts results correctly."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")
    insert[Person](db, "persons", Person(name="Carol", age=40, score=8.8, active=True))
    insert[Person](db, "persons", Person(name="Alice", age=30, score=9.5, active=True))
    insert[Person](db, "persons", Person(name="Bob", age=25, score=7.2, active=False))

    var rows = query[Person](db, "persons", where="1=1 ORDER BY name")
    assert_equal(rows[0].name, "Alice")
    assert_equal(rows[1].name, "Bob")
    assert_equal(rows[2].name, "Carol")


# ---------------------------------------------------------------------------
# Bulk insert / count invariant
# ---------------------------------------------------------------------------


def test_bulk_insert_50_rows() raises:
    """Inserting 50 rows produces exactly 50 retrievable rows."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")

    for i in range(50):
        insert[Person](
            db, "persons",
            Person(
                name="user_" + String(i),
                age=i,
                score=Float64(i) * 0.1,
                active=(i % 2 == 0),
            ),
        )

    var rows = query[Person](db, "persons")
    assert_equal(len(rows), 50)


def test_bulk_insert_count_active() raises:
    """25 of 50 inserted rows have active=True (even indices)."""
    var db = Database(":memory:")
    create_table[Person](db, "persons")

    for i in range(50):
        insert[Person](
            db, "persons",
            Person(name=String(i), age=i, score=0.0, active=(i % 2 == 0)),
        )

    var active_rows = query[Person](db, "persons", where="active = 1")
    assert_equal(len(active_rows), 25)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() raises:
    # create_table
    test_create_table_no_error()
    print("test_create_table_no_error           PASSED")
    test_create_table_idempotent()
    print("test_create_table_idempotent         PASSED")
    test_multiple_tables()
    print("test_multiple_tables                 PASSED")

    # Basic insert / query
    test_insert_one_row()
    print("test_insert_one_row                  PASSED")
    test_query_empty_table()
    print("test_query_empty_table               PASSED")
    test_insert_query_roundtrip()
    print("test_insert_query_roundtrip          PASSED")
    test_insert_multiple_rows()
    print("test_insert_multiple_rows            PASSED")

    # String edge cases
    test_empty_string_field()
    print("test_empty_string_field              PASSED")
    test_unicode_string_field()
    print("test_unicode_string_field            PASSED")
    test_string_with_single_quote()
    print("test_string_with_single_quote        PASSED")
    test_string_with_newline()
    print("test_string_with_newline             PASSED")
    test_large_string_field()
    print("test_large_string_field              PASSED")

    # Integer edge cases
    test_zero_int_field()
    print("test_zero_int_field                  PASSED")
    test_negative_int_field()
    print("test_negative_int_field              PASSED")
    test_large_positive_int_field()
    print("test_large_positive_int_field        PASSED")
    test_large_negative_int_field()
    print("test_large_negative_int_field        PASSED")

    # Float edge cases
    test_float_zero_roundtrip()
    print("test_float_zero_roundtrip            PASSED")
    test_float_negative_roundtrip()
    print("test_float_negative_roundtrip        PASSED")
    test_float32_field()
    print("test_float32_field                   PASSED")

    # Bool
    test_bool_true_roundtrip()
    print("test_bool_true_roundtrip             PASSED")
    test_bool_false_roundtrip()
    print("test_bool_false_roundtrip            PASSED")
    test_bool_both_values()
    print("test_bool_both_values                PASSED")

    # Optional
    test_optional_fields_none()
    print("test_optional_fields_none            PASSED")
    test_optional_fields_some()
    print("test_optional_fields_some            PASSED")
    test_optional_int_zero()
    print("test_optional_int_zero               PASSED")
    test_optional_int_negative()
    print("test_optional_int_negative           PASSED")
    test_optional_string_empty()
    print("test_optional_string_empty           PASSED")
    test_optional_float_some()
    print("test_optional_float_some             PASSED")
    test_optional_float_none()
    print("test_optional_float_none             PASSED")
    test_optional_bool_true()
    print("test_optional_bool_true              PASSED")
    test_optional_bool_false()
    print("test_optional_bool_false             PASSED")
    test_all_optionals_none()
    print("test_all_optionals_none              PASSED")
    test_all_optionals_some()
    print("test_all_optionals_some              PASSED")

    # WHERE and ordering
    test_query_where_clause()
    print("test_query_where_clause              PASSED")
    test_query_where_filters_all()
    print("test_query_where_filters_all         PASSED")
    test_query_where_matches_all()
    print("test_query_where_matches_all         PASSED")
    test_query_order_by_name()
    print("test_query_order_by_name             PASSED")

    # Bulk
    test_bulk_insert_50_rows()
    print("test_bulk_insert_50_rows             PASSED")
    test_bulk_insert_count_active()
    print("test_bulk_insert_count_active        PASSED")

    print("\nAll ORM tests passed.")
