"""ORM layer -- compile-time struct-to-SQL mapping via morph reflection.

Provides three generic functions that map between Mojo structs and SQLite
tables using the same compile-time reflection pattern as ``envo/loader.mojo``:

- ``create_table[T]`` -- generates and executes a ``CREATE TABLE IF NOT EXISTS``
  DDL statement from the struct's field names and types.
- ``insert[T]`` -- generates a parameterised ``INSERT`` statement and binds
  each struct field value to the corresponding parameter.
- ``query[T]`` -- executes a ``SELECT *`` query and reconstructs ``List[T]``
  from the result rows.

**Type mapping**

+------------------+----------------+
| Mojo type        | SQLite column  |
+==================+================+
| String           | TEXT           |
+------------------+----------------+
| Int / Int64      | INTEGER        |
+------------------+----------------+
| Float64 / Float32| REAL           |
+------------------+----------------+
| Bool             | INTEGER (0/1)  |
+------------------+----------------+

Fields whose types are not in the table above are silently skipped.
``Optional[T]`` fields are stored as their inner type (or NULL when absent).

Example::

    @fieldwise_init
    struct Person(Defaultable, Movable):
        var name: String
        var age: Int
        var score: Float64
        def __init__(out self):
            self.name = ""
            self.age = 0
            self.score = 0.0

    var db = Database(":memory:")
    create_table[Person](db, "people")
    insert[Person](db, "people", Person(name="Alice", age=30, score=9.5))
    var rows = query[Person](db, "people")
"""

from std.reflection import (
    struct_field_count,
    struct_field_names,
    struct_field_types,
    get_type_name,
)
from std.builtin.rebind import trait_downcast
from morph.reflect import (
    _Base,
    Morphable,
    INT_NAME,
    INT64_NAME,
    BOOL_NAME,
    STRING_NAME,
    FLOAT64_NAME,
    FLOAT32_NAME,
    OPT_INT_NAME,
    OPT_STRING_NAME,
    OPT_FLOAT64_NAME,
    OPT_BOOL_NAME,
)
from .db import Database, Row


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def create_table[T: Morphable](db: Database, table: String) raises:
    """Create a SQLite table matching struct ``T``'s fields.

    Executes ``CREATE TABLE IF NOT EXISTS <table> (...)``.  Column order
    matches the struct field declaration order.

    Parameters:
        T: A ``Morphable`` (``Defaultable & Movable``) struct type.

    Args:
        db:    Open database connection.
        table: Target table name.

    Raises:
        Error: If ``sqlite3_exec`` fails (e.g. syntax error, permissions).
    """
    comptime count = struct_field_count[T]()
    comptime names = struct_field_names[T]()
    comptime types = struct_field_types[T]()

    var col_defs = String("")
    var first = True

    comptime
    for idx in range(count):
        comptime field_name = names[idx]
        comptime field_type = types[idx]
        comptime type_name = get_type_name[field_type]()
        comptime sql_type = _sql_type_for[field_type]()

        if sql_type != "":
            if not first:
                col_defs += ", "
            col_defs += String(field_name) + " " + sql_type
            first = False

    var sql = "CREATE TABLE IF NOT EXISTS " + table + " (" + col_defs + ")"
    db.execute(sql)


def insert[T: Morphable](db: Database, table: String, value: T) raises:
    """Insert one row into ``table`` from struct ``value``.

    Generates a parameterised ``INSERT INTO <table> (...) VALUES (?,...)``,
    then binds each struct field to its corresponding ``?`` placeholder.

    Parameters:
        T: A ``Morphable`` struct type.

    Args:
        db:    Open database connection.
        table: Target table name.
        value: Struct instance whose fields are bound as row values.

    Raises:
        Error: If statement preparation or parameter binding fails.
    """
    comptime count = struct_field_count[T]()
    comptime names = struct_field_names[T]()
    comptime types = struct_field_types[T]()

    # Build column list and placeholder list.
    var col_list  = String("")
    var ph_list   = String("")
    var first = True

    comptime
    for idx in range(count):
        comptime field_type = types[idx]
        comptime field_name = names[idx]
        comptime sql_type = _sql_type_for[field_type]()

        if sql_type != "":
            if not first:
                col_list += ", "
                ph_list  += ", "
            col_list += String(field_name)
            ph_list  += "?"
            first = False

    var sql = (
        "INSERT INTO " + table
        + " (" + col_list + ") VALUES (" + ph_list + ")"
    )
    var stmt = db.prepare(sql)

    # Bind field values in the same column order.
    var param_idx = 1

    comptime
    for idx in range(count):
        comptime field_type = types[idx]
        comptime type_name = get_type_name[field_type]()
        comptime sql_type = _sql_type_for[field_type]()

        if sql_type != "":
            ref field = trait_downcast[_Base](__struct_field_ref(idx, value))
            var ptr = UnsafePointer(to=field)

            comptime
            if type_name == STRING_NAME:
                stmt.bind_text(param_idx, ptr.bitcast[String]()[])
            elif type_name == INT_NAME:
                stmt.bind_int(param_idx, ptr.bitcast[Int]()[])
            elif type_name == INT64_NAME:
                stmt.bind_int(param_idx, Int(ptr.bitcast[Int64]()[]))
            elif type_name == FLOAT64_NAME:
                stmt.bind_float(param_idx, ptr.bitcast[Float64]()[])
            elif type_name == FLOAT32_NAME:
                stmt.bind_float(
                    param_idx, Float64(ptr.bitcast[Float32]()[])
                )
            elif type_name == BOOL_NAME:
                stmt.bind_int(param_idx, 1 if ptr.bitcast[Bool]()[] else 0)
            elif type_name == OPT_INT_NAME:
                var opt = ptr.bitcast[Optional[Int]]()[]
                if opt:
                    stmt.bind_int(param_idx, opt.value())
                else:
                    stmt.bind_null(param_idx)
            elif type_name == OPT_STRING_NAME:
                var opt = ptr.bitcast[Optional[String]]()[]
                if opt:
                    stmt.bind_text(param_idx, opt.value())
                else:
                    stmt.bind_null(param_idx)
            elif type_name == OPT_FLOAT64_NAME:
                var opt = ptr.bitcast[Optional[Float64]]()[]
                if opt:
                    stmt.bind_float(param_idx, opt.value())
                else:
                    stmt.bind_null(param_idx)
            elif type_name == OPT_BOOL_NAME:
                var opt = ptr.bitcast[Optional[Bool]]()[]
                if opt:
                    stmt.bind_int(param_idx, 1 if opt.value() else 0)
                else:
                    stmt.bind_null(param_idx)

            param_idx += 1

    _ = stmt.step()


def query[T: Morphable & Copyable](
    db: Database, table: String, where: String = ""
) raises -> List[T]:
    """Execute ``SELECT * FROM table [WHERE ...]`` and return ``List[T]``.

    Each result row is decoded into a fresh ``T()`` instance using the same
    ``UnsafePointer`` mutation pattern as ``envo/loader.mojo``.

    Parameters:
        T: A ``Morphable`` struct type.

    Args:
        db:    Open database connection.
        table: Source table name.
        where: Optional ``WHERE`` clause text, e.g. ``"age > 18"``.
               Do not include the ``WHERE`` keyword; it is prepended
               automatically.

    Returns:
        List of ``T`` instances, one per result row.

    Raises:
        Error: If statement preparation or stepping fails.
    """
    comptime count = struct_field_count[T]()
    comptime names = struct_field_names[T]()
    comptime types = struct_field_types[T]()

    var sql = "SELECT * FROM " + table
    if where != "":
        sql += " WHERE " + where

    var stmt = db.prepare(sql)
    var results = List[T]()

    while True:
        var maybe_row = stmt.step()
        if not maybe_row:
            break
        ref row = maybe_row.value()

        var item = T()
        var col_idx = 0

        comptime
        for idx in range(count):
            comptime field_type = types[idx]
            comptime type_name = get_type_name[field_type]()
            comptime sql_type = _sql_type_for[field_type]()

            if sql_type != "":
                ref field = trait_downcast[_Base](__struct_field_ref(idx, item))
                var ptr = UnsafePointer(to=field)

                if row.is_null(col_idx):
                    comptime
                    if type_name == OPT_INT_NAME:
                        ptr.destroy_pointee()
                        ptr.bitcast[Optional[Int]]().init_pointee_move(None)
                    elif type_name == OPT_STRING_NAME:
                        ptr.destroy_pointee()
                        ptr.bitcast[Optional[String]]().init_pointee_move(None)
                    elif type_name == OPT_FLOAT64_NAME:
                        ptr.destroy_pointee()
                        ptr.bitcast[Optional[Float64]]().init_pointee_move(None)
                    elif type_name == OPT_BOOL_NAME:
                        ptr.destroy_pointee()
                        ptr.bitcast[Optional[Bool]]().init_pointee_move(None)
                else:
                    comptime
                    if type_name == STRING_NAME:
                        ptr.destroy_pointee()
                        ptr.bitcast[String]().init_pointee_move(
                            row.text_val(col_idx)
                        )
                    elif type_name == INT_NAME:
                        ptr.destroy_pointee()
                        ptr.bitcast[Int]().init_pointee_move(
                            row.int_val(col_idx)
                        )
                    elif type_name == INT64_NAME:
                        ptr.destroy_pointee()
                        ptr.bitcast[Int64]().init_pointee_move(
                            Int64(row.int_val(col_idx))
                        )
                    elif type_name == FLOAT64_NAME:
                        ptr.destroy_pointee()
                        ptr.bitcast[Float64]().init_pointee_move(
                            row.float_val(col_idx)
                        )
                    elif type_name == FLOAT32_NAME:
                        ptr.destroy_pointee()
                        ptr.bitcast[Float32]().init_pointee_move(
                            Float32(row.float_val(col_idx))
                        )
                    elif type_name == BOOL_NAME:
                        ptr.destroy_pointee()
                        ptr.bitcast[Bool]().init_pointee_move(
                            row.int_val(col_idx) != 0
                        )
                    elif type_name == OPT_INT_NAME:
                        ptr.destroy_pointee()
                        ptr.bitcast[Optional[Int]]().init_pointee_move(
                            row.int_val(col_idx)
                        )
                    elif type_name == OPT_STRING_NAME:
                        ptr.destroy_pointee()
                        ptr.bitcast[Optional[String]]().init_pointee_move(
                            row.text_val(col_idx)
                        )
                    elif type_name == OPT_FLOAT64_NAME:
                        ptr.destroy_pointee()
                        ptr.bitcast[Optional[Float64]]().init_pointee_move(
                            row.float_val(col_idx)
                        )
                    elif type_name == OPT_BOOL_NAME:
                        ptr.destroy_pointee()
                        ptr.bitcast[Optional[Bool]]().init_pointee_move(
                            row.int_val(col_idx) != 0
                        )

                col_idx += 1

        results.append(item^)

    return results^


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


@always_inline
def _sql_type_for[T: AnyType]() -> StaticString:
    """Return the SQLite column type string for a Mojo type, or empty string.

    Parameters:
        T: The Mojo field type to classify.

    Returns:
        ``"TEXT"``, ``"INTEGER"``, ``"REAL"``, or ``""`` (unsupported/skip).
    """
    comptime type_name = get_type_name[T]()

    comptime
    if type_name == STRING_NAME or type_name == OPT_STRING_NAME:
        return "TEXT"
    elif (
        type_name == INT_NAME
        or type_name == INT64_NAME
        or type_name == BOOL_NAME
        or type_name == OPT_INT_NAME
        or type_name == OPT_BOOL_NAME
    ):
        return "INTEGER"
    elif (
        type_name == FLOAT64_NAME
        or type_name == FLOAT32_NAME
        or type_name == OPT_FLOAT64_NAME
    ):
        return "REAL"
    else:
        return ""
