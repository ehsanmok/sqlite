"""Example 5 — Optional fields and WHERE filtering.

``Optional[T]`` fields map to nullable SQLite columns:

- ``None``    → NULL (``bind_null``).
- ``Some(v)`` → the inner value bound normally.

On read-back, a NULL column becomes ``None``; a non-NULL column becomes
``Some(decoded_value)``.

This example also shows the ``where`` parameter of ``query``, which appends a
raw ``WHERE <clause>`` to the generated ``SELECT *``.  Pass only the predicate
expression — mosqlite inserts the ``WHERE`` keyword automatically.
"""

from mosqlite.db import Database
from mosqlite.orm import create_table, insert, query


# ---------------------------------------------------------------------------
# Domain structs
# ---------------------------------------------------------------------------


@fieldwise_init
struct Employee(Defaultable, Movable, Copyable):
    """A company employee, some fields optional.

    Fields:
        name:       Full name (required).
        department: Department name (required).
        salary:     Annual salary in USD (optional — contractors may lack one).
        manager:    Manager's name (optional — the CEO has none).
        remote:     True when the employee works remotely (optional).
    """

    var name:       String
    var department: String
    var salary:     Optional[Float64]
    var manager:    Optional[String]
    var remote:     Optional[Bool]

    def __init__(out self):
        self.name       = ""
        self.department = ""
        self.salary     = None
        self.manager    = None
        self.remote     = None

    def __init__(out self, *, copy: Self):
        self.name       = copy.name
        self.department = copy.department
        self.salary     = copy.salary
        self.manager    = copy.manager
        self.remote     = copy.remote


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _opt_float_str(v: Optional[Float64]) -> String:
    if v:
        return String(v.value())
    return "NULL"


def _opt_str(v: Optional[String]) -> String:
    if v:
        return v.value()
    return "NULL"


def _opt_bool_str(v: Optional[Bool]) -> String:
    if not v:
        return "NULL"
    return "true" if v.value() else "false"


def _print_employee(e: Employee):
    print(
        " •", e.name,
        "| dept:", e.department,
        "| salary:", _opt_float_str(e.salary),
        "| manager:", _opt_str(e.manager),
        "| remote:", _opt_bool_str(e.remote),
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() raises:
    var db = Database(":memory:")
    create_table[Employee](db, "employees")

    # CEO — no manager, no remote flag (works in-person by definition).
    insert[Employee](
        db, "employees",
        Employee(
            name="Dana",
            department="Executive",
            salary=Optional[Float64](250_000.0),
            manager=None,
            remote=None,
        ),
    )

    # Full-time engineer — all fields present.
    insert[Employee](
        db, "employees",
        Employee(
            name="Alice",
            department="Engineering",
            salary=Optional[Float64](120_000.0),
            manager=Optional[String]("Dana"),
            remote=Optional[Bool](True),
        ),
    )

    # Contractor — no salary on record (paid hourly outside the system).
    insert[Employee](
        db, "employees",
        Employee(
            name="Bob",
            department="Engineering",
            salary=None,
            manager=Optional[String]("Alice"),
            remote=Optional[Bool](False),
        ),
    )

    # Designer — salaried, in-office.
    insert[Employee](
        db, "employees",
        Employee(
            name="Carol",
            department="Design",
            salary=Optional[Float64](95_000.0),
            manager=Optional[String]("Dana"),
            remote=Optional[Bool](False),
        ),
    )

    # -----------------------------------------------------------------------
    # Query all employees.
    # -----------------------------------------------------------------------
    print("All employees:")
    var all_emps = query[Employee](db, "employees")
    for i in range(len(all_emps)):
        _print_employee(all_emps[i])

    print()

    # -----------------------------------------------------------------------
    # Filter with WHERE — only Engineering department.
    # -----------------------------------------------------------------------
    print("Engineering team:")
    var eng = query[Employee](db, "employees", where="department = 'Engineering'")
    for i in range(len(eng)):
        _print_employee(eng[i])

    print()

    # -----------------------------------------------------------------------
    # Only employees whose salary is known (not NULL).
    # -----------------------------------------------------------------------
    print("Salaried employees:")
    var salaried = query[Employee](db, "employees", where="salary IS NOT NULL")
    for i in range(len(salaried)):
        _print_employee(salaried[i])

    print()
    print("Done.")
