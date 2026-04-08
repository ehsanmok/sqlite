"""Example 4 — ORM basics: create_table, insert, query.

The ORM layer derives SQL from struct field names and types at compile time
using morph reflection.  No schema string, no column mapping — the struct *is*
the schema.

Type mapping:

    Mojo type      │ SQLite column
    ───────────────┼──────────────
    String         │ TEXT
    Int / Int64    │ INTEGER
    Float64/Float32│ REAL
    Bool           │ INTEGER (0/1)

Structs must satisfy ``Defaultable & Movable & Copyable``:

- ``Defaultable`` — ``T()`` is called for each result row.
- ``Movable``     — rows are moved into ``List[T]``.
- ``Copyable``    — ``List[T]`` requires ``T`` to be copyable.

``@fieldwise_init`` generates an all-field constructor for free; the default
constructor and the copy constructor must be written by hand (Mojo limitation).
"""

from mosqlite.db import Database
from mosqlite.orm import create_table, insert, query


# ---------------------------------------------------------------------------
# Domain struct
# ---------------------------------------------------------------------------


@fieldwise_init
struct Book(Defaultable, Movable, Copyable):
    """A book in a library catalogue.

    Fields:
        title:  Book title.
        author: Author name.
        year:   Publication year.
        rating: Average reader rating (0.0 – 5.0).
        in_stock: Whether at least one copy is on the shelf.
    """

    var title:    String
    var author:   String
    var year:     Int
    var rating:   Float64
    var in_stock: Bool

    def __init__(out self):
        """Default-construct an empty Book."""
        self.title    = ""
        self.author   = ""
        self.year     = 0
        self.rating   = 0.0
        self.in_stock = False

    def __init__(out self, *, copy: Self):
        """Copy constructor required by ``Copyable``."""
        self.title    = copy.title
        self.author   = copy.author
        self.year     = copy.year
        self.rating   = copy.rating
        self.in_stock = copy.in_stock


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() raises:
    var db = Database(":memory:")

    # One call — mosqlite inspects Book at *compile time* and runs:
    #   CREATE TABLE IF NOT EXISTS books
    #     (title TEXT, author TEXT, year INTEGER, rating REAL, in_stock INTEGER)
    create_table[Book](db, "books")

    # Insert rows — each field is bound in declaration order.
    insert[Book](
        db, "books",
        Book(
            title="The Pragmatic Programmer",
            author="Hunt & Thomas",
            year=1999,
            rating=4.8,
            in_stock=True,
        ),
    )
    insert[Book](
        db, "books",
        Book(
            title="Clean Code",
            author="Robert C. Martin",
            year=2008,
            rating=4.3,
            in_stock=False,
        ),
    )
    insert[Book](
        db, "books",
        Book(
            title="Structure and Interpretation of Computer Programs",
            author="Abelson & Sussman",
            year=1996,
            rating=4.9,
            in_stock=True,
        ),
    )

    # Query all rows — returns List[Book]; field values restored from SQLite.
    var books = query[Book](db, "books")

    print("Library catalogue (" + String(len(books)) + " books):")
    print()

    for i in range(len(books)):
        ref b = books[i]
        var stock = "in stock" if b.in_stock else "out of stock"
        print(" •", b.title)
        print("   Author:", b.author, "| Year:", b.year)
        print("   Rating:", b.rating, "| Status:", stock)
        print()

    print("Done.")
