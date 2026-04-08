"""Example 6 — Contacts mini-application.

A realistic, self-contained CRUD demo that combines everything:

- ORM ``create_table`` / ``insert`` / ``query`` for high-level access.
- Raw prepared statements for UPDATE and DELETE (not yet in the ORM layer).
- ``Optional`` fields for nullable columns.
- ``WHERE`` filtering via the ``query`` helper.
- Transaction wrapping with ``BEGIN`` / ``COMMIT`` for bulk writes.

The application manages a simple contacts list: add contacts, update a phone
number, delete a contact, then print a filtered directory.
"""

from mosqlite.db import Database
from mosqlite.orm import create_table, insert, query


# ---------------------------------------------------------------------------
# Domain struct
# ---------------------------------------------------------------------------


@fieldwise_init
struct Contact(Defaultable, Movable, Copyable):
    """One entry in a contacts list.

    Fields:
        id:      Surrogate integer key (set at insert time).
        name:    Full display name.
        email:   E-mail address.
        phone:   Phone number (optional).
        active:  False when the contact has been soft-deleted.
    """

    var id:     Int
    var name:   String
    var email:  String
    var phone:  Optional[String]
    var active: Bool

    def __init__(out self):
        self.id     = 0
        self.name   = ""
        self.email  = ""
        self.phone  = None
        self.active = True

    def __init__(out self, *, copy: Self):
        self.id     = copy.id
        self.name   = copy.name
        self.email  = copy.email
        self.phone  = copy.phone
        self.active = copy.active


# ---------------------------------------------------------------------------
# Helper: print a contact line
# ---------------------------------------------------------------------------


def _print_contact(c: Contact):
    var phone = c.phone.value() if c.phone else "(none)"
    var status = "active" if c.active else "inactive"
    print(
        "  [" + String(c.id) + "]",
        c.name,
        "<" + c.email + ">",
        "| phone:", phone,
        "| status:", status,
    )


# ---------------------------------------------------------------------------
# Application helpers
# ---------------------------------------------------------------------------


def _seed_contacts(db: Database) raises:
    """Insert initial contacts inside a transaction for atomicity."""
    db.execute("BEGIN")

    var contacts = List[Contact]()
    contacts.append(Contact(
        id=1, name="Alice Nguyen", email="alice@example.com",
        phone=Optional[String]("+1-415-555-0101"), active=True,
    ))
    contacts.append(Contact(
        id=2, name="Bob Martínez", email="bob@example.com",
        phone=None, active=True,
    ))
    contacts.append(Contact(
        id=3, name="Carol Smith", email="carol@example.com",
        phone=Optional[String]("+44-20-7946-0958"), active=True,
    ))
    contacts.append(Contact(
        id=4, name="Dave Kim", email="dave@example.com",
        phone=Optional[String]("+82-2-555-0199"), active=True,
    ))

    for i in range(len(contacts)):
        insert[Contact](db, "contacts", contacts[i])

    db.execute("COMMIT")


def _update_phone(db: Database, contact_id: Int, new_phone: String) raises:
    """Set a new phone number for a contact by id (raw prepared statement)."""
    var stmt = db.prepare(
        "UPDATE contacts SET phone = ? WHERE id = ?"
    )
    stmt.bind_text(1, new_phone)
    stmt.bind_int(2, contact_id)
    _ = stmt.step()


def _deactivate(db: Database, contact_id: Int) raises:
    """Soft-delete: mark a contact inactive rather than removing the row."""
    var stmt = db.prepare(
        "UPDATE contacts SET active = 0 WHERE id = ?"
    )
    stmt.bind_int(1, contact_id)
    _ = stmt.step()


def _count_active(db: Database) raises -> Int:
    """Return the count of active contacts via a raw aggregation query."""
    var stmt = db.prepare(
        "SELECT COUNT(*) FROM contacts WHERE active = 1"
    )
    var maybe_row = stmt.step()
    if not maybe_row:
        return 0
    ref row = maybe_row.value()
    return row.int_val(0)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() raises:
    var db = Database(":memory:")
    create_table[Contact](db, "contacts")

    # -----------------------------------------------------------------------
    # 1. Seed initial data.
    # -----------------------------------------------------------------------
    _seed_contacts(db)
    print("After seeding:")
    var all = query[Contact](db, "contacts")
    for i in range(len(all)):
        _print_contact(all[i])
    print()

    # -----------------------------------------------------------------------
    # 2. Update Bob's phone number.
    # -----------------------------------------------------------------------
    _update_phone(db, 2, "+1-212-555-0188")
    print("After updating Bob's phone:")
    var bob_rows = query[Contact](db, "contacts", where="id = 2")
    _print_contact(bob_rows[0])
    print()

    # -----------------------------------------------------------------------
    # 3. Soft-delete Dave.
    # -----------------------------------------------------------------------
    _deactivate(db, 4)
    print("After deactivating Dave:")
    print("  Active contact count:", _count_active(db))
    print()

    # -----------------------------------------------------------------------
    # 4. Print active contacts only.
    # -----------------------------------------------------------------------
    print("Active contacts:")
    var active = query[Contact](db, "contacts", where="active = 1")
    for i in range(len(active)):
        _print_contact(active[i])
    print()

    # -----------------------------------------------------------------------
    # 5. Contacts whose phone is still unknown.
    # -----------------------------------------------------------------------
    print("Contacts without a phone number:")
    var no_phone = query[Contact](
        db, "contacts", where="phone IS NULL AND active = 1"
    )
    if len(no_phone) == 0:
        print("  (none)")
    else:
        for i in range(len(no_phone)):
            _print_contact(no_phone[i])
    print()

    print("Done.")
