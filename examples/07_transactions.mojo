"""Transactions — atomic multi-statement operations with mosqlite.

Demonstrates:
- ``db.transaction()`` — returns an RAII ``Transaction`` guard
- ``tx.commit()``   — persist all changes atomically
- ``tx.rollback()`` — revert all changes (called in ``except`` handler)
- ``_ = tx^``       — force immediate destruction → ROLLBACK (no raise needed)
- Verifying that a failed batch leaves data unchanged

**Idiomatic pattern**::

    var tx = db.transaction()   # BEGIN
    try:
        db.execute(...)
        db.execute(...)
        tx.commit()             # COMMIT on success
    except e:
        tx.rollback()           # ROLLBACK on any error
        raise e                 # re-raise to caller (optional)
"""

from mosqlite import Database, Transaction


# -------------------------------------------------------------------------
# Schema helpers
# -------------------------------------------------------------------------


def setup_accounts(mut db: Database) raises:
    """Create and seed an ``accounts`` table."""
    db.execute("""
        CREATE TABLE accounts (
            id      INTEGER PRIMARY KEY,
            name    TEXT    NOT NULL,
            balance INTEGER NOT NULL
        )
    """)
    db.execute("INSERT INTO accounts VALUES (1, 'Alice', 1000)")
    db.execute("INSERT INTO accounts VALUES (2, 'Bob',    500)")


def show_balances(db: Database) raises:
    """Print id, name, balance for every account."""
    var q = db.prepare("SELECT id, name, balance FROM accounts ORDER BY id")
    while True:
        var row = q.step()
        if not row:
            break
        ref r = row.value()
        print(
            "  id=" + String(r.int_val(0))
            + "  name=" + r.text_val(1)
            + "  balance=" + String(r.int_val(2))
        )


def get_balance(db: Database, account_id: Int) raises -> Int:
    """Return the current balance for ``account_id``."""
    var q = db.prepare(
        "SELECT balance FROM accounts WHERE id = " + String(account_id)
    )
    var row = q.step()
    if not row:
        raise Error("account " + String(account_id) + " not found")
    return row.value().int_val(0)


# -------------------------------------------------------------------------
# Business logic using transactions
# -------------------------------------------------------------------------


def transfer(mut db: Database, from_id: Int, to_id: Int, amount: Int) raises:
    """Transfer ``amount`` from account ``from_id`` to ``to_id`` atomically.

    Both UPDATE statements are wrapped in a single transaction.  If either
    fails, the entire transfer is rolled back by calling ``tx.rollback()``
    in the ``except`` handler.

    Args:
        db:      Open database connection.
        from_id: Source account ID.
        to_id:   Destination account ID.
        amount:  Amount to transfer (must be positive).

    Raises:
        Error: If the source account has insufficient funds, or if either
               UPDATE fails for any reason.
    """
    if amount <= 0:
        raise Error("transfer amount must be positive")

    var from_balance = get_balance(db, from_id)
    if from_balance < amount:
        raise Error(
            "insufficient funds: balance=" + String(from_balance)
            + " < amount=" + String(amount)
        )

    var tx = db.transaction()   # BEGIN
    try:
        db.execute(
            "UPDATE accounts SET balance = balance - "
            + String(amount) + " WHERE id = " + String(from_id)
        )
        db.execute(
            "UPDATE accounts SET balance = balance + "
            + String(amount) + " WHERE id = " + String(to_id)
        )
        tx.commit()             # COMMIT — both rows updated atomically
    except e:
        tx.rollback()           # ROLLBACK — neither row is changed
        raise e.copy()


# -------------------------------------------------------------------------
# main
# -------------------------------------------------------------------------


def main() raises:
    var db = Database(":memory:")
    setup_accounts(db)

    # ------------------------------------------------------------------
    # 1. Successful transfer
    # ------------------------------------------------------------------
    print("=== Balances before transfer ===")
    show_balances(db)

    transfer(db, from_id=1, to_id=2, amount=200)

    print("\n=== Balances after $200 transfer from Alice → Bob ===")
    show_balances(db)

    # ------------------------------------------------------------------
    # 2. Failed transfer — insufficient funds, no changes applied
    # ------------------------------------------------------------------
    print("\n=== Attempting to over-draft Alice (balance=800, amount=1000) ===")
    try:
        transfer(db, from_id=1, to_id=2, amount=1000)
    except e:
        print("  Transfer failed (expected): " + String(e))

    print("\n=== Balances after failed transfer (unchanged) ===")
    show_balances(db)

    # ------------------------------------------------------------------
    # 3. Explicit destruction pattern (no exception needed)
    # ------------------------------------------------------------------
    print("\n=== Explicit cancellation via `_ = tx^` ===")
    var tx = db.transaction()           # BEGIN
    db.execute("UPDATE accounts SET balance = 0 WHERE id = 1")  # zeroes Alice
    _ = tx^                             # consume guard → immediate ROLLBACK
    print("  Alice balance after cancellation: " + String(get_balance(db, 1)))

    # ------------------------------------------------------------------
    # 4. Verify final state
    # ------------------------------------------------------------------
    print("\n=== Final balances ===")
    show_balances(db)

    var alice = get_balance(db, 1)
    var bob   = get_balance(db, 2)
    assert alice == 800, "Alice should have 800"
    assert bob   == 700, "Bob should have 700"
    assert alice + bob == 1500, "Total must be conserved"
    print("\nAll assertions passed — total balance conserved: "
          + String(alice + bob))
