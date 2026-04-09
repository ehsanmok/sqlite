"""Transactions — atomic multi-statement operations with mosqlite.

Demonstrates:
- ``with db.transaction():``  — context-manager pattern; auto-commit on clean
  exit, auto-rollback on exception (recommended)
- ``with db.transaction() as tx:``  — ``as`` form; access the guard inside the block
- ``var tx = db.transaction()`` + ``tx.commit()``/``tx.rollback()`` — manual control
- ``_ = tx^``  — force immediate destruction → ROLLBACK (no raise needed)

**Recommended pattern (context manager)**::

    with db.transaction():
        db.execute("INSERT ...")
        db.execute("INSERT ...")
    # → COMMIT on success; ROLLBACK if any statement raised

**Manual pattern (fine-grained control)**::

    var tx = db.transaction()   # BEGIN
    try:
        db.execute(...)
        db.execute(...)
        tx.commit()             # COMMIT on success
    except e:
        tx.rollback()           # ROLLBACK on any error
        raise e.copy()          # re-raise to caller
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

    Uses the context-manager pattern: ``with db.transaction():`` auto-commits
    on clean exit and auto-rolls back (re-raising) if any statement raises.

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

    with db.transaction():
        db.execute(
            "UPDATE accounts SET balance = balance - "
            + String(amount) + " WHERE id = " + String(from_id)
        )
        db.execute(
            "UPDATE accounts SET balance = balance + "
            + String(amount) + " WHERE id = " + String(to_id)
        )
    # → COMMIT on success; if either UPDATE raised, ROLLBACK + re-raise


def transfer_manual(mut db: Database, from_id: Int, to_id: Int, amount: Int) raises:
    """Same transfer, written with the manual ``var tx`` pattern for comparison.

    Demonstrates that the explicit ``try/except/rollback`` style is also
    supported — useful when you need multiple commit points or conditional
    rollback without raising.

    Args:
        db:      Open database connection.
        from_id: Source account ID.
        to_id:   Destination account ID.
        amount:  Amount to transfer (must be positive).

    Raises:
        Error: If the source has insufficient funds or any UPDATE fails.
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
        tx.rollback()           # ROLLBACK — neither row changed
        raise e.copy()


# -------------------------------------------------------------------------
# main
# -------------------------------------------------------------------------


def main() raises:
    var db = Database(":memory:")
    setup_accounts(db)

    print("=== Balances before transfer ===")
    show_balances(db)

    # ------------------------------------------------------------------
    # 1. Context-manager pattern — auto-commit on success
    # ------------------------------------------------------------------
    print("\n--- Pattern 1: with db.transaction() --- auto-commit ---")
    transfer(db, from_id=1, to_id=2, amount=200)
    print("$200 transfer Alice → Bob: committed via `with`")

    print("\n=== Balances after $200 transfer ===")
    show_balances(db)

    # ------------------------------------------------------------------
    # 2. Context-manager pattern — auto-rollback on exception
    # ------------------------------------------------------------------
    print("\n--- Pattern 2: with db.transaction() --- auto-rollback on raise ---")
    try:
        transfer(db, from_id=1, to_id=2, amount=1000)  # overdraft → raises
    except e:
        print("Transfer failed (expected): " + String(e))

    print("\n=== Balances after failed overdraft (unchanged) ===")
    show_balances(db)

    # ------------------------------------------------------------------
    # 3. Manual pattern — explicit rollback without raising
    #
    # Note: Mojo's with/__exit__ protocol requires a non-consuming __enter__,
    # so "with ... as tx:" binds tx to None.  Use "var tx" for guard access.
    # ------------------------------------------------------------------
    print("\n--- Pattern 3: var tx --- explicit rollback without raising ---")
    var tx3 = db.transaction()
    db.execute("UPDATE accounts SET balance = 999 WHERE id = 1")
    tx3.rollback()          # abort without raising; no rows change
    print("  Alice after explicit rollback: " + String(get_balance(db, 1)))

    # ------------------------------------------------------------------
    # 4. Manual pattern — var tx + explicit commit/rollback
    # ------------------------------------------------------------------
    print("\n--- Pattern 4: var tx + try/except/rollback --- manual ---")
    transfer_manual(db, from_id=2, to_id=1, amount=100)
    print("$100 transfer Bob → Alice: committed via manual tx")

    print("\n=== Balances after Bob → Alice $100 ===")
    show_balances(db)

    # ------------------------------------------------------------------
    # 5. Explicit destruction (no exception needed)
    # ------------------------------------------------------------------
    print("\n--- Pattern 5: _ = tx^ --- immediate destruction → ROLLBACK ---")
    var tx2 = db.transaction()
    db.execute("UPDATE accounts SET balance = 0 WHERE id = 1")  # zeroes Alice
    _ = tx2^                            # consume guard → immediate ROLLBACK
    print("  Alice after _ = tx^ cancellation: " + String(get_balance(db, 1)))

    # ------------------------------------------------------------------
    # 6. Verify final state
    # ------------------------------------------------------------------
    print("\n=== Final balances ===")
    show_balances(db)

    var alice = get_balance(db, 1)
    var bob   = get_balance(db, 2)
    assert alice == 900, "Alice should have 900 (800 + 100 back from Bob)"
    assert bob   == 600, "Bob should have 600 (700 - 100 to Alice)"
    assert alice + bob == 1500, "Total must be conserved"
    print("\nAll assertions passed — total balance conserved: "
          + String(alice + bob))
