#!/usr/bin/env python

import json
import logging
import sys
from pathlib import Path

import psycopg2 as dbapi
from beancount import loader
from beancount.core import data
from beancount.parser import version
from beancount.utils import misc_utils
from psycopg2.extensions import parse_dsn
from psycopg2.extras import execute_batch

account_map: dict[str, int] = {}
document_path: Path


def get_amount(amount):
    return (amount.number, amount.currency) if amount is not None else None


def get_meta_json(meta):
    keys_to_remove = {"filename", "lineno"}
    filtered_meta = {
        key: value for key, value in meta.items() if key not in keys_to_remove
    }
    return json.dumps(filtered_meta)


def build_account_map(cursor):
    """Build a map of account names to IDs from the database."""
    cursor.execute("SELECT id, name FROM account")
    account_map.clear()
    for row in cursor.fetchall():
        account_map[row[1]] = row[0]


def update_accounts(cursor, entries):
    """Update or insert accounts from Open/Close directives."""
    logging.info("Updating accounts...")
    updated_count = 0
    inserted_count = 0

    # Process Open directives
    for entry in entries:
        if isinstance(entry, data.Open):
            meta = get_meta_json(entry.meta)
            currencies = entry.currencies

            if entry.account in account_map:
                # Account exists, check if we need to update
                account_id = account_map[entry.account]
                cursor.execute(
                    """
                    SELECT open_date, currencies, meta FROM account WHERE id = %s
                    """,
                    (account_id,),
                )
                existing = cursor.fetchone()
                if existing:
                    existing_date, existing_currencies, existing_meta = existing
                    # Update if any field changed
                    if (
                        entry.date != existing_date
                        or currencies != existing_currencies
                        or meta != existing_meta
                    ):
                        cursor.execute(
                            """
                            UPDATE account SET open_date = %s, currencies = %s, meta = %s
                            WHERE id = %s
                            """,
                            (entry.date, currencies, meta, account_id),
                        )
                        updated_count += 1
            else:
                # New account, insert it
                new_id = max(account_map.values()) + 1 if account_map else 1
                cursor.execute(
                    """
                    INSERT INTO account (id, name, open_date, currencies, meta)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (new_id, entry.account, entry.date, currencies, meta),
                )
                account_map[entry.account] = new_id
                inserted_count += 1

    # Process Close directives
    for entry in entries:
        if isinstance(entry, data.Close):
            if entry.account in account_map:
                account_id = account_map[entry.account]
                cursor.execute(
                    "SELECT close_date FROM account WHERE id = %s", (account_id,)
                )
                existing = cursor.fetchone()
                if existing and existing[0] != entry.date:
                    cursor.execute(
                        "UPDATE account SET close_date = %s WHERE id = %s",
                        (entry.date, account_id),
                    )
                    updated_count += 1
                elif not existing[0]:
                    cursor.execute(
                        "UPDATE account SET close_date = %s WHERE id = %s",
                        (entry.date, account_id),
                    )
                    updated_count += 1

    logging.info(f"  Inserted: {inserted_count}, Updated: {updated_count}")


def update_transactions(cursor, entries):
    """Update or insert transactions and postings."""
    logging.info("Updating transactions...")
    updated_count = 0
    inserted_count = 0

    # Get existing transaction IDs to avoid conflicts
    cursor.execute("SELECT MAX(id) FROM transaction")
    result = cursor.fetchone()
    max_id = (result[0] or 0) if result else 0

    for eid, entry in enumerate(entries, start=max_id + 1):
        if isinstance(entry, data.Transaction):
            # Check if transaction already exists by matching date, payee, narration, and postings
            cursor.execute(
                """
                SELECT id FROM transaction WHERE date = %s AND payee = %s AND narration = %s
                LIMIT 1
                """,
                (
                    entry.date,
                    entry.payee or "",
                    entry.narration or "",
                ),
            )
            existing_txn = cursor.fetchone()

            if existing_txn:
                txn_id = existing_txn[0]
                # Update transaction metadata
                cursor.execute(
                    """
                    UPDATE transaction SET flag = %s, tags = %s, links = %s
                    WHERE id = %s
                    """,
                    (entry.flag or "", list(entry.tags), list(entry.links), txn_id),
                )
                updated_count += 1

                # Update postings - delete old ones and insert new ones for this transaction
                cursor.execute(
                    "DELETE FROM posting WHERE transaction_id = %s", (txn_id,)
                )
            else:
                # Insert new transaction
                cursor.execute(
                    """
                    INSERT INTO transaction (id, flag, payee, narration, tags, links)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        eid,
                        entry.flag or "",
                        entry.payee or "",
                        entry.narration or "",
                        list(entry.tags),
                        list(entry.links),
                    ),
                )
                txn_id = eid
                inserted_count += 1

            # Insert postings for this transaction
            posting_values = []
            for posting in entry.postings:
                amount = get_amount(posting.units)
                cost = get_amount(posting.cost)
                cost_date = posting.cost.date if cost else None
                cost_label = posting.cost.label if cost else None
                price = get_amount(posting.price)
                account_id = account_map.get(posting.account)

                if account_id is None:
                    logging.warning(
                        f"  Account {posting.account} not found for posting in transaction {txn_id}"
                    )
                    continue

                posting_values.append(
                    (
                        entry.date,
                        account_id,
                        txn_id,
                        posting.flag,
                        amount,
                        price,
                        cost,
                        cost_date,
                        cost_label,
                    )
                )

            if posting_values:
                execute_batch(
                    cursor,
                    """
                    INSERT INTO posting (date, account_id, transaction_id, flag, amount, price, cost, cost_date, cost_label)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    posting_values,
                )

    # Re-match lots after updating transactions
    cursor.execute(
        """
        WITH augmentations AS (
            SELECT * FROM posting WHERE (amount).number > 0
        )
        UPDATE posting
        SET matching_lot_id = (
            SELECT id FROM augmentations
            WHERE (augmentations.cost = posting.cost
                OR augmentations.cost_date = posting.cost_date
                OR augmentations.cost_label = posting.cost_label)
                AND augmentations.id != posting.id
            LIMIT 1
        )
        WHERE (amount).number < 0
        """
    )

    logging.info(f"  Inserted: {inserted_count}, Updated: {updated_count}")


def update_balances(cursor, entries):
    """Update or insert balance assertions."""
    logging.info("Updating balances...")
    updated_count = 0
    inserted_count = 0

    cursor.execute("SELECT MAX(id) FROM assertion")
    result = cursor.fetchone()
    max_id = (result[0] or 0) if result else 0

    for eid, entry in enumerate(entries, start=max_id + 1):
        if isinstance(entry, data.Balance):
            account_id = account_map.get(entry.account)
            if account_id is None:
                logging.warning(
                    f"  Account {entry.account} not found for balance assertion"
                )
                continue

            amount = get_amount(entry.amount)

            # Check if assertion already exists
            cursor.execute(
                """
                SELECT id FROM assertion WHERE date = %s AND account_id = %s
                LIMIT 1
                """,
                (entry.date, account_id),
            )
            existing = cursor.fetchone()

            if existing:
                assertion_id = existing[0]
                cursor.execute(
                    """
                    SELECT amount FROM assertion WHERE id = %s
                    """,
                    (assertion_id,),
                )
                existing_amount = cursor.fetchone()[0]
                if amount != existing_amount:
                    cursor.execute(
                        """
                        UPDATE assertion SET amount = %s WHERE id = %s
                        """,
                        (amount, assertion_id),
                    )
                    updated_count += 1
            else:
                cursor.execute(
                    """
                    INSERT INTO assertion (id, date, account_id, amount)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (eid, entry.date, account_id, amount),
                )
                inserted_count += 1

    logging.info(f"  Inserted: {inserted_count}, Updated: {updated_count}")


def update_prices(cursor, entries):
    """Update or insert prices."""
    logging.info("Updating prices...")
    updated_count = 0
    inserted_count = 0

    cursor.execute("SELECT MAX(id) FROM price")
    result = cursor.fetchone()
    max_id = (result[0] or 0) if result else 0

    for eid, entry in enumerate(entries, start=max_id + 1):
        if isinstance(entry, data.Price):
            amount = get_amount(entry.amount)

            # Check if price already exists
            cursor.execute(
                """
                SELECT id FROM price WHERE date = %s AND currency = %s AND amount = %s
                LIMIT 1
                """,
                (entry.date, entry.currency, amount),
            )
            existing = cursor.fetchone()

            if not existing:
                cursor.execute(
                    """
                    INSERT INTO price (id, date, currency, amount)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (eid, entry.date, entry.currency, amount),
                )
                inserted_count += 1

    logging.info(f"  Inserted: {inserted_count}, Updated: {updated_count}")


def update_commodities(cursor, entries):
    """Update or insert commodities."""
    logging.info("Updating commodities...")
    updated_count = 0
    inserted_count = 0

    cursor.execute("SELECT MAX(id) FROM commodity")
    result = cursor.fetchone()
    max_id = (result[0] or 0) if result else 0

    for eid, entry in enumerate(entries, start=max_id + 1):
        if isinstance(entry, data.Commodity):
            decimal_places = entry.meta.pop("decimal_places", 0)
            meta = get_meta_json(entry.meta)

            # Check if commodity already exists
            cursor.execute(
                """
                SELECT id FROM commodity WHERE currency = %s
                LIMIT 1
                """,
                (entry.currency,),
            )
            existing = cursor.fetchone()

            if existing:
                commodity_id = existing[0]
                cursor.execute(
                    """
                    SELECT date, decimal_places, meta FROM commodity WHERE id = %s
                    """,
                    (commodity_id,),
                )
                existing_data = cursor.fetchone()
                if (
                    existing_data[0] != entry.date
                    or existing_data[1] != decimal_places
                    or existing_data[2] != meta
                ):
                    cursor.execute(
                        """
                        UPDATE commodity SET date = %s, decimal_places = %s, meta = %s
                        WHERE id = %s
                        """,
                        (entry.date, decimal_places, meta, commodity_id),
                    )
                    updated_count += 1
            else:
                cursor.execute(
                    """
                    INSERT INTO commodity (id, date, currency, decimal_places, meta)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (eid, entry.date, entry.currency, decimal_places, meta),
                )
                inserted_count += 1

    logging.info(f"  Inserted: {inserted_count}, Updated: {updated_count}")


def update_documents(cursor, entries):
    """Update or insert documents."""
    if document_path is None:
        logging.info("No documents directory configured, skipping documents")
        return

    logging.info("Updating documents...")
    updated_count = 0
    inserted_count = 0

    def read_data(filename):
        """Reads the content of the file specified by `filename` in binary mode."""
        with open(filename, "rb") as file:
            return file.read()

    cursor.execute("SELECT MAX(id) FROM document")
    result = cursor.fetchone()
    max_id = (result[0] or 0) if result else 0

    for eid, entry in enumerate(entries, start=max_id + 1):
        if isinstance(entry, data.Document):
            account_id = account_map.get(entry.account)
            if account_id is None:
                logging.warning(f"  Account {entry.account} not found for document")
                continue

            filename = str(Path(entry.filename).relative_to(document_path))

            # Check if the document already exists in the database
            cursor.execute(
                """
                SELECT id FROM document WHERE date = %s AND filename = %s LIMIT 1
                """,
                (entry.date, filename),
            )
            existing = cursor.fetchone()

            if existing:
                # Document exists, update account_id if changed
                cursor.execute(
                    """
                    SELECT account_id FROM document WHERE id = %s
                    """,
                    (existing[0],),
                )
                existing_account_id = cursor.fetchone()[0]
                if existing_account_id != account_id:
                    cursor.execute(
                        """
                        UPDATE document SET account_id = %s WHERE id = %s
                        """,
                        (account_id, existing[0]),
                    )
                    updated_count += 1
            else:
                # New document, insert it
                file_data = read_data(entry.filename)
                cursor.execute(
                    """
                    INSERT INTO document (id, date, account_id, filename, data)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (eid, entry.date, account_id, str(filename), file_data),
                )
                inserted_count += 1

    logging.info(f"  Inserted: {inserted_count}, Updated: {updated_count}")


def main():
    global document_path

    parser = version.ArgumentParser(
        description="Update a Beanpost database with data from a Beancount file. "
        "Checks for new values and updates existing ones if they change."
    )
    parser.add_argument("filename", help="Beancount input filename")
    parser.add_argument("database", help="PostgreSQL connection string")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)-8s: %(message)s")

    logging.info("Loading Beancount file...")
    entries, errors, options_map = loader.load_file(
        args.filename, log_timings=logging.info, log_errors=sys.stderr
    )

    if len(options_map["documents"]) > 0:
        document_path = Path(args.filename).parent / options_map["documents"][0]
    else:
        document_path = None

    dsn = parse_dsn(args.database)
    connection = dbapi.connect(**dsn)
    cursor = connection.cursor()

    try:
        logging.info("Building account map from database...")
        build_account_map(cursor)

        for function in [
            update_accounts,
            update_transactions,
            update_balances,
            update_prices,
            update_commodities,
            update_documents,
        ]:
            step_name = getattr(function, "__name__", function.__class__.__name__)
            with misc_utils.log_time(step_name, logging.info):
                function(cursor, entries)

        connection.commit()
        logging.info("Update completed successfully")
    except Exception as e:
        connection.rollback()
        logging.error(f"Error during update: {e}")
        raise
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()
