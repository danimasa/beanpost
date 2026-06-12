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


def truncate_all(cursor):
    """Truncate all data tables to prepare for a full re-import.

    Uses TRUNCATE ... CASCADE to handle foreign key dependencies.
    The order doesn't matter with CASCADE, but we truncate child tables
    first for clarity.
    """
    logging.info("Truncating all tables for full sync...")
    cursor.execute("""
        TRUNCATE posting, assertion, document, price, commodity, transaction, account
        RESTART IDENTITY CASCADE
    """)
    logging.info("  All tables truncated.")


def insert_accounts(cursor, entries):
    """Insert all accounts from Open/Close directives."""
    logging.info("Inserting accounts...")
    inserted_count = 0

    # Collect Open directives
    open_entries = {}
    for entry in entries:
        if isinstance(entry, data.Open):
            open_entries[entry.account] = entry

    # Collect Close directives
    close_dates = {}
    for entry in entries:
        if isinstance(entry, data.Close):
            close_dates[entry.account] = entry.date

    # Insert all accounts
    for account_name, entry in open_entries.items():
        meta = get_meta_json(entry.meta)
        currencies = entry.currencies
        close_date = close_dates.get(account_name)

        cursor.execute(
            """
            INSERT INTO account (name, open_date, close_date, currencies, meta)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (account_name, entry.date, close_date, currencies, meta),
        )
        account_id = cursor.fetchone()[0]
        account_map[account_name] = account_id
        inserted_count += 1

    logging.info(f"  Inserted: {inserted_count}")


def insert_transactions(cursor, entries):
    """Insert all transactions and their postings."""
    logging.info("Inserting transactions...")
    txn_count = 0
    posting_count = 0

    for entry in entries:
        if isinstance(entry, data.Transaction):
            cursor.execute(
                """
                INSERT INTO transaction (flag, payee, narration, tags, links)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    entry.flag or "",
                    entry.payee or "",
                    entry.narration or "",
                    str(sorted(entry.tags)) if entry.tags else "",
                    str(sorted(entry.links)) if entry.links else "",
                ),
            )
            txn_id = cursor.fetchone()[0]
            txn_count += 1

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
                posting_count += len(posting_values)

    # Match lots after all postings are inserted
    cursor.execute("""
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
        """)

    logging.info(f"  Inserted: {txn_count} transactions, {posting_count} postings")


def insert_balances(cursor, entries):
    """Insert all balance assertions."""
    logging.info("Inserting balance assertions...")
    inserted_count = 0

    for entry in entries:
        if isinstance(entry, data.Balance):
            account_id = account_map.get(entry.account)
            if account_id is None:
                logging.warning(
                    f"  Account {entry.account} not found for balance assertion"
                )
                continue

            amount = get_amount(entry.amount)
            cursor.execute(
                """
                INSERT INTO assertion (date, account_id, amount)
                VALUES (%s, %s, %s)
                """,
                (entry.date, account_id, amount),
            )
            inserted_count += 1

    logging.info(f"  Inserted: {inserted_count}")


def insert_prices(cursor, entries):
    """Insert all prices."""
    logging.info("Inserting prices...")
    inserted_count = 0

    for entry in entries:
        if isinstance(entry, data.Price):
            amount = get_amount(entry.amount)
            cursor.execute(
                """
                INSERT INTO price (date, currency, amount)
                VALUES (%s, %s, %s)
                """,
                (entry.date, entry.currency, amount),
            )
            inserted_count += 1

    logging.info(f"  Inserted: {inserted_count}")


def insert_commodities(cursor, entries):
    """Insert all commodities."""
    logging.info("Inserting commodities...")
    inserted_count = 0

    for entry in entries:
        if isinstance(entry, data.Commodity):
            decimal_places = entry.meta.pop("decimal_places", 0)
            meta = get_meta_json(entry.meta)
            cursor.execute(
                """
                INSERT INTO commodity (date, currency, decimal_places, meta)
                VALUES (%s, %s, %s, %s)
                """,
                (entry.date, entry.currency, decimal_places, meta),
            )
            inserted_count += 1

    logging.info(f"  Inserted: {inserted_count}")


def insert_documents(cursor, entries):
    """Insert all documents."""
    if document_path is None:
        logging.info("No documents directory configured, skipping documents")
        return

    logging.info("Inserting documents...")
    inserted_count = 0

    def read_data(filename):
        """Reads the content of the file specified by `filename` in binary mode."""
        with open(filename, "rb") as file:
            return file.read()

    for entry in entries:
        if isinstance(entry, data.Document):
            account_id = account_map.get(entry.account)
            if account_id is None:
                logging.warning(f"  Account {entry.account} not found for document")
                continue

            filename = str(Path(entry.filename).relative_to(document_path))
            file_data = read_data(entry.filename)
            cursor.execute(
                """
                INSERT INTO document (date, account_id, filename, data)
                VALUES (%s, %s, %s, %s)
                """,
                (entry.date, account_id, filename, file_data),
            )
            inserted_count += 1

    logging.info(f"  Inserted: {inserted_count}")


def main():
    global document_path

    parser = version.ArgumentParser(
        description="Sync a Beanpost database with data from a Beancount file. "
        "Replaces all database content to match the Beancount file exactly."
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
        # Truncate everything first for a clean sync
        with misc_utils.log_time("truncate_all", logging.info):
            truncate_all(cursor)

        # Insert everything from the beancount file
        for function in [
            insert_accounts,
            insert_transactions,
            insert_balances,
            insert_prices,
            insert_commodities,
            insert_documents,
        ]:
            step_name = getattr(function, "__name__", function.__class__.__name__)
            with misc_utils.log_time(step_name, logging.info):
                function(cursor, entries)

        connection.commit()
        logging.info("Sync completed successfully")
    except Exception as e:
        connection.rollback()
        logging.error(f"Error during sync: {e}")
        raise
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()
