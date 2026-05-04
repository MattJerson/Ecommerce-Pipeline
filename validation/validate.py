import sys
import logging
import psycopg2
import pandas as pd
from datetime import datetime
import great_expectations as gx

# --- Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# --- DB Config
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "ecommerce",
    "user":     "pipeline_user",
    "password": "pipeline_pass",
}

# --- Fetch Table from PostgreSQL into DataFrame
def fetch_table(table: str) -> pd.DataFrame:
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    conn.close()
    log.info("📦  Loaded %d rows from '%s'", len(df), table)
    return df

# --- Validation Suites
def validate_raw_orders(context, df: pd.DataFrame) -> bool:
    suite_name = "raw_orders_suite"

    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

    batch_definition = (
        context.data_sources
        .add_pandas(name="orders_source")
        .add_dataframe_asset(name="orders_asset")
        .add_batch_definition_whole_dataframe(suite_name)
    )

    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    # 1. Required columns must exist
    for col in ["order_id", "user_id", "product_id", "total_amount",
                "status", "payment_method", "quantity", "category"]:
        suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column=col)
        )

    # 2. order_id must never be null
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id")
    )

    # 3. total_amount must be positive
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="total_amount", min_value=0.01, strict_min=True
        )
    )

    # 4. quantity must be between 1 and 100
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="quantity", min_value=1, max_value=100
        )
    )

    # 5. status must only contain known values
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="status",
            value_set=["pending", "confirmed", "shipped", "delivered", "cancelled"]
        )
    )

    # 6. payment_method must only contain known values
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="payment_method",
            value_set=["credit_card", "debit_card", "paypal", "gcash", "maya"]
        )
    )

    # 7. category must only contain known values
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="category",
            value_set=["Electronics", "Sports", "Home", "Books", "Fashion"]
        )
    )

    # 8. Table must have at least 1 row
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)
    )

    # 9. No duplicate order_ids
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="order_id")
    )

    result = batch.validate(suite)
    return result.success


def validate_raw_page_clicks(context, df: pd.DataFrame) -> bool:
    suite_name = "raw_page_clicks_suite"

    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

    batch_definition = (
        context.data_sources
        .add_pandas(name="clicks_source")
        .add_dataframe_asset(name="clicks_asset")
        .add_batch_definition_whole_dataframe(suite_name)
    )

    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    # 1. Required columns must exist
    for col in ["session_id", "page", "device", "duration_secs"]:
        suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column=col)
        )

    # 2. session_id must never be null
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="session_id")
    )

    # 3. device must be known values
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="device",
            value_set=["mobile", "desktop", "tablet"]
        )
    )

    # 4. duration_secs must be positive
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="duration_secs", min_value=1
        )
    )

    # 5. page must not be null
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="page")
    )

    # 6. Table must have rows
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)
    )

    result = batch.validate(suite)
    return result.success


def validate_raw_user_signups(context, df: pd.DataFrame) -> bool:
    suite_name = "raw_user_signups_suite"

    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

    batch_definition = (
        context.data_sources
        .add_pandas(name="signups_source")
        .add_dataframe_asset(name="signups_asset")
        .add_batch_definition_whole_dataframe(suite_name)
    )

    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    # 1. Required columns must exist
    for col in ["user_id", "email", "signup_method"]:
        suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column=col)
        )

    # 2. user_id must never be null
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="user_id")
    )

    # 3. email must match email format
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="email",
            regex=r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
        )
    )

    # 4. signup_method must be known values
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="signup_method",
            value_set=["email", "google", "facebook"]
        )
    )

    # 5. Table must have rows
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)
    )

    result = batch.validate(suite)
    return result.success

# --- Main
def main():
    log.info("🔍  Starting data validation with Great Expectations...")
    start = datetime.utcnow()

    context = gx.get_context(mode="ephemeral")

    results = {}

    # --- Validate raw_orders ---
    log.info("📋  Validating raw_orders...")
    df_orders = fetch_table("raw_orders")
    results["raw_orders"] = validate_raw_orders(context, df_orders)

    # --- Validate raw_page_clicks ---
    log.info("📋  Validating raw_page_clicks...")
    df_clicks = fetch_table("raw_page_clicks")
    results["raw_page_clicks"] = validate_raw_page_clicks(context, df_clicks)

    # --- Validate raw_user_signups ---
    log.info("📋  Validating raw_user_signups...")
    df_signups = fetch_table("raw_user_signups")
    results["raw_user_signups"] = validate_raw_user_signups(context, df_signups)

    # --- Summary ---
    duration = (datetime.utcnow() - start).total_seconds()
    log.info("─" * 60)
    log.info("📊  Validation Summary (%.2fs)", duration)
    log.info("─" * 60)

    all_passed = True
    for table, passed in results.items():
        status = "✅  PASSED" if passed else "❌  FAILED"
        log.info("  %-30s %s", table, status)
        if not passed:
            all_passed = False

    log.info("─" * 60)

    if not all_passed:
        log.error("🚨  Validation FAILED — pipeline should not proceed with bad data!")
        sys.exit(1)
    else:
        log.info("🎉  All validations passed — data is clean!")
        sys.exit(0)


if __name__ == "__main__":
    main()