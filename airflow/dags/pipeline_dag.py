import logging
import psycopg2
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# ── DB config (matches your .env) ────────────────────────────────────────────
DB_CONFIG = {
    "host":     "postgres",   # docker service name, not localhost
    "port":     5432,
    "dbname":   "ecommerce",
    "user":     "pipeline_user",
    "password": "pipeline_pass",
}

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"   # path inside the Airflow container

# ── Default args applied to every task ───────────────────────────────────────
default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

# ── Health-check functions ────────────────────────────────────────────────────

def check_raw_data(**context) -> None:
    """
    Fail the DAG early if no new raw data has landed in the last hour.
    This prevents dbt from running on stale data.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()

    cur.execute("""
        SELECT COUNT(*)
        FROM raw_orders
        WHERE ingested_at >= NOW() - INTERVAL '1 hour'
    """)
    row_count = cur.fetchone()[0]
    cur.close()
    conn.close()

    log.info("📦  New raw_orders in last hour: %d", row_count)

    if row_count == 0:
        raise ValueError(
            "No new orders ingested in the last hour. "
            "Check that the Kafka producer and consumer are running."
        )

    # Push count to XCom so downstream tasks can log it
    context["ti"].xcom_push(key="new_order_count", value=row_count)


def check_mart_freshness(**context) -> None:
    """
    After dbt runs, verify that mart_daily_revenue was actually populated today.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()

    cur.execute("""
        SELECT COUNT(*)
        FROM marts.mart_daily_revenue
        WHERE order_date = CURRENT_DATE
    """)
    row_count = cur.fetchone()[0]
    cur.close()
    conn.close()

    log.info("📊  mart_daily_revenue rows for today: %d", row_count)

    if row_count == 0:
        raise ValueError(
            "mart_daily_revenue has no rows for today after dbt run. "
            "Check dbt model logic."
        )


def log_pipeline_success(**context) -> None:
    new_orders = context["ti"].xcom_pull(
        task_ids="check_raw_data", key="new_order_count"
    )
    log.info("✅  Pipeline completed successfully.")
    log.info("    Orders processed this run: %s", new_orders)


# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="ecommerce_pipeline",
    description="Orchestrates dbt transformations for the e-commerce analytics pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 * * * *",   # every hour at :00
    catchup=False,                   # don't backfill missed runs
    max_active_runs=1,               # only one run at a time
    tags=["ecommerce", "dbt", "portfolio"],
) as dag:

    # ── Stage 1: Gate — check data exists before doing anything ──────────────
    start = EmptyOperator(task_id="start")

    check_data = PythonOperator(
        task_id="check_raw_data",
        python_callable=check_raw_data,
    )

    run_validation = BashOperator(
        task_id="run_great_expectations",
        bash_command="cd /opt/airflow && python validation/validate.py"
    )

    # ── Stage 2: dbt tasks ───────────────────────────────────────────────────
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt debug --profiles-dir .",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select staging --profiles-dir .",
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select staging --profiles-dir .",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select marts --profiles-dir .",
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select marts --profiles-dir .",
    )

    # ── Stage 3: Quality gate — verify marts are fresh ───────────────────────
    check_freshness = PythonOperator(
        task_id="check_mart_freshness",
        python_callable=check_mart_freshness,
    )

    # ── Stage 4: Generate updated dbt docs ───────────────────────────────────
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir .",
    )

    # ── Stage 5: Done ─────────────────────────────────────────────────────────
    log_success = PythonOperator(
        task_id="log_success",
        python_callable=log_pipeline_success,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,  # always runs, even if upstream fails
    )

    # ── Task dependencies (the DAG shape) ────────────────────────────────────
    #
    #   start
    #     └── check_raw_data
    #           └── dbt_debug
    #                 └── dbt_run_staging
    #                       └── dbt_test_staging
    #                             └── dbt_run_marts
    #                                   └── dbt_test_marts
    #                                         └── check_mart_freshness
    #                                               └── dbt_docs_generate
    #                                                     └── log_success
    #                                                           └── end

    (
        start
        >> check_data
        >> run_validation
        >> dbt_debug
        >> dbt_run_staging
        >> dbt_test_staging
        >> dbt_run_marts
        >> dbt_test_marts
        >> check_freshness
        >> dbt_docs
        >> log_success
        >> end
    )