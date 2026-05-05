"""
DAG 2: Daily Analytics Pipeline
Schedule: 2:00 AM daily
Flow: yesterday check → quality gate → RFM update → churn scores → report snapshot → notify
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

POSTGRES_CONN_ID = "ecommerce_postgres"

# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def check_yesterdays_data(**context):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    count = hook.get_first("""
        SELECT COUNT(*)
        FROM transactions
        WHERE DATE(transaction_date) = CURRENT_DATE - 1
    """)[0]

    log.info("Yesterday's transaction count: %d", count)
    context["ti"].xcom_push(key="yesterday_txn_count", value=count)

    if count == 0:
        raise AirflowSkipException("No transactions found for yesterday — skipping pipeline")


def run_data_quality_checks(**context):
    """Inline quality gate: checks critical rules only (fast subset)."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    failures = []

    # Null user IDs
    null_users = hook.get_first(
        "SELECT COUNT(*) FROM transactions WHERE user_id IS NULL"
    )[0]
    if null_users > 0:
        failures.append(f"null_user_ids={null_users}")

    # Negative amounts
    bad_amounts = hook.get_first(
        "SELECT COUNT(*) FROM transactions WHERE total_amount < 0"
    )[0]
    if bad_amounts > 0:
        failures.append(f"negative_amounts={bad_amounts}")

    # Orphan transactions (yesterday only, faster)
    orphans = hook.get_first("""
        SELECT COUNT(*)
        FROM transactions t
        LEFT JOIN users u ON t.user_id = u.user_id
        WHERE u.user_id IS NULL
          AND DATE(t.transaction_date) = CURRENT_DATE - 1
    """)[0]
    if orphans > 0:
        failures.append(f"orphan_transactions={orphans}")

    result = "fail" if failures else "pass"
    context["ti"].xcom_push(key="quality_result", value=result)
    context["ti"].xcom_push(key="quality_failures", value=failures)
    log.info("Quality check result: %s | failures=%s", result, failures)
    return result


def branch_on_quality(**context):
    result = context["ti"].xcom_pull(task_ids="run_data_quality_checks", key="quality_result")
    if result == "pass":
        return "quality_pass"
    return "quality_fail_alert"


def quality_fail_alert(**context):
    failures = context["ti"].xcom_pull(task_ids="run_data_quality_checks", key="quality_failures")
    log.warning(
        "Quality checks FAILED for %s — pipeline continues with known issues: %s",
        context["ds"],
        failures,
    )


def notify_completion(**context):
    ti = context["ti"]
    txn_count = ti.xcom_pull(task_ids="check_yesterdays_data", key="yesterday_txn_count") or 0

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    row = hook.get_first("""
        SELECT total_revenue, avg_order_value
        FROM daily_report_log
        WHERE report_date = CURRENT_DATE - 1
    """)
    revenue = float(row[0]) if row and row[0] else 0.0
    aov = float(row[1]) if row and row[1] else 0.0

    log.info(
        "Daily pipeline complete for %s | transactions=%d | revenue=$%.2f | AOV=$%.2f",
        context["ds"],
        txn_count,
        revenue,
        aov,
    )


# ---------------------------------------------------------------------------
# SQL strings
# ---------------------------------------------------------------------------

UPDATE_RFM_SQL = """
UPDATE users u
SET
    customer_segment = rfm.segment,
    updated_at       = CURRENT_TIMESTAMP
FROM (
    WITH rfm_base AS (
        SELECT
            user_id,
            CURRENT_DATE - MAX(transaction_date)::date        AS recency_days,
            COUNT(DISTINCT transaction_id)                    AS frequency,
            COALESCE(SUM(total_amount), 0)                    AS monetary
        FROM transactions
        GROUP BY user_id
    ),
    scored AS (
        SELECT user_id,
            NTILE(5) OVER (ORDER BY recency_days DESC) AS r,
            NTILE(5) OVER (ORDER BY frequency)         AS f,
            NTILE(5) OVER (ORDER BY monetary)          AS m
        FROM rfm_base
    )
    SELECT user_id,
        CASE
            WHEN r >= 4 AND f >= 4 AND m >= 4 THEN 'Champions'
            WHEN r >= 3 AND f >= 4             THEN 'Loyal Customers'
            WHEN r >= 4 AND f >= 3             THEN 'Potential Loyalists'
            WHEN r >= 4 AND f <= 2             THEN 'New Customers'
            WHEN r >= 3 AND m >= 4             THEN 'Promising'
            WHEN r >= 3 AND f >= 3             THEN 'Need Attention'
            WHEN r <= 2 AND f >= 4 AND m >= 4  THEN 'At Risk'
            WHEN r <= 1 AND f >= 4 AND m >= 4  THEN 'Cannot Lose Them'
            WHEN r <= 2 AND f >= 3             THEN 'About to Sleep'
            WHEN r <= 2 AND f <= 2             THEN 'Hibernating'
            WHEN r <= 1                        THEN 'Lost'
            ELSE 'Other'
        END AS segment
    FROM scored
) rfm
WHERE u.user_id = rfm.user_id;
"""

UPDATE_CHURN_SQL = """
UPDATE users u
SET
    churn_risk_score = LEAST(1.0,
        ROUND(
            (COALESCE(CURRENT_DATE - MAX(t.transaction_date)::date, 365)::numeric / 365)
            * (1 - LEAST(1.0, COUNT(DISTINCT t.transaction_id)::numeric / 20)),
            2
        )
    ),
    updated_at = CURRENT_TIMESTAMP
FROM transactions t
WHERE u.user_id = t.user_id
GROUP BY u.user_id;
"""

SNAPSHOT_REPORT_SQL = """
INSERT INTO daily_report_log
    (report_date, total_users, total_transactions, total_revenue, avg_order_value, created_at)
SELECT
    CURRENT_DATE - 1,
    COUNT(DISTINCT user_id),
    COUNT(*),
    ROUND(SUM(total_amount)::numeric, 2),
    ROUND(AVG(total_amount)::numeric, 2),
    CURRENT_TIMESTAMP
FROM transactions
WHERE DATE(transaction_date) = CURRENT_DATE - 1
ON CONFLICT (report_date) DO UPDATE
    SET total_users        = EXCLUDED.total_users,
        total_transactions = EXCLUDED.total_transactions,
        total_revenue      = EXCLUDED.total_revenue,
        avg_order_value    = EXCLUDED.avg_order_value,
        created_at         = EXCLUDED.created_at;
"""

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="daily_analytics_dag",
    description="Daily pipeline: quality gate → RFM update → churn scores → report snapshot",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["daily", "rfm", "churn", "phase3"],
) as dag:

    t_check_yesterday = PythonOperator(
        task_id="check_yesterdays_data",
        python_callable=check_yesterdays_data,
    )

    t_quality = PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=run_data_quality_checks,
    )

    t_branch = BranchPythonOperator(
        task_id="branch_on_quality",
        python_callable=branch_on_quality,
    )

    # Pass branch: empty gate task
    t_quality_pass = EmptyOperator(task_id="quality_pass")

    # Fail branch: log warning, then continue
    t_quality_fail = PythonOperator(
        task_id="quality_fail_alert",
        python_callable=quality_fail_alert,
    )

    t_rfm = PostgresOperator(
        task_id="update_rfm_segments",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=UPDATE_RFM_SQL,
        # Runs after either branch completes
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    t_churn = PostgresOperator(
        task_id="update_churn_scores",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=UPDATE_CHURN_SQL,
    )

    t_snapshot = PostgresOperator(
        task_id="snapshot_daily_report",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=SNAPSHOT_REPORT_SQL,
    )

    t_notify = PythonOperator(
        task_id="notify_completion",
        python_callable=notify_completion,
    )

    # Pipeline wiring
    t_check_yesterday >> t_quality >> t_branch
    t_branch >> [t_quality_pass, t_quality_fail]
    [t_quality_pass, t_quality_fail] >> t_rfm
    t_rfm >> t_churn >> t_snapshot >> t_notify
