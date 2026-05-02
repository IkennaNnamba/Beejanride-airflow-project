# ─────────────────────────────────────────────────────────────
# beejanride_elt_pipeline.py
# Main DAG for the BeejanRide ELT pipeline.
#
# Flow:
#   Airbyte syncs (ingestion)
#   → dbt staging → dbt intermediate → dbt marts (transformation)
#   → dbt tests per layer (testing)
#   → dbt snapshot
# ─────────────────────────────────────────────────────────────

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
# from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.utils.task_group import TaskGroup

# Local helpers from the include folder
from BeejanRide_project.include.dbt_commands import dbt_cmd
# from BeejanRide_project.include.airbyte_connections import AIRBYTE_CONN_ID, AIRBYTE_CONNECTION_IDS

# ── Default arguments applied to every task in this DAG ──────────────────────
default_args = {
    "owner": "ikenna",
    "retries": 2,                                  # retry a failed task twice
    "retry_delay": timedelta(minutes=5),           # wait 5 mins between retries
    "retry_exponential_backoff": True,             # 5min → 10min → 20min
    "email_on_failure": False,                     # turn on after email is configured
    "email_on_retry": False,
    "depends_on_past": False,                      # each run is independent
}

# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="beejanride_elt_pipeline",
    description="BeejanRide ELT: Airbyte → dbt staging → intermediate → marts → tests → snapshot",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,          # don't run missed historical runs automatically
    max_active_runs=1,      # only one run at a time — prevents data conflicts
    default_args=default_args,
    tags=["beejanride", "elt", "production"],
) as dag:

    # Marks the beginning of the pipeline
    start = EmptyOperator(task_id="start")

    # Marks the end of the pipeline
    end = EmptyOperator(task_id="end")

    # ── 1. INGESTION: Trigger all Airbyte syncs in parallel ───────────────────
    # with TaskGroup("ingestion", tooltip="Trigger Airbyte syncs for all 6 raw tables") as ingestion:
    #    for table, connection_id in AIRBYTE_CONNECTION_IDS.items():
    #        AirbyteTriggerSyncOperator(
    #            task_id=f"sync_{table}",
    #            airbyte_conn_id=AIRBYTE_CONN_ID,
    #            connection_id=connection_id,
    #            asynchronous=False,    # wait for sync to complete before moving on
    #            timeout=3600,          # fail if sync takes longer than 1 hour
    #            wait_seconds=30,       # poll Airbyte every 30 seconds for status
    #        )

    # ── 2. TRANSFORMATION: Run dbt models layer by layer ─────────────────────
    with TaskGroup("transformation", tooltip="Run dbt models: staging → intermediate → marts") as transformation:

        dbt_staging = BashOperator(
            task_id="dbt_staging",
            bash_command=dbt_cmd("run", "staging", full_refresh=True),
            execution_timeout=timedelta(minutes=30),
        )

        dbt_intermediate = BashOperator(
            task_id="dbt_intermediate",
            bash_command=dbt_cmd("run", "intermediate", full_refresh=True),
            execution_timeout=timedelta(minutes=30),
        )

        dbt_marts = BashOperator(
            task_id="dbt_marts",
            bash_command=dbt_cmd("run", "marts", full_refresh=True),
            execution_timeout=timedelta(minutes=30),
        )

        # staging must finish before intermediate, intermediate before marts
        dbt_staging >> dbt_intermediate >> dbt_marts

    # ── 3. TESTING: Run dbt tests layer by layer ──────────────────────────────
    with TaskGroup("testing", tooltip="Run dbt tests: staging → intermediate → marts") as testing:

        test_staging = BashOperator(
            task_id="test_staging",
            bash_command=dbt_cmd("test", "staging"),
            execution_timeout=timedelta(minutes=20),
        )

        test_intermediate = BashOperator(
            task_id="test_intermediate",
            bash_command=dbt_cmd("test", "intermediate"),
            execution_timeout=timedelta(minutes=20),
        )

        test_marts = BashOperator(
            task_id="test_marts",
            bash_command=dbt_cmd("test", "marts"),
            execution_timeout=timedelta(minutes=20),
        )

        test_staging >> test_intermediate >> test_marts

    # ── 4. SNAPSHOT: Capture SCD Type 2 history for drivers ───────────────────
    #dbt_snapshot = BashOperator(
    #    task_id="dbt_snapshot",
    #    bash_command=dbt_cmd("snapshot"),
    #    execution_timeout=timedelta(minutes=20),
    #)

    # ── 5. PIPELINE DEPENDENCIES: Define the full execution order ─────────────
    #
    #   start
    #     └── ingestion (all 6 Airbyte syncs run in parallel)
    #           └── transformation
    #                 └── dbt_staging → dbt_intermediate → dbt_marts
    #                       └── testing
    #                             └── test_staging → test_intermediate → test_marts
    #                                   └── dbt_snapshot
    #                                         └── end
    #
    # start >> ingestion >> transformation >> testing >> dbt_snapshot >> end
    # start >> transformation >> testing >> dbt_snapshot >> end
    start >> transformation >> testing >> end