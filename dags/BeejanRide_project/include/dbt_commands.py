DBT_PROJECT_DIR  = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_profiles"
DBT_TARGET       = "prod"
GOOGLE_CREDS     = "/opt/airflow/gcp_keyfile.json"


def dbt_cmd(command: str, select: str = "", full_refresh: bool = False) -> str:
    """
    Builds a dbt bash command string.

    Args:
        command:      dbt sub-command e.g. 'run', 'test', 'snapshot'
        select:       dbt node selector e.g. 'staging', 'marts'
        full_refresh: if True, adds --full-refresh flag (needed for
                      incremental models on BigQuery free tier)

    Returns:
        A full bash command string ready for BashOperator.
    """
    select_flag       = f"--select {select}" if select else ""
    full_refresh_flag = "--full-refresh" if full_refresh else ""

    return (
        f"export GOOGLE_APPLICATION_CREDENTIALS={GOOGLE_CREDS} && "
        f"dbt {command} "
        f"--project-dir {DBT_PROJECT_DIR} "
        f"--profiles-dir {DBT_PROFILES_DIR} "
        f"--target {DBT_TARGET} "
        f"--no-use-colors "
        f"{select_flag} "
        f"{full_refresh_flag}"
    ).strip()
