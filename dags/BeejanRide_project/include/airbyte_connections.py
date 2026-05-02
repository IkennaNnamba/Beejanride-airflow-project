# ─────────────────────────────────────────────────────────────
# airbyte_connections.py
# Airbyte connection IDs for each raw table sync.
#
# How to update:
# 1. Open Airbyte UI → Connections
# 2. Click each connection → copy the UUID from the URL
# 3. Go to Airflow UI → Admin → Variables
# 4. Set each variable key with the real UUID as value
#
# Until real IDs are set, placeholders are used and
# Airbyte tasks will fail gracefully without breaking the DAG.
# ─────────────────────────────────────────────────────────────

from airflow.models import Variable

# The Airflow Connection ID pointing to your Airbyte instance
# Set this in Airflow UI → Admin → Connections
AIRBYTE_CONN_ID = "airbyte_default"

# Each key maps to an Airflow Variable holding the Airbyte connection UUID
# for that table's sync job
AIRBYTE_CONNECTION_IDS = {
    "trips":                Variable.get("airbyte_conn_trips",                default_var="placeholder_trips"),
    "drivers":              Variable.get("airbyte_conn_drivers",              default_var="placeholder_drivers"),
    "riders":               Variable.get("airbyte_conn_riders",               default_var="placeholder_riders"),
    "payments":             Variable.get("airbyte_conn_payments",             default_var="placeholder_payments"),
    "cities":               Variable.get("airbyte_conn_cities",               default_var="placeholder_cities"),
    "driver_status_events": Variable.get("airbyte_conn_driver_status_events", default_var="placeholder_driver_status_events"),
}
