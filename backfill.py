from datetime import datetime, timedelta
import os
import requests

from airflow.sdk import DAG, task, BaseHook
from shared_tools.tools import default_args


timedeltas = {"j_7": 7, "j_30": 30}


def _is_composer() -> bool:
    return "COMPOSER_ENVIRONMENT" in os.environ


def _get_token_and_base() -> tuple[str, str]:
    conn = BaseHook.get_connection("airflow_api")
    scheme = conn.schema or ("https" if _is_composer() else "http")
    base = f"{scheme}://{conn.host}"
    if conn.port:
        base += f":{conn.port}"

    if _is_composer():
        import google.auth
        from google.auth.transport.requests import Request
        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        creds.refresh(Request())
        return creds.token, base

    r = requests.post(
        f"{base}/auth/token",
        json={"username": conn.login, "password": conn.password},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"], base


@task
def trigger_backfill(days: int, **context):
    token, base = _get_token_and_base()
    print(f"Token length: {len(token)} | base: {base}")

    ds = context["ds"]
    d = datetime.strptime(ds, "%Y-%m-%d")
    payload = {
        "dag_id": "etl_paid_media",
        "from_date": (d - timedelta(days=days + 1)).strftime("%Y-%m-%dT00:00:00Z"),
        "to_date":   (d - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00Z"),
        "reprocess_behavior": "completed",
        "max_active_runs": 1,
        "run_backwards": False,
    }
    print(f"Payload: {payload}")

    r = requests.post(
        f"{base}/api/v2/backfills",
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/json"},
        json=payload,
        timeout=60,
    )
    print(f"Response {r.status_code}: {r.text}")
    r.raise_for_status()
    return r.json()


with DAG(
    "etl_paid_media_refresh",
    default_args=default_args,
    description="Data Refresh on day minus 8 each day",
    schedule=None,
    catchup=False,
) as dag:
    for delta_name, days in timedeltas.items():
        trigger_backfill.override(task_id=f"backfill_task_{delta_name}")(days=days)
