from datetime import datetime, timedelta
import json
import os

from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.operators.http import HttpOperator

from shared_tools.tools import default_args


# Backfill deltas in days
timedeltas = {
    "j_7": 7,
    "j_30": 30,
}


def _is_composer() -> bool:
    # Variable d'env injectée automatiquement par Cloud Composer
    return "COMPOSER_ENVIRONMENT" in os.environ


@task(task_id="get_bearer_token")
def get_bearer_token() -> str:
    """Récupère un Bearer token pour l'API Airflow.

    - Local (SimpleAuthManager) : JWT via /auth/token (login/password).
    - Composer 3 (Airflow 3)    : access token GCP — l'API est derrière IAP,
      les creds du service account du worker suffisent.
    """
    if _is_composer():
        import google.auth
        from google.auth.transport.requests import Request

        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        creds.refresh(Request())
        return creds.token

    # Local
    import requests
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection("airflow_api")
    base = f"{conn.schema or 'http'}://{conn.host}:{conn.port or 8080}"
    r = requests.post(
        f"{base}/auth/token",
        json={"username": conn.login, "password": conn.password},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


with DAG(
    "etl_paid_media_refresh",
    default_args=default_args,
    description="Data Refresh on day minus 8 each day -> Daily data not fully reliable on GCM",
    schedule=None,
    catchup=False,
) as dag:

    token = get_bearer_token()

    for delta, days in timedeltas.items():
        # Évaluation au runtime via Jinja (avant tu utilisais datetime.today()
        # au parse time, ce qui figeait la date au moment du parsing du DAG).
        start_date = "{{ macros.ds_add(ds, -" + str(days + 1) + ") }}"
        end_date   = "{{ macros.ds_add(ds, -" + str(days) + ") }}"

        backfill_task = HttpOperator(
            task_id=f"backfill_task_{delta}",
            http_conn_id="airflow_api",
            endpoint="api/v2/backfills",
            method="POST",
            headers={
                "Authorization": "Bearer {{ ti.xcom_pull(task_ids='get_bearer_token') }}",
                "Content-Type": "application/json",
            },
            data=json.dumps({
                "dag_id": "etl_paid_media",
                "from_date": f"{start_date}T00:00:00Z",
                "to_date":   f"{end_date}T00:00:00Z",
                "reprocess_behavior": "completed",
                "max_active_runs": 1,
                "run_backwards": False,
            }),
            log_response=True,
        )

        token >> backfill_task
