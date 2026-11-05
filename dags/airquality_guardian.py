"""AirQuality Guardian ingestion DAG.

One DAG, two tasks, every 5 minutes (override with the AIRFLOW_SCHEDULE
env var):

    run_cycle -> python -m src.main            fetch -> validate -> insert
    replay    -> python -m src.main --replay   drain the quarantine

Design rules (deliberate, do not "improve" them without re-reading):

* No Airflow-side ``retries``: the self-healing engine already does
  backoff + 3 retries *inside* the cycle. Two retry systems would
  multiply the work and blur the "attempts" reported to Datadog.
  Airflow is only responsible for "pick the cycle back up next tick".
* A degraded cycle is a SUCCESSFUL task: the pipeline already rescued
  the data into quarantine_events, so nothing is lost. The alarm path
  is the Datadog monitor on pipeline.records.failed, not the task
  colour. Failing the task would just burn Airflow retries on cycles
  that already self-healed.
* replay runs every tick: it is idempotent (claim/lease CAS + the
  dedup-key unique index), a no-op when nothing is quarantined.
"""

import os

from airflow import DAG
from airflow.operators.bash import BashOperator

# In-container python: the Airflow image's venv already has the
# pipeline requirements (see Dockerfile.airflow).
PY = "/opt/airflow/venv/bin/python"
WORKDIR = "/opt/airflow"
LOCATION_ID = os.getenv("PIPELINE_LOCATION_ID", "4946813")

# Catchup=False: when this DAG is first deployed it must NOT backfill
# every 5-min interval since 1970.
dag = DAG(
    dag_id="airquality_guardian",
    description="Ingest OpenAQ -> Atlas with self-healing quarantine + replay.",
    # 5 min matches the standalone scheduler default; Airflow fires a
    # new run as soon as the previous one ends, so the effective cadence
    # is max(5 min, cycle duration).
    schedule=os.getenv("AIRFLOW_SCHEDULE", "*/5 * * * *"),
    catchup=False,
    tags=["airquality", "hackathon"],
    default_args={
        # See module docstring: the self-healing engine owns retry.
        "retries": 0,
        "email_on_failure": False,
    },
)

run_cycle = BashOperator(
    task_id="run_cycle",
    bash_command=f"cd {WORKDIR} && {PY} -m src.main --location-id {LOCATION_ID}",
    dag=dag,
)

replay_quarantine = BashOperator(
    task_id="replay_quarantine",
    bash_command=f"cd {WORKDIR} && {PY} -m src.main --replay",
    dag=dag,
)

run_cycle >> replay_quarantine
