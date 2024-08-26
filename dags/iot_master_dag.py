"""
# iot_master_dag

DAG kicks off extract/transform external pipelines (i.e. every hour)
to extract sources from MongoDB, ingest them to Timescaledb, and normalise
them in `stage` schema. Purpose of this DAG is to maintain source data
updated by doing ELT during small interval (hourly) to prepare `stage`
data to be used for DWH aggregates.

## Schedule

- **Frequency**: Runs every 15 minutes.
- **Catch Up**: False

## Tasks

1. **trigger_dag_extract**: Extract sources and load to DWH dag:
    > subdag: iot_extract_raw (iot_mongo_extract_to_dwh.py)
2. **trigger_dag_transform**: Normalise raw tables and push to `stage` DWH schema:
    > subdag: iot_transform_stage (iot_dwh_transform_stage.py)
"""

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': 'test@test.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'is_paused_upon_creation': True,
    'retries': 0,
    'start_date': datetime(2020, 1, 1),
}

with DAG(
  'iot_master_dag',
  catchup=False,
  max_active_runs=1,
  schedule="*/15 * * * *",
  default_args=default_args,
) as dag:
    start_task = DummyOperator(
      task_id='start_master',
      dag=dag
    )

    trigger_extract = TriggerDagRunOperator(
        task_id="trigger_dag_extract",
        trigger_dag_id="iot_raw_extract",
        wait_for_completion=True
    )

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_dag_transform",
        trigger_dag_id="iot_stage_transform",
        wait_for_completion=True
    )

    end_task = DummyOperator(
      task_id='end_master',
      dag=dag
    )

    start_task >> trigger_extract >> trigger_transform >> end_task
    dag.doc_md = __doc__