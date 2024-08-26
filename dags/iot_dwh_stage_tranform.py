"""
# iot_stage_transform

DAG kicks off dbt pipeline to normalize tables from DWH
`raw` schema and store results in `stage` schema. Purpose
of this DAG is to maintain raw data layer clean and cleanse
new data during small interval (hourly) to prepare `stage`
data to be used for DWH aggregates.

## Schedule

- **Frequency**: None (has to be triggered manually or from iot_master_dag).
- **Catch Up**: False

## Tasks

1. **run_dbt_stage_users**: Cleanse IOT device users
2. **run_dbt_stage_steps**: Cleanse step metrics for IOT devices
3. **run_dbt_stage_sleeps**: Cleanse sleep metrics for IOT devices
4. **run_dbt_stage_heart_rates**: Cleanse bpm metrics for IOT devices
SQL models for each task in `dbt_project/models/stage`

## DWH table relations
```
      |dwh.raw.heart_rates         |
      |dwh.raw.heart_rates__metrics| -> dwh.stage.heart_rates
    /
   /  |dwh.raw.sleeps         |
  /  /|dwh.raw.sleeps__metrics| -> dwh.stage.sleeps
 /  /
- 
 \  \\
  \  \|dwh.raw.steps         |
   \  |dwh.raw.steps__metrics| -> dwh.stage.steps
    \\
      |dwh.raw.users         |
      |dwh.raw.users__devices| -> dwh.stage.users
```
"""

from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from config import DBT_PROFILES_DIR
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': 'test@test.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2020, 1, 1),
}

STAGE_TABLES = [
    'heart_rates',
    'sleeps',
    'steps',
    'users',
]

with DAG(
  'iot_stage_transform',
  catchup=False,
  max_active_runs=1,
  schedule=None,
  default_args=default_args,
) as dag:
    start_task = DummyOperator(
      task_id='start_stage_transform',
      dag=dag
    )
    
    # build staging tables `dwh.stage.<heart_rates,sleeps,steps,users>`
    with TaskGroup(group_id="run_dbt_stage_models") as dbt_pipeline:
        dbt_tasks = []
        for model in STAGE_TABLES:
            @task.bash(task_id=f'run_dbt_stage_{model}')
            def run_dbt_cmd(model):
                # display generated SQL code for DWH and
                # execute dbt command to build models defined in stage.* schema
                return f"""
                    dbt compile --select stage.{model} --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROFILES_DIR} &&
                    dbt run --models stage.{model} --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROFILES_DIR}
                """
            dbt_tasks.append(run_dbt_cmd(model=model))
        dbt_tasks

    end_task = DummyOperator(
      task_id='end_stage_transform',
      dag=dag
    )

    start_task >> dbt_pipeline >> end_task
    dag.doc_md = __doc__