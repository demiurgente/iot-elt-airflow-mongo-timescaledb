"""
# iot_agg_transform_weekly

DAG kicks off dbt pipeline to aggregate tables from DWH
`stage` schema and create health metrics tables in `stage`
schema. Purpose of this DAG is to maintain agg data layer
updated weekly with new staged batches to be used for reporting,
analytical purposes.

## Schedule

- **Frequency**: @weekly
- **Catch Up**: False

## Tasks

1. **run_dbt_weekly_steps**: Aggregate weekly step metrics for IOT devices
2. **run_dbt_weekly_sleeps**: Aggregate weekly sleep metrics for IOT devices
4. **run_dbt_weekly_summary**: Combine weekly health metrics for IOT devices
SQL models for each task in `dbt_project/models/agg/weekly_*`:

## DWH table relations:
```
  |dwh.stage.heart_rates|
  |dwh.stage.sleeps     | -> dwh.agg.weekly_sleeps
 /                                               \\
-                                                 dwh.agg.weekly_summary                                
 \                                               /
  |dwh.stage.heart_rates| -> dwh.agg.weekly_steps
  |dwh.stage.steps      |

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
    'is_paused_upon_creation': True,
    'retries': 0,
    'start_date': datetime(2020, 1, 1),
}

AGG_TABLES = [
    'sleeps',
    'steps',
]

def create_named_dbt_task(model):
    @task.bash(task_id=f'run_dbt_weekly_{model}')
    def run_dbt_cmd(model):
        # display generated SQL code for DWH and
        # execute dbt command to build models defined in agg.weekly_* schema
        return f"""
            dbt compile --select agg.weekly_{model} --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROFILES_DIR} &&
            dbt run --models agg.weekly_{model} --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROFILES_DIR}
        """
    return run_dbt_cmd(model)

with DAG(
  'iot_agg_transform_weekly',
  catchup=False,
  max_active_runs=1,
  schedule=None,
  schedule_interval="@weekly",
  default_args=default_args,
) as dag:
    start_task = DummyOperator(
      task_id='start_agg_transform_weekly',
      dag=dag
    )

    # build aggregates `dwh.agg.weekly_<sleeps,steps>`
    with TaskGroup(group_id="run_dbt_weekly_models") as dbt_pipeline:
        dbt_tasks = []
        for model in AGG_TABLES:
            dbt_tasks.append(create_named_dbt_task(model))
        dbt_tasks >> create_named_dbt_task("summary")
        
    end_task = DummyOperator(
      task_id='end_agg_transform_weekly',
      dag=dag
    )

    start_task >> dbt_pipeline >> end_task
    dag.doc_md = __doc__
