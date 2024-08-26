"""
# iot_raw_extract

DAG kicks off dlt pipeline to extract source collections
from MongoDB, ingest them to Timescaledb `raw` schema. More
on how nested documents are extracted:
- https://dlthub.com/docs/examples/nested_data

Level of parallelism is set in global `.env` variables.
Current setup needs tweaking to achieve better performance
for bulk uploads of historical data. To tune the performance
for extraction/loading refer to:
- https://dlthub.com/docs/reference/performance#extract
- https://dlthub.com/docs/reference/performance#load

Purpose of this DAG is to maintain source data retrieved
in batches and perform ELT during small intervals (15 min).

## Schedule

- **Frequency**: None (has to be triggered manually or from iot_master_dag).
- **Catch Up**: False

## Tasks
1. **mongodb_users**: Extract IOT device users and load to DWH
2. **mongodb_steps**: Extract step metrics for IOT devices and load to DWH
3. **mongodb_sleeps**: Extract sleep metrics for IOT devices and load to DWH
4. **mongodb_heart_rates**: Extract bpm metrics for IOT devices and load to DWH
(auto-generated based on existing Mongo collections)
"""

from airflow import DAG
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from dlt.common import pendulum
from dlt.helpers.airflow_helper import PipelineTasksGroup
from tenacity import Retrying, stop_after_attempt

import dlt

default_task_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': 'test@test.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2020,1,1),
}

with DAG(
  'iot_raw_extract',
  catchup=True,
  max_active_runs=1,
  schedule=None,
  default_args=default_task_args,
  # Uncomment to extract data from mongodb source for specific date
  #   and trigger DAG to filter out records below `earliest_date`:
  #   
  # params={
  #     "earliest_date": dt.datetime.today(),
  # }
) as dag:
    start_task = DummyOperator(
      task_id='start_raw_extract',
      dag=dag
    )
    # Set `use_data_folder` to True to store temporary data on the `data` bucket.
    # Use only when it does not fit on the local storage
    # more about dlt elt: https://dlthub.com/docs/walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer
    dlt_pipeline = PipelineTasksGroup(
        "extract_mongo_to_pg",
        use_data_folder=False,
        wipe_local_data=True,
        use_task_logger=True,
        retry_policy=Retrying(stop=stop_after_attempt(3), reraise=True),
    )

    # Import your source from pipeline script
    from dlt_sources.mongodb import mongodb

    # Modify the pipeline parameters
    pipeline = dlt.pipeline(
        pipeline_name='extract_mongo_to_pg',
        dataset_name='raw',
        destination='postgres',
        progress="log",
        dev_mode=False # must be false if we decompose
    )

    source = mongodb(
        incremental=dlt.sources.incremental("created_at"),
        # initial_value="{{ params.earliest_date }}"
    )

    dlt_pipeline.add_run(
      pipeline,
      source,
      decompose="parallel",
      trigger_rule="all_done",
      retries=0,
      provide_context=True
    )

    end_task = DummyOperator(
      task_id='end_raw_extract',
      dag=dag
    )

    start_task >> dlt_pipeline >> end_task
    dag.doc_md = __doc__