from airflow import models
from airflow import DAG
import airflow
from datetime import datetime, timedelta
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType

from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'Airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=50),
    'dataflow_default_options': {
        'project': 'spark-learning-43150',
        'region': 'us-central1',
        'runner': 'DataflowRunner'
    }
}

dag = DAG(
    dag_id='dataflow_earthquake_pipeline_daily_schedule',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 10, 4),
    catchup=False,
    description="DAG for data ingestion and transformation"
)

start = DummyOperator(
    task_id="start_task_id",
    dag=dag

)

t1_extract_data_api_and_write_gcs_monthly = BeamRunPythonPipelineOperator(
    task_id="p1_extract_data_api_and_write_gcs_monthly",
    dag=dag,
    gcp_conn_id="gcp_connection",
    runner=BeamRunnerType.DataflowRunner,
    py_file="gs://earthquake_analysis_buck/dataflow/dataflow_code/p1_1_extract_data_api_write_gcs_monthly.py",

)


t2_read_from_landing_apply_trans_load_silver = BeamRunPythonPipelineOperator(
    task_id="t2_read_from_landing_apply_trans_load_silver_monthly",
    dag=dag,
    gcp_conn_id="gcp_connection",
    runner=BeamRunnerType.DataflowRunner,
    py_file="gs://earthquake_analysis_buck/dataflow/dataflow_code/p2_read_from_landing_apply_trans_load_silver.py",

)

t3_read_silver_load_bq_golden = BeamRunPythonPipelineOperator(
    task_id="p3_read_silver_load_bq_golden_monthly",
    dag=dag,
    gcp_conn_id="gcp_connection",
    runner=BeamRunnerType.DataflowRunner,
    py_file="gs://earthquake_analysis_buck/dataflow/dataflow_code/p3_read_silver_load_bq_golden.py",

)

end = DummyOperator(
    task_id="end_task_id",
    dag=dag
)


start >> t1_extract_data_api_and_write_gcs_monthly >> t2_read_from_landing_apply_trans_load_silver>>t3_read_silver_load_bq_golden>> end




