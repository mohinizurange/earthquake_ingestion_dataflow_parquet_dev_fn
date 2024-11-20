import apache_beam as beam
import pyarrow # Use PyArrow to handle Arrow data (like Parquet files).
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, WorkerOptions
import os
import logging
# from util import Utils
from datetime import datetime
import requests
import json
import argparse
from google.cloud import bigquery
from apache_beam.io.gcp.gcsio import GcsIO




clean_data_parquet_schema = pyarrow.schema([
    ('mag', pyarrow.float64()),
    ('place', pyarrow.string()),
    ('time', pyarrow.string()),        # Timestamp in seconds
    ('updated', pyarrow.string()),     # Timestamp in seconds
    ('tz', pyarrow.string()),
    ('url', pyarrow.string()),
    ('detail', pyarrow.string()),
    ('felt', pyarrow.float64()),
    ('cdi', pyarrow.float64()),
    ('mmi', pyarrow.float64()),
    ('alert', pyarrow.string()),
    ('status', pyarrow.string()),
    ('tsunami', pyarrow.int64()),
    ('sig', pyarrow.int64()),
    ('net', pyarrow.string()),
    ('code', pyarrow.string()),
    ('ids', pyarrow.string()),
    ('sources', pyarrow.string()),
    ('types', pyarrow.string()),
    ('nst', pyarrow.int64()),
    ('dmin', pyarrow.float64()),
    ('rms', pyarrow.float64()),
    ('gap', pyarrow.int64()),
    ('magType', pyarrow.string()),
    ('type', pyarrow.string()),
    ('title', pyarrow.string()),
    ('area', pyarrow.string()),
    ('geometry', pyarrow.struct([
        ('longitude', pyarrow.float64()),
        ('latitude', pyarrow.float64()),
        ('depth', pyarrow.float64())
    ])),
    ('insert_date', pyarrow.string())  # Timestamp in seconds
])


bq_schema = {
    "fields": [
        {"name": "mag", "type": "FLOAT"},
        {"name": "place", "type": "STRING"},
        {"name": "time", "type": "TIMESTAMP"},
        {"name": "updated", "type": "TIMESTAMP"},
        {"name": "tz", "type": "STRING"},
        {"name": "url", "type": "STRING"},
        {"name": "detail", "type": "STRING"},
        {"name": "felt", "type": "FLOAT"},
        {"name": "cdi", "type": "FLOAT"},
        {"name": "mmi", "type": "FLOAT"},
        {"name": "alert", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "tsunami", "type": "INTEGER"},
        {"name": "sig", "type": "INTEGER"},
        {"name": "net", "type": "STRING"},
        {"name": "code", "type": "STRING"},
        {"name": "ids", "type": "STRING"},
        {"name": "sources", "type": "STRING"},
        {"name": "types", "type": "STRING"},
        {"name": "nst", "type": "INTEGER"},
        {"name": "dmin", "type": "FLOAT"},
        {"name": "rms", "type": "FLOAT"},
        {"name": "gap", "type": "INTEGER"},
        {"name": "magType", "type": "STRING"},
        {"name": "type", "type": "STRING"},
        {"name": "title", "type": "STRING"},
        {"name": "area", "type": "STRING"},
        {"name": "geometry", "type": "RECORD", "fields": [
            {"name": "longitude", "type": "FLOAT"},
            {"name": "latitude", "type": "FLOAT"},
            {"name": "depth", "type": "FLOAT"}
        ]},
        {"name": "insert_date", "type": "TIMESTAMP"}
    ]
}


def audit_event(bigquery_audit_tbl,job_id, pipeline_nm,start_time, task_name, end_time, status, error_msg=None):
    client = bigquery.Client()
    table_id = bigquery_audit_tbl

    # # Convert datetime objects to string format
    job_id_str= job_id.strftime('%Y-%m-%d %H:%M:%S') if isinstance(job_id, datetime) else start_time

    start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S') if isinstance(start_time, datetime) else start_time
    end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S') if isinstance(end_time, datetime) else end_time

    rows_to_insert = [
        {
            "job_id": job_id_str,
            "pipeline_nm": pipeline_nm,
            "task_name": task_name,
            "start_time": start_time_str,
            "end_time": end_time_str,
            "status": status,
            "error_type": error_msg
        }
    ]

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        logging.info("Errors occurred while inserting audit record:", errors)
    else:
        logging.info("Audit record inserted successfully")



if __name__ == '__main__':
    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\Mohini Data Science\earthquake_ingestion_dataflow_parquet_dev_fn\spark-learning-43150-3d588125392c.json"
    option_obj = PipelineOptions()
    google_cloud = option_obj.view_as(GoogleCloudOptions)
    google_cloud.project = 'spark-learning-43150'
    google_cloud.job_name = "earthquake-ingestion-data"
    google_cloud.region = "us-central1"
    google_cloud.staging_location ="gs://earthquake_df_temp_bk/stagging_loc"
    google_cloud.temp_location = "gs://earthquake_df_temp_bk/temp_loc"

    # Set the runner to Dataflow
    # option_obj.view_as(PipelineOptions).runner = 'DataflowRunner'  # For Dataflow

    # Set up logging
    logging.basicConfig(level=logging.INFO)


    ## API Url
    api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

    ###paths
    ## Get the current date and time in 'YYYYMMDD_HHMMSS' format
    cur_timestamp = datetime.now().strftime('%Y%m%d')
    ## bronze layer location
    landing_gcs_location = f'gs://earthquake_analysis_buck/dataflow/landing/{cur_timestamp}/earthquake'
    ## read data from landinng location
    read_data_loc = landing_gcs_location + '.parquet'
    print(f'read data path : {read_data_loc}')

    ### write data in gsc silver location
    silver_gcs_loction = f'gs://earthquake_analysis_buck/dataflow/silver/{cur_timestamp}/flatten_earthquake_data'+'.parquet'

    ## bq table location
    bigquery_tbl_location = 'spark-learning-43150.earthquake_db.dataflow_earthquake_data'
    bigquery_audit_tbl='spark-learning-43150.earthquake_db.dataflow_audit_tbl'

    ## create job id and pipeline nam for audit table
    job_id=datetime.utcnow()
    pipeline_nm='daily'


    with beam.Pipeline(options=option_obj) as p2:
        try:
            start_time = datetime.utcnow()
            task_name='4_read_data_from_silver_loc'
            read_data_from_silver_loc = (
                        p2 | 'Read data From GCS Silver location' >> beam.io.ReadFromParquet(silver_gcs_loction)
                        # | 'PrintReadData' >> beam.Map(lambda x: printline(x))
                        )
            end_time = datetime.utcnow()
            status = 'success'
            error_msg = None
            logging.info("pipeline2 task1 execuated sucessfully")
        except Exception as e:
            error_msg = e
            end_time = datetime.utcnow()
            status = 'fail'
            logging.error(f"pipeline2 task1 get error {e}")

        # audit function call
        audit_event(bigquery_audit_tbl, job_id, pipeline_nm, start_time, task_name, end_time, status, error_msg)

############## bq #####
        try:
            start_time = datetime.utcnow()
            task_name = '5_write_clean_data_into_bq_loc'
            write_clean_data_into_bq_loc = (read_data_from_silver_loc
                                            | "write into bigquery" >> beam.io.WriteToBigQuery(table=bigquery_tbl_location,
                                                                                               schema=bq_schema,
                                                                                               # schema='SCHEMA_AUTODETECT',
                                                                                               write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                                                               create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
                                            )
            end_time = datetime.utcnow()
            status = 'success'
            error_msg = None
            logging.info("pipeline2.2 task3 execuated sucessfully")
        except Exception as e:
            error_msg = e
            status = 'fail'
            end_time = datetime.utcnow()
            logging.error(f"pipeline2.2 task3 get error {e}")

        audit_event(bigquery_audit_tbl, job_id, pipeline_nm, start_time, task_name, end_time, status, error_msg)


