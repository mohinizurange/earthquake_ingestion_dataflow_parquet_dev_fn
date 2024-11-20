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




# Define the complete schema for the GeoJSON data structure
raw_parquet_schema = pyarrow.schema([
    ('type', pyarrow.string()),  # Type of the GeoJSON (FeatureCollection)
    ('metadata', pyarrow.struct([
        ('generated', pyarrow.int64()),
        ('url', pyarrow.string()),
        ('title', pyarrow.string()),
        ('status', pyarrow.int64()),
        ('api', pyarrow.string()),
        ('count', pyarrow.int64())
    ])),  # End of 'metadata' struct
    ('features', pyarrow.list_(
        pyarrow.struct([
            ('type', pyarrow.string()),
            ('properties', pyarrow.struct([
                ('mag', pyarrow.float64()),  # Magnitude
                ('place', pyarrow.string()),  # Location
                ('time', pyarrow.int64()),    # Timestamp
                ('updated', pyarrow.int64()),  # Update timestamp
                ('tz', pyarrow.int64()),      # Time zone
                ('url', pyarrow.string()),     # URL for the event
                ('detail', pyarrow.string()),  # Detail URL
                ('felt', pyarrow.float64()),   # Felt reports
                ('cdi', pyarrow.float64()),    # Community Internet Intensity
                ('mmi', pyarrow.float64()),    # Maximum Intensity
                ('alert', pyarrow.string()),    # Alert level
                ('status', pyarrow.string()),    # Event status
                ('tsunami', pyarrow.int64()),   # Tsunami indicator
                ('sig', pyarrow.int64()),       # Significance
                ('net', pyarrow.string()),      # Network
                ('code', pyarrow.string()),     # Event code
                ('ids', pyarrow.string()),      # IDs associated with the event
                ('sources', pyarrow.string()),   # Sources
                ('types', pyarrow.string()),     # Types of data
                ('nst', pyarrow.int64()),       # Number of stations
                ('dmin', pyarrow.float64()),    # Minimum distance to event
                ('rms', pyarrow.float64()),     # Root Mean Square
                ('gap', pyarrow.int64()),       # Gap
                ('magType', pyarrow.string()),   # Magnitude type
                ('type', pyarrow.string()),      # Type of earthquake
                ('title', pyarrow.string())      # Title of the event
            ])),  # End of 'properties' struct
            ('geometry', pyarrow.struct([
                ('type', pyarrow.string()),                   # Type of geometry
                ('coordinates', pyarrow.list_(pyarrow.float64()))  # Coordinates
            ])),  # End of 'geometry' struct
            ('id', pyarrow.string())  # ID of the feature
        ])  # End of feature struct
    ))  # End of features list
])  # End of schema


class ExtractDataFormAPI(beam.DoFn):
    def process(self, ele, api_url):
        import requests
        import json
        response = requests.get(api_url)
        # print(response,type(response)) ##<Response [200]> <class 'requests.models.Response'>
        if response.status_code == 200:
            extracted_data = response.json()  # convert the response from the API (which is in JSON format) into a Python dictionary.
            # print(extracted_data,type(extracted_data)) ##dict
            logging.info(f"extract data successfully from {api_url}")
            yield extracted_data # Convert the Python dictionary to a JSON string
        else:
            logging.error(f"Failed to retrieve data. Status code: {response.status_code}")
            yield None

class ExtractRequiredDataAndFlatten(beam.DoFn):
    def process(self, json_str):
        import json

        # print(type(data)) #str
        data_dic = json_str

        extract_feature_lst = data_dic['features']  ## fetch feature lst from dic
        # print(extract_feature_lst,type(extract_feature_lst)) ##list

        for feature_dict in extract_feature_lst:
            properties_dic = feature_dict['properties']

            properties_dic["geometry"] = {"longitude":feature_dict["geometry"]["coordinates"][0],
                                          "latitude":feature_dict["geometry"]["coordinates"][1],
                                          "depth" : feature_dict["geometry"]["coordinates"][2]
                                           }
            yield properties_dic


class ApplyTransformation(beam.DoFn):
    def process(self, properties_dic):
        import json
        from datetime import datetime

        # print(type(properties_dic))

        ## conver UNIX timestamps( in milliseconds )to timestamp(Convert milliseconds to seconds and then to readable timestamp)

        try:
            time = float(properties_dic['time']) / 1000  ## Convert milliseconds to seconds
            if time > 0:
                utc_time = datetime.utcfromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')
            else:
                utc_time = None  # Set to None
        except Exception as e:
            logging.error(f"error while convertiong time :{e}")
            utc_time = None

        try:
            update = float(properties_dic['updated']) / 1000
            if update > 0:
                utc_updated = datetime.utcfromtimestamp(update).strftime('%Y-%m-%d %H:%M:%S')
            else:
                utc_updated = None  # Set to None or a default value if invalid

        except (ValueError, OSError) as e:
            logging.error(f"error while convertiong update time :{e}")
            utc_updated = None

        ### add column “area” - based on existing “place” column

        place_str = properties_dic.get("place", "")

        # Use 'find' to locate the first occurrence of 'of'
        index_of_of = place_str.find(' of ')

        if index_of_of != -1:
            # Extract substring after 'of'
            area_loc = place_str[index_of_of + len(' of '):].strip()
            # earthquake_data_dic["area"] = area_loc
        else:
            # earthquake_data_dic["area"] = None  # If no 'of' found, set area to None
            area_loc = place_str

        insert_date = datetime.now().timestamp()  # ) # Output: 1729842930.123456 (timestamp- datetime  convert Unix timestamp.)
        # Convert UNIX Timestamp to UTC Datetime
        insert_date_values = datetime.utcfromtimestamp(insert_date).strftime('%Y-%m-%d %H:%M:%S')
        # Prepare dictionary with required fields
        earthquake_dic = {
            "mag": properties_dic.get("mag"),
            "place": properties_dic.get("place"),
            "time": utc_time,
            "updated": utc_updated,
            "tz": properties_dic.get("tz"),
            "url": properties_dic.get("url"),
            "detail": properties_dic.get("detail"),
            "felt": properties_dic.get("felt"),
            "cdi": properties_dic.get("cdi"),
            "mmi": properties_dic.get("mmi"),
            "alert": properties_dic.get("alert"),
            "status": properties_dic.get("status"),
            "tsunami": properties_dic.get("tsunami"),
            "sig": properties_dic.get("sig"),
            "net": properties_dic.get("net"),
            "code": properties_dic.get("code"),
            "ids": properties_dic.get("ids"),
            "sources": properties_dic.get("sources"),
            "types": properties_dic.get("types"),
            "nst": properties_dic.get("nst"),
            "dmin": properties_dic.get("dmin"),
            "rms": properties_dic.get("rms"),
            "gap": properties_dic.get("gap"),
            "magType": properties_dic.get("magType"),
            "type": properties_dic.get("type"),
            "title": properties_dic.get("title"),
            "area": area_loc,
            "geometry": properties_dic.get("geometry"),
            "insert_date": insert_date_values
        }

        yield earthquake_dic

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
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\Mohini Data Science\FN_earthquake_ingestion_dataflow_parquet_dev\spark-learning-43150-3d588125392c.json"
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
    cur_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    ## bronze layer location
    landing_gcs_location = f'gs://earthquake_analysis_buck/dataflow/landing/{cur_timestamp}/earthquake'
    ## read data from landinng location
    read_data_loc = landing_gcs_location + '.parquet'
    print(f'read data path : {read_data_loc}')

    ### write data in gsc silver location
    silver_gcs_loction = f'gs://earthquake_analysis_buck/dataflow/silver/{cur_timestamp}/flatten_earthquake_data'

    ## bq table location
    bigquery_tbl_location = 'spark-learning-43150.earthquake_db.dataflow_earthquake_data'
    bigquery_audit_tbl='spark-learning-43150.earthquake_db.dataflow_audit_tbl'

    ## create job id and pipeline nam for audit table
    job_id=datetime.utcnow()
    pipeline_nm='daily'

    ## create first pipeline for  extract data from API and write extracted data in gcs(bronze layer)

    with beam.Pipeline(options=option_obj) as p:
        try:
            start_time = datetime.utcnow()
            task_name='1_extract_data_api_and_write_gcs'
            extract_data_api_and_write_gcs = (p | "StartPipeline" >> beam.Create([None])  # Initialize with dummy element
                                              | 'extract data from api' >> beam.ParDo(ExtractDataFormAPI(), api_url)
                                              | 'extracted data write to GCS' >> beam.io.WriteToParquet(landing_gcs_location,
                                                                                                        schema=raw_parquet_schema,
                                                                                                     file_name_suffix='.parquet',
                                                                                                     shard_name_template='')
                                              # |'print extracted location'>> beam.Map(print)
                                              )
            end_time = datetime.utcnow()
            status='success'
            error_msg=None
            logging.info("pipeline1 get execuated sucessfully")
        except Exception as e :
            error_msg=e
            status='fail'
            logging.error(f"pipeline1 get error {e}")
        # audit function call

        audit_event(bigquery_audit_tbl,job_id, pipeline_nm,start_time, task_name, end_time, status, error_msg)



########################################################

    with beam.Pipeline(options=option_obj) as p2:
        try:
            start_time = datetime.utcnow()
            task_name='2_read_data_from_landing_loc_apply_trans'
            read_data_from_landing_loc_apply_trans = (
                        p2 | 'Read data From GCS landing location' >> beam.io.ReadFromParquet(read_data_loc)
                        | 'fetch required data and flatten it' >> beam.ParDo(ExtractRequiredDataAndFlatten())
                        | 'apply transformation on flatten data' >> beam.ParDo(ApplyTransformation())
                        # | 'PrintReadData' >> beam.Map(lambda x: printline(x))
                        )
            end_time = datetime.utcnow()
            status = 'success'
            error_msg = None
            logging.info("pipeline2 task1 execuated sucessfully")
        except Exception as e:
            error_msg = e
            status = 'fail'
            logging.error(f"pipeline2 task1 get error {e}")

        # audit function call
        audit_event(bigquery_audit_tbl, job_id, pipeline_nm, start_time, task_name, end_time, status, error_msg)


        try:
            start_time = datetime.utcnow()
            task_name = '3_write_clean_data_into_silver_loc'
            write_clean_data_into_silver_loc = (read_data_from_landing_loc_apply_trans
                                       |'clean data write in gcs silver layer' >> beam.io.WriteToParquet
                                        (silver_gcs_loction,schema=clean_data_parquet_schema, file_name_suffix='.parquet', shard_name_template='')
                                            )
            end_time = datetime.utcnow()
            status = 'success'
            error_msg = None
            logging.info("pipeline2.1 task2 execuated sucessfully")
        except Exception as e:
            error_msg = e
            status = 'fail'
            logging.error(f"pipeline2.1 task2 get error {e}")

        # audit function call
        audit_event(bigquery_audit_tbl, job_id, pipeline_nm, start_time, task_name, end_time, status, error_msg)


        try:
            start_time = datetime.utcnow()
            task_name = '4_write_clean_data_into_bq_loc'
            write_clean_data_into_bq_loc = (read_data_from_landing_loc_apply_trans
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
            logging.error(f"pipeline2.2 task3 get error {e}")

