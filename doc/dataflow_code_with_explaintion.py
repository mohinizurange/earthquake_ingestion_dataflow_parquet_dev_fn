import apache_beam as beam  # Use Apache Beam to process data.
import pyarrow  # Use PyArrow to handle Arrow data (like Parquet files).
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions,WorkerOptions  # Import options to set up the pipeline in Google Cloud.#Import pipeline options for configuring the Dataflow job.
import os  # Use the os module to work with files and folders on your computer.
import logging  # Use logging to record messages for debugging or tracking.
# from util import Utils  # (This is commented out) Import custom helper functions from the 'util' file.
from datetime import datetime  # Use datetime to work with dates and times.
import requests  # Use requests to make web requests to APIs.
import json  # Use json to work with JSON data (like reading or writing it).
import argparse  # Use argparse to handle command-line inputs.

# Define the complete schema for the GeoJSON data structure

raw_parquet_schema = pyarrow.schema([  # Create the schema for the GeoJSON data using PyArrow
    ('type', pyarrow.string()),  # Define the 'type' field (e.g., 'FeatureCollection') as a string
    ('metadata',
     pyarrow.struct([  # Define a 'metadata' field that contains various sub-fields (like 'generated', 'status')
         ('generated', pyarrow.int64()),  # 'generated' field as a 64-bit integer
         ('url', pyarrow.string()),  # 'url' field as a string
         ('title', pyarrow.string()),  # 'title' field as a string
         ('status', pyarrow.int64()),  # 'status' field as a 64-bit integer
         ('api', pyarrow.string()),  # 'api' field as a string
         ('count', pyarrow.int64())  # 'count' field as a 64-bit integer
     ])),  # End of 'metadata' struct
    ('features', pyarrow.list_(  # 'features' field is a list of feature structs
        pyarrow.struct([  # Each feature has a structure with various sub-fields
            ('type', pyarrow.string()),  # 'type' field as a string
            ('properties', pyarrow.struct([  # 'properties' field is a struct with multiple sub-fields
                ('mag', pyarrow.float64()),  # Magnitude of the event as a float
                ('place', pyarrow.string()),  # Location of the event as a string
                ('time', pyarrow.int64()),  # Event time as a 64-bit integer
                ('updated', pyarrow.int64()),  # Last update time as a 64-bit integer
                ('tz', pyarrow.int64()),  # Timezone as a 64-bit integer
                ('url', pyarrow.string()),  # URL for the event as a string
                ('detail', pyarrow.string()),  # Event detail URL as a string
                ('felt', pyarrow.float64()),  # Felt reports as a float
                ('cdi', pyarrow.float64()),  # Community intensity data as a float
                ('mmi', pyarrow.float64()),  # Maximum intensity as a float
                ('alert', pyarrow.string()),  # Alert level as a string
                ('status', pyarrow.string()),  # Event status as a string
                ('tsunami', pyarrow.int64()),  # Tsunami indicator as a 64-bit integer
                ('sig', pyarrow.int64()),  # Significance of the event as a 64-bit integer
                ('net', pyarrow.string()),  # Network as a string
                ('code', pyarrow.string()),  # Event code as a string
                ('ids', pyarrow.string()),  # IDs associated with the event as a string
                ('sources', pyarrow.string()),  # Sources as a string
                ('types', pyarrow.string()),  # Types of event data as a string
                ('nst', pyarrow.int64()),  # Number of stations as a 64-bit integer
                ('dmin', pyarrow.float64()),  # Minimum distance to the event as a float
                ('rms', pyarrow.float64()),  # Root Mean Square as a float
                ('gap', pyarrow.int64()),  # Gap as a 64-bit integer
                ('magType', pyarrow.string()),  # Magnitude type as a string
                ('type', pyarrow.string()),  # Type of earthquake as a string
                ('title', pyarrow.string())  # Title of the event as a string
            ])),  # End of 'properties' struct
            ('geometry', pyarrow.struct([  # 'geometry' contains type and coordinates
                ('type', pyarrow.string()),  # Type of geometry as a string
                ('coordinates', pyarrow.list_(pyarrow.float64()))  # Coordinates as a list of floats
            ])),  # End of 'geometry' struct
            ('id', pyarrow.string())  # ID of the feature as a string
        ])  # End of feature struct
    ))  # End of features list
])  # End of schema


# Class to fetch data from an API
class ExtractDataFormAPI(beam.DoFn):
    def process(self, ele, api_url):
        import requests  # Import the requests library to fetch data from the API
        import json  # Import json to parse the response

        response = requests.get(api_url)  # Send GET request to the API
        if response.status_code == 200:  # If the response is successful (status code 200)
            extracted_data = response.json()  # Convert the API response to JSON (Python dictionary)
            logging.info(f"extract data successfully from {api_url}")  # Log success
            yield extracted_data  # Yield the extracted data (convert to a JSON string)
        else:
            logging.error(f"Failed to retrieve data. Status code: {response.status_code}")  # Log error if request fails
            yield None  # Yield None in case of failure


# Class to extract and flatten the required data from the API response
class ExtractRequiredDataAndFlatten(beam.DoFn):
    def process(self, json_str):
        import json  # Import json to process the JSON string
        data_dic = json_str  # Convert string to dictionary

        extract_feature_lst = data_dic['features']  # Extract list of features from the response

        for feature_dict in extract_feature_lst:
            properties_dic = feature_dict['properties']  # Extract the 'properties' from each feature

            # Add 'geometry' field with longitude, latitude, and depth
            properties_dic["geometry"] = {"longitude": feature_dict["geometry"]["coordinates"][0],
                                          "latitude": feature_dict["geometry"]["coordinates"][1],
                                          "depth": feature_dict["geometry"]["coordinates"][2]}
            yield properties_dic  # Yield the transformed data


# Class to apply transformations to the extracted data
class ApplyTransformation(beam.DoFn):
    def process(self, properties_dic):
        from datetime import datetime  # Import datetime to handle timestamps

        try:
            # Convert 'time' from UNIX timestamp (milliseconds) to readable UTC time
            time = float(properties_dic['time']) / 1000  # Convert to seconds
            if time > 0:
                utc_time = datetime.utcfromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')  # Format as UTC datetime
            else:
                utc_time = None  # Set to None if invalid
        except Exception as e:
            logging.error(f"error while converting time :{e}")  # Log any errors during conversion
            utc_time = None

        try:
            # Convert 'updated' timestamp (milliseconds) to UTC datetime
            update = float(properties_dic['updated']) / 1000
            if update > 0:
                utc_updated = datetime.utcfromtimestamp(update).strftime('%Y-%m-%d %H:%M:%S')
            else:
                utc_updated = None  # Set to None if invalid
        except (ValueError, OSError) as e:
            logging.error(f"error while converting update time :{e}")  # Log any errors
            utc_updated = None

        # Add a new field "area" based on 'place'
        place_str = properties_dic.get("place", "")
        index_of_of = place_str.find(' of ')  # Find 'of' in the place string

        if index_of_of != -1:  # If 'of' is found, extract the area part
            area_loc = place_str[index_of_of + len(' of '):].strip()
        else:
            area_loc = place_str  # If not found, use the full place as area

        # Generate an insert timestamp
        insert_date = datetime.now().timestamp()
        insert_date_values = datetime.utcfromtimestamp(insert_date).strftime('%Y-%m-%d %H:%M:%S')

        # Create a dictionary with the required transformed fields
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

        yield earthquake_dic  # Yield the final dictionary with transformed data

clean_data_parquet_schema = pyarrow.schema([
            ('mag', pyarrow.float64()),  # Magnitude of the earthquake
            ('place', pyarrow.string()),  # Location where the earthquake occurred
            ('time', pyarrow.string()),  # Timestamp (in seconds)
            ('updated', pyarrow.string()),  # Timestamp (in seconds)
            ('tz', pyarrow.string()),  # Timezone information
            ('url', pyarrow.string()),  # URL to the detailed report
            ('detail', pyarrow.string()),  # Detailed description of the event
            ('felt', pyarrow.float64()),  # Intensity felt at various locations
            ('cdi', pyarrow.float64()),  # Community internet intensity map value
            ('mmi', pyarrow.float64()),  # Modified Mercalli Intensity
            ('alert', pyarrow.string()),  # Alert level
            ('status', pyarrow.string()),  # Event status
            ('tsunami', pyarrow.int64()),  # Tsunami flag (1 = Yes, 0 = No)
            ('sig', pyarrow.int64()),  # Signal strength
            ('net', pyarrow.string()),  # Network reporting the event
            ('code', pyarrow.string()),  # Event code
            ('ids', pyarrow.string()),  # Event IDs
            ('sources', pyarrow.string()),  # Sources of information
            ('types', pyarrow.string()),  # Types of event
            ('nst', pyarrow.int64()),  # Number of stations reporting
            ('dmin', pyarrow.float64()),  # Minimum distance to event
            ('rms', pyarrow.float64()),  # Root mean square
            ('gap', pyarrow.int64()),  # Gap in data
            ('magType', pyarrow.string()),  # Type of magnitude
            ('type', pyarrow.string()),  # Type of event
            ('title', pyarrow.string()),  # Title or name of the event
            ('area', pyarrow.string()),  # Area of the event
            ('geometry', pyarrow.struct([  # Geographical information (nested struct)
                ('longitude', pyarrow.float64()),  # Longitude of the event
                ('latitude', pyarrow.float64()),  # Latitude of the event
                ('depth', pyarrow.float64())  # Depth of the event
            ])),
            ('insert_date', pyarrow.string())  # Timestamp of when the event was inserted
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
        {"name": "geometry", "type": "RECORD", "fields": [  # Nested record for geometry
            {"name": "longitude", "type": "FLOAT"},
            {"name": "latitude", "type": "FLOAT"},
            {"name": "depth", "type": "FLOAT"}
        ]},
        {"name": "insert_date", "type": "TIMESTAMP"}
    ]
}
if __name__ == '__main__':

    # Set up the pipeline options (configuration)
    option_obj = PipelineOptions()  # Create an empty pipeline options object to hold the settings

    # Set specific Google Cloud options like project details, job name, and regions
    google_cloud = option_obj.view_as(GoogleCloudOptions)  # Access the Google Cloud specific settings
    google_cloud.project = 'spark-learning-43150'  # Set the project ID where the pipeline will run
    google_cloud.job_name = "earthquake-ingestion-data"  # Set the name for the pipeline job
    google_cloud.region = "us-central1"  # Set the region to run the job
    google_cloud.staging_location = "gs://earthquake_df_temp_bk/stagging_loc"  # Temporary location in GCS for the pipeline's data
    google_cloud.temp_location = "gs://earthquake_df_temp_bk/temp_loc"  # Another GCS location for temporary files during the pipeline's run

    # Set up logging to show information during the pipeline execution
    logging.basicConfig(level=logging.INFO)  # Set the log level to INFO for important messages

    # Define the API URL from which we will fetch earthquake data
    api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"  # URL to get earthquake data

    # Get the current date and time to make the file names unique
    cur_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')  # Get the current date and time in 'YYYYMMDD_HHMMSS' format

    # Set the GCS location to store raw data (first layer, called "bronze")
    landing_gcs_location = f'gs://earthquake_analysis_buck/dataflow/landing/{cur_timestamp}/earthquake'  # GCS path to save raw data

    # Create the file path to read the data later from the landing location
    read_data_loc = landing_gcs_location + '.parquet'  # Location of the raw Parquet data

    # Set the GCS location where we will save the cleaned data (second layer, called "silver")
    silver_gcs_location = f'gs://earthquake_analysis_buck/dataflow/silver/{cur_timestamp}/flatten_earthquake_data'  # GCS path for cleaned data

    # Set the BigQuery table where the final cleaned data will go
    bigquery_tbl_location = 'spark-learning-43150.earthquake_db.dataflow_earthquake_data'  # BigQuery table where data will be stored

    # First pipeline: Fetch earthquake data from the API and save it to GCS
    with beam.Pipeline(options=option_obj) as p:  # Start the pipeline with the options we defined
        # Initialize the pipeline with a dummy element to trigger it
        extract_data_api_and_write_gcs = (
                    p | "StartPipeline" >> beam.Create([None])  # Start the pipeline with a dummy element
                    | 'extract data from api' >> beam.ParDo(ExtractDataFormAPI(), api_url)  # Fetch data from the API
                    | 'extracted data write to GCS' >> beam.io.WriteToParquet(landing_gcs_location,
                                                                              # Save data to GCS as Parquet file
                                                                              schema=raw_parquet_schema,
                                                                              # Define the structure of the data
                                                                              file_name_suffix='.parquet')
                    # Set the file extension to .parquet
                    )

        # Log that the first pipeline has completed
        logging.info("First pipeline executed successfully")  # Print a message when the first pipeline finishes

    # Second pipeline: Read raw data from GCS, transform it, and save it in both GCS (Silver) and BigQuery
    with beam.Pipeline(options=option_obj) as p2:  # Start the second pipeline with the same options
        # Read the raw data from GCS, flatten it, and apply any necessary transformations
        read_data_from_landing_loc_apply_trans = (
                p2 | 'Read data From GCS landing location' >> beam.io.ReadFromParquet(read_data_loc)  # Read the raw data from the landing location in GCS
                | 'fetch required data and flatten it' >> beam.ParDo(ExtractRequiredDataAndFlatten())  # Flatten the data to a simpler form
                |'apply transformation on flatten data' >> beam.ParDo(ApplyTransformation())# Apply additional cleaning or transformations to the data
        )

        # Save the cleaned data into GCS (Silver layer) for further use
        write_clean_data_into_silver_loc = (read_data_from_landing_loc_apply_trans
                                            | 'clean data write in GCS silver layer' >> beam.io.WriteToParquet(silver_gcs_location,  # Save cleaned data to GCS
                                                                                                                schema=clean_data_parquet_schema,  # Define the schema for cleaned data
                                                                                                                file_name_suffix='.parquet')  # Set the file extension to .parquet
                                            )

        # Save the cleaned data into BigQuery for long-term storage
        write_clean_data_into_bq_loc = (read_data_from_landing_loc_apply_trans
                                        | "write into BigQuery" >> beam.io.WriteToBigQuery(
                    table=bigquery_tbl_location,  # Define the BigQuery table for final storage
                    schema='SCHEMA_AUTODETECT',  # Let BigQuery automatically detect the schema based on the data
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND, # Append the new data to the existing data in BigQuery
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED # Create the BigQuery table if it doesn't already exist
                ))

        # Log that the second pipeline has completed
        logging.info("Second pipeline executed successfully")  # Print a message when the second pipeline finishes


#####airflow

# Import necessary modules for Airflow and DAG creation
from airflow import models  # Airflow models import for DAG creation
from airflow import DAG  # Import DAG class for defining workflows
import airflow  # Import Airflow core for various functionalities
from datetime import datetime, timedelta  # For setting the execution time and schedule
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator  # Import operator to trigger Apache Beam pipeline
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType  # Import to define the Beam runner (DataflowRunner)

# Import dummy operators for defining start and end tasks
from airflow.operators.dummy_operator import DummyOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'Airflow',  # Owner of the DAG (typically the user or team name)
    'retries': 1,  # Number of retries allowed for task failure
    'retry_delay': timedelta(seconds=50),  # Delay between retries (50 seconds in this case)
    # Default options for running Dataflow jobs through Apache Beam
    'dataflow_default_options': {
        'project': 'spark-learning-43150',  # GCP project where the Dataflow job will be executed
        'region': 'us-central1',  # GCP region where Dataflow job will run
        'runner': 'DataflowRunner'  # Specify the Beam runner to use (Dataflow in this case)
    }
}

# Create the DAG object and configure it
dag = DAG(
    dag_id='dataflow_earthquake_pipeline_daily_schedule',  # Unique identifier for this DAG
    default_args=default_args,  # Set the default arguments for this DAG
    schedule_interval=None,  # The DAG will not run on a schedule, only when triggered manually
    start_date=datetime(2024, 10, 4),  # The date the DAG should start executing
    catchup=False,  # Do not backfill the missed executions for past dates
    description="DAG for data ingestion and transformation"  # A short description of what the DAG does
)

# Define the start task of the DAG (a dummy task to mark the start)
start = DummyOperator(
    task_id="start_task_id",  # Unique task identifier for the start task
    dag=dag  # Assign this task to the defined DAG
)

# Define the DataFlow task using the BeamRunPythonPipelineOperator
dataflow_task = BeamRunPythonPipelineOperator(
    task_id="dataproc_daily",  # Unique task identifier for the Dataflow task
    dag=dag,  # Assign this task to the defined DAG
    gcp_conn_id="gcp_connection",  # The GCP connection ID to authenticate with Google Cloud services
    runner=BeamRunnerType.DataflowRunner,  # Specify that the Beam runner to be used is DataflowRunner
    py_file="gs://earthquake_analysis_buck/dataflow/dataflow_code/dataflow_earthquake_pipeline_parquet_daily.py",  # Path to the Python script that defines the Beam pipeline
)

# Define the end task of the DAG (a dummy task to mark the end)
end = DummyOperator(
    task_id="end_task_id",  # Unique task identifier for the end task
    dag=dag  # Assign this task to the defined DAG
)

# Define the task sequence (start -> dataflow_task -> end)
start >> dataflow_task >> end  # This ensures that the tasks will be executed in this order










