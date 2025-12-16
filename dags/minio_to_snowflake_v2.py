import os
import glob
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta

# Configuration
BUCKET_NAME = "bronze-transactions"
SOURCE_FOLDER = ""
ARCHIVE_FOLDER = "archive/"
LOCAL_DIR = "/tmp/minio_downloads"
SNOWFLAKE_TABLE = "bronze_stock_quotes_raw"
SNOWFLAKE_STAGE = f"@%{SNOWFLAKE_TABLE}"
SNOWFLAKE_CONN_ID = "snowflake_conn"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 9, 9),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def upload_to_stage(**context):
    """
    Task 1: Downloads from MinIO and PUTs to Snowflake Internal Stage.
    Does NOT load into the table yet.
    """
    s3_hook = S3Hook(aws_conn_id="minio_conn")
    snow_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    # 1. List Files
    keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=SOURCE_FOLDER)
    # Filter for json files that are NOT in archive
    processing_keys = [
        k for k in keys if not k.startswith(ARCHIVE_FOLDER) and k.endswith(".json")
    ]

    # Process max 1000 files per run to avoid memory issues
    processing_keys = processing_keys[:1000]

    if not processing_keys:
        print("No new files found. Skipping.")
        # We push 'False' to XCom so the next task knows whether to run or skip
        return False

    print(f"Preparing to move {len(processing_keys)} files to Snowflake Stage...")
    os.makedirs(LOCAL_DIR, exist_ok=True)

    # 2. Batch Download
    downloaded_files = []
    for key in processing_keys:
        actual_path = s3_hook.download_file(
            key=key,
            bucket_name=BUCKET_NAME,
            local_path=LOCAL_DIR,
            preserve_file_name=True,
        )
        downloaded_files.append(actual_path)

    # 3. Batch Upload (PUT)
    # This puts the files into the "Waiting Room" (Stage) inside Snowflake
    print("Uploading to Snowflake Stage...")
    upload_sql = f"PUT file://{LOCAL_DIR}/*.json {SNOWFLAKE_STAGE} auto_compress=True overwrite=True"
    snow_hook.run(upload_sql)

    # 4. Cleanup & Archive (MinIO)
    print("Archiving files in MinIO...")
    for key in processing_keys:
        new_key = f"{ARCHIVE_FOLDER}{os.path.basename(key)}"
        s3_hook.copy_object(key, new_key, BUCKET_NAME, BUCKET_NAME)
        s3_hook.delete_objects(BUCKET_NAME, key)

    # 5. Cleanup Local Disk
    for f in downloaded_files:
        if os.path.exists(f):
            os.remove(f)

    return True  # Tell Task 2 that we have data


with DAG(
    "minio_to_snowflake_2_tasks",
    default_args=default_args,
    schedule="*/5 * * * *",
    catchup=False,
) as dag:

    # --- TASK 1: Move Data (Python) ---
    move_data_task = PythonOperator(
        task_id="move_files_to_stage",
        python_callable=upload_to_stage,
    )

    # --- TASK 2: Load Data (SQL) ---
    # This runs the COPY command.
    # It will pick up ANY file sitting in the stage that hasn't been loaded yet.
    load_table_task = SQLExecuteQueryOperator(
        task_id="copy_into_table",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            COPY INTO {SNOWFLAKE_TABLE}
            FROM {SNOWFLAKE_STAGE}
            FILE_FORMAT = (TYPE = JSON)
            PATTERN = '.*.json.gz' 
            ON_ERROR = 'CONTINUE';
        """,
    )

    move_data_task >> load_table_task
