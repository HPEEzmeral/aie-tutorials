from __future__ import annotations

import os
import time
from datetime import datetime

import boto3
import botocore.exceptions

from airflow import DAG
from airflow.models.param import Param, ParamsDict
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "max_active_runs": 1,
    "retries": 0,
}

dag = DAG(
    "s3_transfer_data_native",
    default_args=default_args,
    schedule=None,
    tags=["example", "aie", "s3", "transfer", "boto"],
    params=ParamsDict(
        {
            "s3_endpoint": Param(
                "local-s3-service.ezdata-system.svc.cluster.local:30000",
                type="string",
                description="Local S3 endpoint to download or upload data from/to",
            ),
            "s3_endpoint_ssl_enabled": Param(
                False, type="boolean", description="Whether to use SSL for S3 endpoint"
            ),
            "s3_bucket": Param(
                "ezaf-demo",
                type="string",
                description="S3 bucket to download or upload data from/to",
            ),
            "s3_path": Param(
                "data/financial.csv",
                type="string",
                description="S3 key to download or upload data from/to",
            ),
            "local_file_path": Param(
                "/mnt/shared/aie-tutorials/current-release/Data-Science/Kubeflow/Financial-Time-Series/dataset/financial.csv",
                type="string",
                description="Local path to upload to S3 or download from S3",
            ),
            "mode": Param(
                "upload",
                type="string",
                enum=["upload", "download"],
                description="Whether to upload or download file from S3",
            ),
        }
    ),
    render_template_as_native_obj=True,
    access_control={"All": {"DAGs": {"can_read", "can_edit", "can_delete"}}},
)


def get_token():
    with open("/etc/secrets/ezua/.auth_token") as f:
        return f.read().strip()


def get_s3_client(endpoint_host: str, ssl_enabled: bool):
    endpoint_url = f"http{'s' if ssl_enabled else ''}://{endpoint_host}"
    jwt_token = get_token()
    s3 = boto3.client(
        "s3",
        aws_access_key_id=jwt_token,
        aws_secret_access_key="s3",
        endpoint_url=endpoint_url,
        use_ssl=ssl_enabled,
    )
    return s3


def create_bucket_if_not_exists(s3, bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            s3.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' created.")
        else:
            print(f"An error occurred: {str(e)}")


def upload_file_to_s3(s3, local_file_path, bucket_name, s3_file_key):
    s3_full_path = f"s3://{bucket_name}/{s3_file_key}"
    start_time = time.time()
    try:
        s3.upload_file(local_file_path, bucket_name, s3_file_key)
        end_time = time.time()
        dt = end_time - start_time
        print(f"File '{local_file_path}' uploaded to '{s3_full_path}' in {dt:.2f} s.")
    except botocore.exceptions.ClientError as e:
        print(f"An error occurred while uploading '{local_file_path}': {str(e)}")


def download_file_from_s3(s3, bucket_name, s3_file_key, local_file_path):
    # Create parent directories if they do not exist
    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
    s3_full_path = f"s3://{bucket_name}/{s3_file_key}"
    start_time = time.time()
    try:
        s3.download_file(bucket_name, s3_file_key, local_file_path)
        end_time = time.time()
        dt = end_time - start_time
        print(f"File '{s3_full_path}' downloaded to '{local_file_path}' in {dt:.2f} s.")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print(f"The object '{s3_full_path}' does not exist.")
        else:
            print(f"An error occurred while downloading '{s3_full_path}': {str(e)}")


def callable_s3_data_transfer(
    s3_endpoint,
    s3_endpoint_ssl_enabled,
    mode,
    local_file_path,
    bucket_name,
    s3_file_key,
):
    s3 = get_s3_client(s3_endpoint, s3_endpoint_ssl_enabled)

    if mode == "upload":
        create_bucket_if_not_exists(s3, bucket_name)
        upload_file_to_s3(s3, local_file_path, bucket_name, s3_file_key)
    elif mode == "download":
        download_file_from_s3(s3, bucket_name, s3_file_key, local_file_path)


s3_transfer_task = PythonOperator(
    task_id="data_transfer",
    python_callable=callable_s3_data_transfer,
    op_args=[
        "{{ params.s3_endpoint }}",
        "{{ params.s3_endpoint_ssl_enabled }}",
        "{{ params.mode }}",
        "{{ params.local_file_path }}",
        "{{ params.s3_bucket }}",
        "{{ params.s3_path }}",
    ],
    dag=dag,
)
