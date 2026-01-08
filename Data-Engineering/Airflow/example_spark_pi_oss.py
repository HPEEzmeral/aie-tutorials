from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param, ParamsDict
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

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
    "spark_pi_oss",
    default_args=default_args,
    schedule=None,
    tags=["example", "aie", "spark", "pi"],
    params=ParamsDict(
        {
            "spark_image_url": Param(
                f"{os.environ.get('AIRGAP_REGISTRY')}hpe-spark/apache-spark:v3.5.5.7",
                type=["string"],
                description="Provide Python-Spark image url",
            ),
            "spark_image_version": Param(
                "3.5.5.7",
                type=["null", "string"],
                description="Provide Spark image Version",
            ),
        }
    ),
    render_template_as_native_obj=True,
    access_control={"All": {"DAGs": {"can_read", "can_edit", "can_delete"}}},
)

submit = SparkKubernetesOperator(
    task_id="submit",
    application_file="example_spark_pi_oss.yaml",
    delete_on_termination=False,
    dag=dag,
    enable_impersonation_from_ldap_user=True,
)
