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
    "spark_pi",
    default_args=default_args,
    schedule=None,
    tags=["example", "aie", "spark", "pi"],
    params=ParamsDict(
        {
            "registry_url": Param(
                os.environ.get("AIRGAP_REGISTRY"),
                type=["string"],
                pattern=r"^\S+/$",
                description="Input your registry url. Trailing slash in the end is required",
            ),
        }
    ),
    render_template_as_native_obj=True,
    access_control={"All": {"DAGs": {"can_read", "can_edit", "can_delete"}}},
)

submit = SparkKubernetesOperator(
    task_id="submit",
    application_file="example_spark_pi.yaml",
    delete_on_termination=False,
    dag=dag,
    enable_impersonation_from_ldap_user=True,
)
