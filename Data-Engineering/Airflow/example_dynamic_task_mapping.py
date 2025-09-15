from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, task, get_current_context
from airflow.models.param import Param, ParamsDict

with DAG(
    dag_id="example_task_mapping_scheduled",
    tags=["example", "aie", "mapping", "sdk", "dynamic", "scheduled"],
    schedule="@hourly",
    catchup=False,
    start_date=datetime(2022, 3, 4),
    params=ParamsDict(
        {
            "numbers": Param(
                [1, 2, 3],
                type=["array"],
                description="List of numbers to process",
                minItems=1,
                items={"type": "integer"},
            ),
        }
    ),
    render_template_as_native_obj=True,
    access_control={"All": {"DAGs": {"can_read", "can_edit", "can_delete"}}},
) as dag2:

    @task
    def get_nums():
        params = get_current_context().get("params")
        if params and "numbers" in params:
            return params["numbers"]
        return [1, 2, 3]

    @task
    def times_2(num):
        return num * 2

    @task
    def add_10(num):
        return num + 10

    _get_nums = get_nums()
    _times_2 = times_2.expand(num=_get_nums)
    add_10.expand(num=_times_2)
