from __future__ import annotations

from datetime import datetime

from airflow.models.param import Param, ParamsDict
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, get_current_context, task

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
            "threshold": Param(
                40,
                type="integer",
                description="Threshold to trigger the other DAG",
            ),
        }
    ),
    render_template_as_native_obj=True,
    access_control={"All": {"DAGs": {"can_read", "can_edit", "can_delete"}}},
) as dag:

    @task
    def get_nums():
        params = get_current_context().get("params")
        if params and "numbers" in params:
            return params["numbers"]
        return [1, 2, 3]

    @task
    def times_2(num: int):
        return num * 2

    @task
    def add_10(num: int):
        return num + 10

    @task
    def sum_it(values):
        return sum(values)

    @task
    def get_threshold():
        params = get_current_context().get("params")
        if params and "threshold" in params:
            return params["threshold"]
        return 40

    @task.short_circuit
    def check_sum(sum, threshold):
        print(f"Total sum is {sum}, threshold is {threshold}")
        if sum >= threshold:
            print("Threshold reached, continuing the workflow")
            return True
        else:
            print("Threshold not reached, stopping here")
            return False

    trigger_task = TriggerDagRunOperator(
        task_id="trigger_other_dag",
        trigger_dag_id="example_retrying_task",
        conf={"number": "{{ ti.xcom_pull('sum_it') }}"},
    )

    _nums = get_nums()
    _times_2 = times_2.expand(num=_nums)
    _added_values = add_10.expand(num=_times_2)
    _threshold = get_threshold()
    _total = sum_it(values=_added_values)
    check_sum_task = check_sum(sum=_total, threshold=_threshold)
    check_sum_task >> trigger_task  # type: ignore
