from __future__ import annotations

import random
from datetime import datetime, timedelta

from airflow.models.param import Param, ParamsDict
from airflow.sdk import dag, get_current_context, task


@dag(
    tags=["example", "aie", "mapping", "sdk", "dynamic", "retries"],
    schedule=None,
    start_date=datetime(2022, 3, 4),
    params=ParamsDict(
        {
            "number": Param(
                42,
                type="integer",
                description="Number to execute some operations on it",
            ),
        }
    ),
    render_template_as_native_obj=True,
    access_control={"All": {"DAGs": {"can_read", "can_edit", "can_delete"}}},
)
def example_retrying_task():

    @task
    def get_number():
        params = get_current_context().get("params")
        if params and "number" in params:
            return params["number"]
        return 42

    @task
    def split_into_digits(number):
        return [int(d) for d in str(abs(number))]

    @task(retries=10, retry_delay=timedelta(seconds=3))
    def add_random_value_or_fail(digit: int):
        if random.random() < 0.6:
            raise ValueError("Random failure, please retry")
        return digit + random.randint(1, 10)

    @task
    def product(values):
        result = 1
        for v in values:
            result *= v
        return result

    _num = get_number()
    _digits = split_into_digits(number=_num)
    _added = add_random_value_or_fail.expand(digit=_digits)
    product(values=_added)


example_retrying_task()
