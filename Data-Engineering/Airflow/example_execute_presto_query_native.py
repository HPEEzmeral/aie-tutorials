from __future__ import annotations

import decimal
from datetime import datetime

import jwt
import requests
import urllib3
from pyhive import presto

from airflow import DAG
from airflow.models.param import Param, ParamsDict
from airflow.providers.standard.operators.python import (
    PythonOperator,
    get_current_context,
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
    "presto_query_dag_native",
    default_args=default_args,
    schedule=None,
    tags=["example", "aie", "presto", "query"],
    params=ParamsDict(
        {
            "host": Param(
                "ezpresto-svc-https-locator.ezpresto.svc.cluster.local",
                type="string",
                description="Presto host",
            ),
            "port": Param(
                8081,
                type="integer",
                description="Presto port",
            ),
            "protocol": Param(
                "https",
                type="string",
                enum=["http", "https"],
                description="Presto protocol",
            ),
            "catalog": Param(
                "customdemocatalog01",
                type="string",
                description="Presto catalog",
            ),
            "schema": Param(
                "public",
                type="string",
                description="Presto schema",
            ),
            "query": Param(
                "SELECT * FROM customer LIMIT 10",
                type="string",
                description="Presto query to execute",
            ),
        }
    ),
    render_template_as_native_obj=True,
    access_control={"All": {"DAGs": {"can_read", "can_edit", "can_delete"}}},
)


def get_token():
    with open("/etc/secrets/ezua/.auth_token") as f:
        return f.read().strip()


def callable_execute_presto_query():
    jwt_token = get_token()
    decoded_token = jwt.decode(jwt_token, options={"verify_signature": False})
    username_from_token = decoded_token.get("preferred_username", "")

    params = get_current_context()["params"]
    host = params["host"]
    port = params["port"]
    protocol = params["protocol"]
    catalog = params["catalog"]
    schema = params["schema"]
    user = username_from_token
    query = params["query"]

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    session = requests.Session()
    session.verify = False
    session.headers.update({"Authorization": f"Bearer {jwt_token}"})

    original_post = requests.post
    original_get = requests.get

    def patched_post(*args, **kwargs):
        kwargs["verify"] = False
        kwargs.setdefault("headers", {}).update(session.headers)
        return original_post(*args, **kwargs)

    def patched_get(*args, **kwargs):
        kwargs["verify"] = False
        kwargs.setdefault("headers", {}).update(session.headers)
        return original_get(*args, **kwargs)

    requests.post = patched_post
    requests.get = patched_get

    conn = presto.connect(
        host=host,
        port=port,
        catalog=catalog,
        schema=schema,
        username=user,
        protocol=protocol,
    )

    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()

    data_table = []
    for row in result:
        data_row = []
        for item in row:
            if isinstance(item, decimal.Decimal):
                data_row.append(float(item))
            else:
                data_row.append(item)
        data_table.append(data_row)
    return data_table


def callable_print_data_table(data_table):
    print("Query Result:")
    for row in data_table:
        for item in row:
            print(f"{str(item)[:20]:<20} | ", end="", flush=True)
        print()
    print("End of Query Result")


presto_query_task = PythonOperator(
    task_id="execute_presto_query",
    python_callable=callable_execute_presto_query,
    dag=dag,
)

print_data_table_task = PythonOperator(
    task_id="print_data_table",
    python_callable=callable_print_data_table,
    op_args=[presto_query_task.output],
    dag=dag,
)

presto_query_task >> print_data_table_task  # type: ignore
