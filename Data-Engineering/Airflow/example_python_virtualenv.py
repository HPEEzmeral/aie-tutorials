from __future__ import annotations

import pendulum

from airflow.models.param import Param
from airflow.operators.python import PythonVirtualenvOperator
from airflow import DAG


# PipInstallTemplatedPythonVirtualenvOperator is a custom operator that extends the PythonVirtualenvOperator
# to include pip_install_options as a template field.
# This allows dynamic configuration of pip install options using Airflow's templating system.
# If pip install options can be hardcoded, use the standard PythonVirtualenvOperator instead.
class PipInstallTemplatedPythonVirtualenvOperator(PythonVirtualenvOperator):
    template_fields = tuple(
        {"pip_install_options"}.union(PythonVirtualenvOperator.template_fields)
    )


with DAG(
    dag_id="example_python_virtualenv_operator",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "python", "virtualenv", "aie"],
    params={
        "index_url": Param(
            "https://pypi.org/simple",
            type="string",
            pattern=r"^https?://\S+",
            description="Input PyPI index URL. It should start with http:// or https://",
        ),
        "trusted_host": Param(
            "pypi.org",
            type="string",
            pattern=r"^\S+$",
            description="Input trusted host for pip install. It should be the hostname (or IP address) of PyPI server",
        ),
        "proxy_url": Param(
            "",
            type=["null", "string"],
            pattern=r"^$|^https?://\S+",
            description="Input HTTP(S) proxy for pip install if needed. If not needed, leave it empty.",
        ),
    },
    access_control={"All": {"can_read", "can_edit", "can_delete"}},
) as dag:

    def callable_virtualenv():
        from time import sleep
        from colorama import Back, Fore, Style

        print(Fore.RED + "some red text")
        print(Back.GREEN + "and with a green background")
        print(Style.DIM + "and in dim text")
        print(Style.RESET_ALL)
        for _ in range(4):
            print(Style.DIM + "Please wait...", flush=True)
            sleep(1)
        print("Finished")

    virtualenv_task = PipInstallTemplatedPythonVirtualenvOperator(
        task_id="virtualenv_python",
        python_callable=callable_virtualenv,
        requirements=["colorama==0.4.0"],
        system_site_packages=False,
        index_urls=["{{ params.index_url }}"],
        pip_install_options=[
            "--trusted-host",
            "{{ params.trusted_host }}",
            "--proxy",
            "{{ params.proxy_url if params.proxy_url is not none else '' }}",
        ],
    )
