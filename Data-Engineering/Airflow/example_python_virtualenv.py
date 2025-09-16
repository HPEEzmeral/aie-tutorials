from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.models.param import Param, ParamsDict
from airflow.providers.standard.operators.python import PythonVirtualenvOperator


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
    tags=["example", "aie", "python", "virtualenv"],
    params=ParamsDict(
        {
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
                description="Input trusted host for pip. It should be hostname or IP address of PyPI server",
            ),
        }
    ),
    render_template_as_native_obj=True,
    access_control={"All": {"DAGs": {"can_read", "can_edit", "can_delete"}}},
) as dag:

    def callable_virtualenv():
        from time import sleep

        import emoji

        print(emoji.emojize("Python is :thumbs_up:"))
        print(emoji.emojize("Water! :water_wave:"))
        print(emoji.emojize("Airflow is :trophy:"))
        for i in range(4):
            print(
                emoji.emojize(
                    f"This is the item {i} and respective emoji: :keycap_{i}:"
                )
            )
            sleep(1)
        print(emoji.emojize("Python virtualenv is working :rocket:"))

    virtualenv_task = PipInstallTemplatedPythonVirtualenvOperator(
        task_id="virtualenv_python",
        python_callable=callable_virtualenv,
        requirements=["emoji==2.14.1"],
        system_site_packages=False,
        index_urls=["{{ params.index_url }}"],
        pip_install_options=[
            "--trusted-host",
            "{{ params.trusted_host }}",
        ],
    )
