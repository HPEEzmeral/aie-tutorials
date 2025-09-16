from __future__ import annotations

import os
import random

import pendulum
from kubernetes.client import models as k8s

from airflow.models.param import Param, ParamsDict
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.sensors.time import TimeSensor
from airflow.sdk import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "aie", "kubernetes", "pod", "trigger", "sensor"],
    params=ParamsDict(
        {
            "registry_url": Param(
                os.environ.get("AIRGAP_REGISTRY"),
                type="string",
                pattern=r"^\S+/$",
                description="Input your registry url. Trailing slash in the end is required",
            ),
            "container_image": Param(
                "docker.io/alpine:3.22",
                type="string",
                description="Container image to run inside Kubernetes Pod",
            ),
            "echo_text": Param(
                "Hello from the Kubernetes Pod!",
                type="string",
                description="Text to echo inside the container",
            ),
            "file_path_to_read": Param(
                "/mnt/shared/aie-tutorials/current-release/Data-Engineering/Airflow/README.md",
                type="string",
                description="File path to read inside the container",
            ),
        }
    ),
    render_template_as_native_obj=True,
    access_control={"All": {"DAGs": {"can_read", "can_edit", "can_delete"}}},
)
def example_kubernetes_executor():

    time_sensor = TimeSensor(
        task_id="wait_until_time_sensor",
        target_time=pendulum.now().add(seconds=random.randint(61, 120)).time(),
        deferrable=True,
        start_from_trigger=True,
        end_from_trigger=True,
    )

    kubernetes_task = KubernetesPodOperator(
        task_id="kubernetes_task_one",
        name="sample-k8s-pod",
        image="{{ params.registry_url }}{{ params.container_image }}",
        labels={
            "sidecar.istio.io/inject": "false",
        },
        cmds=["sh", "-c"],
        arguments=[
            'echo {{ params.echo_text }}; echo \'{"date": "\'"$(date)"\'"}\' > /airflow/xcom/return.json'
        ],
        do_xcom_push=True,
        get_logs=True,
        on_finish_action="keep_pod",
        volumes=get_volumes(),
        volume_mounts=get_volume_mounts(),
    )

    @task.kubernetes_cmd(
        name="execute-shell-cmd",
        image="{{ params.registry_url }}{{ params.container_image }}",
        labels={
            "sidecar.istio.io/inject": "false",
        },
        do_xcom_push=True,
        get_logs=True,
        on_finish_action="keep_pod",
        volumes=get_volumes(),
        volume_mounts=get_volume_mounts(),
    )
    def kubernetes_task_from_sdk():
        command = """
        cat {{ params.file_path_to_read }};
        echo '{"date": "'"$(date)"'"}' > /airflow/xcom/return.json
        """
        return ["sh", "-c", command]

    time_sensor >> kubernetes_task >> kubernetes_task_from_sdk()  # type: ignore


def get_volumes():
    return [
        k8s.V1Volume(
            name="platform-shared-data",
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                claim_name="kubeflow-shared-pvc",
            ),
        ),
        k8s.V1Volume(
            name="user-personal-data",
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                claim_name="user-pvc",
            ),
        ),
    ]


def get_volume_mounts():
    return [
        k8s.V1VolumeMount(
            name="platform-shared-data",
            mount_path="/mnt/shared",
        ),
        k8s.V1VolumeMount(
            name="user-personal-data",
            mount_path="/mnt/user",
        ),
    ]


example_kubernetes_executor()
