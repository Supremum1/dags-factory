from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator


DEFAULT_DAG_COUNT = 10
DEFAULT_TASKS_PER_DAG = 10
DEFAULT_TASK_DURATION = 5
DEFAULT_POOL_NAME = "default_pool"
DEFAULT_MAX_ACTIVE_RUNS = 1
DEFAULT_WORKLOAD_TYPE = "linear"

SCHEDULE = None
CATCHUP = False
RETRIES = 0


# CONFIG READER
# Приоритет:
# 1. Airflow Variable
# 2. env-переменная
# 3. значение по умолчанию

def get_config_value(name: str, default_value: str) -> str:
    airflow_value = Variable.get(name, default_var=None)

    if airflow_value is not None:
        return airflow_value

    env_value = os.getenv(name)

    if env_value is not None:
        return env_value

    return default_value


DAG_COUNT = int(get_config_value("STRESS_DAG_COUNT", str(DEFAULT_DAG_COUNT)))
TASKS_PER_DAG = int(get_config_value("STRESS_TASKS_PER_DAG", str(DEFAULT_TASKS_PER_DAG)))
TASK_DURATION = int(get_config_value("STRESS_TASK_DURATION", str(DEFAULT_TASK_DURATION)))
POOL_NAME = get_config_value("STRESS_POOL_NAME", DEFAULT_POOL_NAME)
MAX_ACTIVE_RUNS = int(get_config_value("STRESS_MAX_ACTIVE_RUNS", str(DEFAULT_MAX_ACTIVE_RUNS)))
WORKLOAD_TYPE = get_config_value("STRESS_WORKLOAD_TYPE", DEFAULT_WORKLOAD_TYPE)


def create_stress_dag(dag_id: str) -> DAG:
    with DAG(
        dag_id=dag_id,
        start_date=datetime(2026, 1, 1),
        schedule=SCHEDULE,
        catchup=CATCHUP,
        max_active_runs=MAX_ACTIVE_RUNS,
        default_args={
            "retries": RETRIES,
            "retry_delay": timedelta(seconds=5),
        },
        tags=["stress_test", "generated"],
    ) as dag:

        tasks = []

        for task_number in range(1, TASKS_PER_DAG + 1):
            task = BashOperator(
                task_id=f"task_{task_number}",
                bash_command=(
                    f"echo 'Running {dag_id}.task_{task_number}' "
                    f"&& sleep {TASK_DURATION}"
                ),
                pool=POOL_NAME,
            )
            tasks.append(task)

        if WORKLOAD_TYPE == "linear":
            for i in range(len(tasks) - 1):
                tasks[i] >> tasks[i + 1]

        elif WORKLOAD_TYPE == "parallel":
            pass

        elif WORKLOAD_TYPE == "fan_out":
            first_task = tasks[0]

            for task in tasks[1:]:
                first_task >> task

        elif WORKLOAD_TYPE == "fan_in":
            final_task = BashOperator(
                task_id="final_task",
                bash_command=f"echo 'Final task for {dag_id}' && sleep {TASK_DURATION}",
                pool=POOL_NAME,
            )

            for task in tasks:
                task >> final_task

        else:
            raise ValueError(
                "Unsupported STRESS_WORKLOAD_TYPE. "
                "Use one of: linear, parallel, fan_out, fan_in"
            )

        return dag


for dag_number in range(1, DAG_COUNT + 1):
    dag_id = f"stress_generated_dag_{dag_number}"

    globals()[dag_id] = create_stress_dag(dag_id)
