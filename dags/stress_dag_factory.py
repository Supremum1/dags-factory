from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

DAGS_COUNT = 10
TASKS_PER_DAG = 10

# "linear"      — task_1 -> task_2 -> task_3 -> ...
# "parallel"    — все задачи независимы
# "fan_out"     — task_1 -> все остальные
# "fan_in"      — все задачи -> final_task
DEPENDENCY_TYPE = "linear"

SCHEDULE = None
CATCHUP = False
RETRIES = 0

TASK_SLEEP_SECONDS = 5

def create_stress_dag(dag_id: str) -> DAG:
    with DAG(
        dag_id=dag_id,
        start_date=datetime(2026, 1, 1),
        schedule=SCHEDULE,
        catchup=CATCHUP,
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
                bash_command=f"echo 'Running {dag_id}.task_{task_number}' && sleep {TASK_SLEEP_SECONDS}",
            )
            tasks.append(task)

        if DEPENDENCY_TYPE == "linear":
            for i in range(len(tasks) - 1):
                tasks[i] >> tasks[i + 1]

        elif DEPENDENCY_TYPE == "parallel":
            pass

        elif DEPENDENCY_TYPE == "fan_out":
            first_task = tasks[0]
            for task in tasks[1:]:
                first_task >> task

        elif DEPENDENCY_TYPE == "fan_in":
            final_task = BashOperator(
                task_id="final_task",
                bash_command=f"echo 'Final task for {dag_id}' && sleep {TASK_SLEEP_SECONDS}",
            )

            for task in tasks:
                task >> final_task

        else:
            raise ValueError(
                "Unsupported DEPENDENCY_TYPE. "
                "Use: linear, parallel, fan_out, fan_in"
            )

        return dag


for dag_number in range(1, DAGS_COUNT + 1):
    dag_id = f"stress_generated_dag_{dag_number}"

    globals()[dag_id] = create_stress_dag(dag_id)