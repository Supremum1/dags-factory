# Stress DAG Factory

## Описание

`stress_dag_factory.py` — простой DAG-файл для стресс-теста Airflow.

Он создаёт несколько DAG'ов в цикле.  
В каждом DAG создаётся несколько задач `BashOperator`.

Файл нужен, чтобы проверить, как Airflow ведёт себя при большом количестве DAG'ов и задач.

## Основные параметры

```python
DAGS_COUNT = 10
TASKS_PER_DAG = 10
DEPENDENCY_TYPE = "linear"
SCHEDULE = None
CATCHUP = False
RETRIES = 0
TASK_SLEEP_SECONDS = 5
