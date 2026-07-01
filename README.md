# Stress DAG Factory

## Описание

`stress_dag_factory.py` — фабрика DAG'ов для стресс-теста Airflow.

Параметры: количество DAG'ов, количество задач в DAG, тип зависимости между задачами, schedule, catchup, retries.

Файл создаёт несколько DAG'ов в цикле.  
В каждом DAG создаётся несколько задач `BashOperator`.

## Основные параметры

```python
DAGS_COUNT = 10
TASKS_PER_DAG = 10
DEPENDENCY_TYPE = "linear"
SCHEDULE = None
CATCHUP = False
RETRIES = 0
TASK_SLEEP_SECONDS = 5
```
## Тест

```
docker compose exec airflow-scheduler airflow dags list | findstr stress_generated
docker compose exec airflow-scheduler airflow dags trigger stress_generated_dag_1
```


![Linear DAG](linear.png)

![Screenshot 10:19:23](Снимок%20экрана%202026-07-01%20101923.png)

![Screenshot 10:22:16](Снимок%20экрана%202026-07-01%20102216.png)

![Screenshot 10:23:57](Снимок%20экрана%202026-07-01%20102357.png)

![Screenshot 10:41:31](Снимок%20экрана%202026-07-01%20104131.png)


## Параметризованная стресс-нагрузка

В DAG-файле `dags\stress_dag_factory_configurable` вынесены DAG_COUNT, TASKS_PER_DAG, TASK_DURATION, POOL_NAME, MAX_ACTIVE_RUNS и тип workload в Airflow Variables и в env-переменные. Метод `get_config_value` пытается прочитать данные переменные сначала из Airflow Variables, затем из переменных среды; иначе использует значения по умолчанию.

Пробрасывание переменных в Airflow Variables:
```
docker compose exec airflow-scheduler airflow variables set STRESS_DAG_COUNT 20
docker compose exec airflow-scheduler airflow variables set STRESS_TASKS_PER_DAG 30
docker compose exec airflow-scheduler airflow variables set STRESS_TASK_DURATION 10
docker compose exec airflow-scheduler airflow variables set STRESS_POOL_NAME default_pool
docker compose exec airflow-scheduler airflow variables set STRESS_MAX_ACTIVE_RUNS 1
docker compose exec airflow-scheduler airflow variables set STRESS_WORKLOAD_TYPE parallel
```
В .env:
```
STRESS_DAG_COUNT = 20
STRESS_TASKS_PER_DAG = 30
STRESS_TASK_DURATION = 10
STRESS_POOL_NAME = default_pool
STRESS_MAX_ACTIVE_RUNS = 1
STRESS_WORKLOAD_TYPE = parallel
```
