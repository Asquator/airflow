from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

N=4000

with DAG(
    dag_id="simple_bash_mapped_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    max_active_tasks=N,
    catchup=False,
) as dag:
    BashOperator.partial(
        task_id="echo_task",
        do_xcom_push=False
    ).expand(
        bash_command=['echo 0'] * N
    )
