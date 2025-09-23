from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="simple_bash_mapped_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    BashOperator.partial(
        task_id="echo_task",
        do_xcom_push=False
    ).expand(
        bash_command=['echo 0'] * 4000
    )
