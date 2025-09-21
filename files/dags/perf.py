from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from time import sleep


def sleep_for_a_while(seconds: int = 1) -> None:
    sleep(seconds)


with DAG(
    dag_id="sample_dag",
    max_active_tasks=3,
    max_active_runs=1,
    catchup=False,
) as mapped_dag:
    for i in range(15):
        PythonOperator(
            task_id=f"sleep_task_{i}",
            python_callable=sleep_for_a_while,
        )

with DAG(
    dag_id="sample_mapped_dag",
    max_active_tasks=60,
    max_active_runs=10,
    catchup=False,
) as mapped_dag:

    sleep_mapped_limit = PythonOperator.partial(
        task_id="sleep_mapped",
        python_callable=sleep_for_a_while,
        max_active_tis_per_dagrun=3,
    ).expand(op_kwargs=[{'seconds': 10} for _ in range(10)])

    sleep_across_dagruns_limit = PythonOperator(
        task_id="sleep_across",
        python_callable=lambda: sleep(15),
        max_active_tis_per_dag=3,
    )
