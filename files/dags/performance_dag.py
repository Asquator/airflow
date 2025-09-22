from dataclasses import dataclass
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from itertools import product

N = 20

def create_performance_dag(
    dag_id: str,
) -> DAG:
    dag_args = {
        "dag_id": dag_id,
        "catchup": False,
        "max_active_tasks": 8,
        "max_active_runs": 1
    }

    dag = DAG(**dag_args)

    for i in range(1, 200):
        # Determine priority: every 10th task gets higher priority
        # if i % 5 == 0:
        #     priority = 100
        # else:
        #     priority = 1

        BashOperator(
            task_id=f'task_{i}', # priority_weight=priority,
            bash_command=f'echo 0',
            pool_slots=1,
            dag=dag,
        )

    return dag


from airflow.operators.trigger_dagrun import TriggerDagRunOperator

for i in range(N):
    dag_id = f"performance_dag_{i}"
    globals()[dag_id] = create_performance_dag(dag_id)


with DAG(
    dag_id="trigger_performance_dags",
    schedule=None,
    catchup=False,
    tags=["trigger"],
) as dag:

    for i in range(N):
        TriggerDagRunOperator(
            task_id=f"trigger_performance_dag_{i}",
            trigger_dag_id=f"performance_dag_{i}",
            poke_interval=10,
        )
