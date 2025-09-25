from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

N = 20
TOTAL_TASKS = 200
MAPPED_BATCH_SIZE = 5
BATCHES = TOTAL_TASKS // MAPPED_BATCH_SIZE  # 40

def create_performance_dag(dag_id: str, seq: int = 0) -> DAG:
    dag_args = {
        "dag_id": dag_id,
        "catchup": False,
        "max_active_tasks": 5,
        "max_active_runs": 1,
        "start_date": datetime(2023, 1, 1),
    }

    dag = DAG(**dag_args)

    for i in range(1, BATCHES):
        BashOperator.partial(
            task_id=f'task_{i}',
            pool=(f'limited_pool_{(i + seq)%N}'),
            pool_slots=1,
            max_active_tis_per_dagrun=5,
            dag=dag
        ).expand(bash_command=["echo 0" for _ in range(MAPPED_BATCH_SIZE)])

    return dag


from airflow.operators.trigger_dagrun import TriggerDagRunOperator

for i in range(N):
    dag_id = f"mapped_pool_performance_dag_{i}"
    globals()[dag_id] = create_performance_dag(dag_id, i)


with DAG(
    dag_id="trigger_mapped_pool_performance_dags",
    schedule=None,
    catchup=False,
    tags=["trigger"],
) as dag:

    for i in range(N):
        TriggerDagRunOperator(
            task_id=f"trigger_pool_performance_dag_{i}",
            trigger_dag_id=f"pool_performance_dag_{i}",
            poke_interval=10,
        )
