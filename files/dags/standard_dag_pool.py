from airflow import DAG
from airflow.operators.bash import BashOperator


N = 20

def create_performance_dag(
    dag_id: str,
    seq: int = 0,
    
) -> DAG:
    dag_args = {
        "dag_id": dag_id,
        "catchup": False,
        "max_active_tasks": 5,
        "max_active_runs": 1
    }

    dag = DAG(**dag_args)

    for i in range(1, 200):
        BashOperator(
            task_id=f'task_{i}',
            pool=(f'limited_pool_{(i + seq)%N}'),
            bash_command=f'echo 0',
            pool_slots=1,
            dag=dag,
        )

    return dag


from airflow.operators.trigger_dagrun import TriggerDagRunOperator

for i in range(N):
    dag_id = f"pool_performance_dag_{i}"
    globals()[dag_id] = create_performance_dag(dag_id, i)


with DAG(
    dag_id="trigger_pool_performance_dags",
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
