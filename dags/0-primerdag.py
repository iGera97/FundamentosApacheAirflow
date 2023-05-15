from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag

#Context manager
with DAG(
    dag_id = "primer_dag",
    description = "Nuestro primer dag",
    start_date = datetime(2023,5,2),
    schedule_interval="@once") as dag_1:
    t1 = EmptyOperator(task_id="dummy",dag=dag_1)
    t1

#Standard constructor
dag_2 = DAG(dag_id = "dag_std",
    description = "Nuestro primer dag std",
    start_date = datetime(2023,5,2),
    schedule_interval="@once")
t2 = EmptyOperator(task_id="std_dummy",dag=dag_2)

#dag_decorator
@dag(dag_id = "dag_deco",
    description = "Nuestro primer dag decorator",
    start_date = datetime(2023,5,2),
    schedule_interval="@once")
def dag_deg():
    t3 = EmptyOperator(task_id="dummy_deco")

dag = dag_deg()