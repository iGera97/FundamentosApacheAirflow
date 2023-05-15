from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def print_hello():
    print("My hello world")

with DAG(dag_id="python_operator",
         description="Primer DAG con python operator",
         start_date = datetime(2023,5,2),
         schedule_interval="@once") as dag:
    t1 = PythonOperator(task_id="python_task",
                        python_callable= print_hello,
                         dag = dag)
    t1