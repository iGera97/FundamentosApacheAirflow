from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
def print_hello():
    print("My hello world")

with DAG(dag_id="dependencies",
         description="Creando dependencias entre tareas",
         start_date = datetime(2023,5,2),
         catchup=False,
         schedule_interval="* * * * *") as dag:
    t1 = PythonOperator(task_id="tarea1",
                        python_callable= print_hello,
                        dag=dag)
    t2 = BashOperator(task_id = "tarea2",
                      bash_command="sleep 10 && echo tarea2",
                      dag=dag)
    t3 = BashOperator(task_id = "tarea3",
                      bash_command="sleep 20 && echo tarea3",
                      dag=dag)
    t4 = BashOperator(task_id = "tarea4",
                      bash_command="sleep 30 && echo tarea4",
                      dag=dag)
    t5 = BashOperator(task_id = "tarea5",
                      bash_command="echo tarea5",
                      dag=dag)
    
    t1.set_downstream([t2,t3,t4])
    t5.set_upstream([t2,t3,t4])