from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG(dag_id="bash_operator",
         description="Primer bash operator",
         start_date = datetime(2023,5,2),
         schedule_interval="@once") as dag:
    t1 = BashOperator(task_id="bash_task",
                      bash_command="echo hola mundo",
                      dag = dag)
    t1