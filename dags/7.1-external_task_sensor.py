from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

with DAG(dag_id="external_task_sensor",
         description="Una dependencia",
         schedule_interval="@daily",
         start_date =datetime(2023,1,6),
         end_date=datetime(2023,1,30)
         ) as dag:
    
    t1 = BashOperator(task_id="t1",
                      bash_command="sleep 10 && echo 'Primer DAG finzalizado'")