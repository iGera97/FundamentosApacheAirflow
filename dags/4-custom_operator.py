from airflow import DAG
from datetime import datetime
from hellooperator import HelloOperator

with DAG(dag_id="customoperator_test",
         description="Una dependencia",
         schedule_interval="@once",
         start_date =datetime(2022,12,6)) as dag:
    t1 = HelloOperator(task_id="hello",
                       name="algún nombre")