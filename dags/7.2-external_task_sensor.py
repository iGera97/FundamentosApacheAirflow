from airflow import DAG
from datetime import datetime
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(dag_id="7.2-external_task_sensor",
         description="Una dependencia",
         schedule_interval="@daily",
         start_date =datetime(2023,1,6),
         end_date=datetime(2023,1,30)
         ) as dag:
    
    t1 = ExternalTaskSensor(task_id='tarea_sensor',
                            external_dag_id='external_task_sensor',
                            external_task_id='t1',
                            poke_interval =10)