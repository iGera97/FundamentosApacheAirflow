from airflow import DAG
from datetime import datetime
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator

with DAG(dag_id="7.3-file_sensor",
         description="Filesensor",
         schedule_interval="@daily",
         start_date =datetime(2023,1,6),
         max_active_runs=1,
         catchup=False
         ) as dag:
    
    t1 = BashOperator(task_id="create_file",
                      bash_command="sleep 10 && echo 'mi archivo de hola mundo' >> /tmp/archivo.txt")
    
    t2 = FileSensor(task_id='tarea_sensor',
                    filepath='/tmp/archivo.txt',
                    )
    t3 = BashOperator(task_id="final",
                      bash_command="echo 'lo lograste'",
                         depends_on_past = True
                      )
    
t1 >> t2 >> t3