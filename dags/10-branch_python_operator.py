from airflow import DAG
from datetime import datetime
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime,date
def filtrar_verdaderos(**context):
   print(context)
   #regreso el nombre del task
   return 'finish_6' if context["logical_date"].date() < date(2023,4,7) else 'finish_7'

with DAG(dag_id="10-branch_python_operator",
         description="10-branch_python_operator",
         schedule_interval="@daily",
         start_date =datetime(2023,4,5),
         max_active_runs=1
         ) as dag:
   branching  = BranchPythonOperator(task_id="branch",
                                      python_callable=filtrar_verdaderos)
   finish_6 = BashOperator(task_id="finish_6",
                            bash_command="echo 'Running {{ds}}'")
   
   finish_7 = BashOperator(task_id="finish_7",
                            bash_command="echo 'Running {{ds}}'")

branching >> [finish_6,finish_7]