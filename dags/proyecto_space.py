from datetime import timedelta
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

def _generate_platzi_data(**kwargs):
    import pandas as pd
    data = pd.DataFrame({"student": ["Maria Cruz", "Daniel Crema",
                                    "Elon Musk", "Karol Castrejon",
                                    "Freddy Vega"],
                        "timestamp": [kwargs['logical_date'],
                                    kwargs['logical_date'],
                                    kwargs['logical_date'],
                                    kwargs['logical_date'],
                                    kwargs['logical_date']]})
    data.to_csv(f"/tmp/platzi_data_{kwargs['ds_nodash']}.csv",header=True)
    return f"/tmp/platzi_data_{kwargs['ds_nodash']}.csv"

default_args = { 'owner': 'airflow',
                'depends_on_past': False,
                'email': ['airflow@example.com'],
                'retries': 1,
                'retry_delay': timedelta(minutes=5)
                }

dag = DAG(dag_id='nasa_sensor',
            default_args=default_args,
            description='proyecto de platzi',
            schedule_interval='*/5 * * * *',
            catchup=False,
            max_active_runs=1,
            start_date=datetime(2023,5,9)
        )

t1 = BashOperator(task_id="generate-info",
                bash_command='sleep 10 && echo "OK" > /tmp/response_{{ds_nodash}}.txt && echo /tmp/response_{{ds_nodash}}.txt',
                dag=dag)

t2 = FileSensor(task_id='wait_nasa_file',
                filepath='/tmp/response_{{ds_nodash}}.txt')

t3 = BashOperator(task_id="get-spacex-info",
                  bash_command="curl -o /tmp/history.json -L 'https://api.spacexdata.com/v4/history' && echo /tmp/history.json")

t4 = PythonOperator(task_id="platzi-info",
                    python_callable=_generate_platzi_data
                    )

email_task_marketing = EmailOperator(
    task_id="email_task_marketing",
    to=['correo@gmail.com'],
    subject="Correo marketing {{ds_nodash}}",
    html_content="<i>El archivo {{ ti.xcom_pull(task_ids='generate-info') }} ya se encuentra cargado</i>"
)

email_task_data = EmailOperator(
    task_id="email_task_data",
    to=['correo@gmail.com'],
    subject="Correo data {{ds_nodash}}",
    html_content="<i>El archivo {{ ti.xcom_pull(task_ids='get-spacex-info') }} y {{ ti.xcom_pull(task_ids='platzi-info') }} ya se encuentran cargados</i>"
)


t1 >> t2 >> t3 >> t4 >> [email_task_marketing,email_task_data]