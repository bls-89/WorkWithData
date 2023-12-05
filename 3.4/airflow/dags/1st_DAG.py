from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import pendulum

# аргументы дага по умолчанию
default_args = {
    "owner": "BS",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
        "template_searchpath" : "/tmp"
}

with DAG(dag_id="first_dag", 
         default_args=default_args, 
         description="надпись в логе таски каждые 10 минут",
         start_date = datetime(2023,10,12),
         schedule_interval="*/10 * * * *",
         tags=["bash","BS"], 
         catchup=False) as dag:

    start = EmptyOperator(task_id='start') 


    bash = BashOperator(
        task_id='bash_message',
        bash_command=f"echo 'Good morning my diggers!'"
    )

    end = EmptyOperator(task_id='end')

    start >> bash >> end
