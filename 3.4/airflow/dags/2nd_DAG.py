from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


# аргументы дага по умолчанию
default_args = {
    "owner": "BS",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}



with DAG(
    default_args = default_args,
    dag_id = 'BASH_for_rates',
    description = 'Сбор данных курса раз в 10 минут',
    start_date = datetime(2023,10,12),
    schedule_interval="*/10 * * * *",
    tags=["bash","BS"],
    catchup=False

) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    bash_miracle = BashOperator(
    task_id='bash_miracle',
    bash_command='python /opt/airflow/dags/app_now.py '
    )


    start >> bash_miracle >> end
