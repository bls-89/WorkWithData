from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.postgres_hook import PostgresHook
def ts_to_date(x):
    return datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S')
import json
import requests

#***** Базовые константы ************

# соединение с базой person
conn_id = Variable.get("conn_name")

#получем данные из переменных
access_key= Variable.get("access_key")
source= Variable.get("source")
currencies= Variable.get("currencies")
URL_API= Variable.get("URL_API")
format= Variable.get("format")

def load_data_psql(**context):
        # соединяемся с БД
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # создадим сначала  содержимое таблицу
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS rates (
              id serial primary key,
              timestamp timestamp,
              source varchar(255),
              currencies varchar(255),
              rate double precision);""")
        response = requests.get(URL_API, params={'access_key': access_key,
                                         'source': source,
                                         'currencies': currencies,
                                         'format': format})
        data = response.json()

        insert_query = """
        INSERT INTO rates(timestamp, source, currencies, rate) VALUES
        (%s, %s, %s, %s)
        """
        cursor.execute(insert_query, (ts_to_date(data['timestamp']), data['source'], 'RUB', data['quotes']['BTCRUB']))

        conn.commit()

        cursor.close()
        conn.close()
        print("Данные успешно загружены в таблицу!")
        print("СТАТУС: ",context["task_instance"].current_state())
    except Exception as error:
        conn.rollback()
        raise Exception(f'Загрузить данные не получилось: {error}!')



# аргументы дага по умолчанию
default_args = {
    "owner": "BS",
    "retries": 5,
    "retry_delay": 5,
    "start_date": datetime(2023, 10, 11),
}

with DAG(dag_id="parsing_and_create",
         default_args=default_args,
         schedule_interval="@once",
         description= "Парсинг и создание таблицы",
         template_searchpath = "/tmp",
         catchup=False) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # загружаем в postgresql сырые данные
    load_datata_to_postgres = PythonOperator(
        task_id='load_data_to_psql',
        python_callable=load_data_psql
    )

    start >> load_datata_to_postgres >> end

