from airflow.models import DAG

from datetime import datetime

from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import json
from pandas import json_normalize

from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args={
    "owner":"Tinmar"
}

def _process_user(ti):
    user = ti.xcom_pull(task_ids='extract_data')
    user = user['results'][0]
    processed_user = json_normalize(
        {
            'firstname':user['name']['first'],
            'lastname':user['name']['last'],
            'country':user['location']['country'],
            'username':user['login']['username'],
            'password':user['login']['password'],
            'email':user['email']
        }
    )
    processed_user.to_csv('/c/Users/tinma/OneDrive/Escritorio/processed_user.csv', index=None, header=False)

def _store_user():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER as ','",
        filename='/c/Users/tinma/OneDrive/Escritorio/processed_user.csv'
    )


with DAG(
            '1.6_user_processing',
            start_date=datetime(2023,2,16),
            schedule_interval='@daily',
            catchup=False,
            default_args=default_args,
            tags=['Curso 3', 'Introduction_to_Apache_Airflow']
        ) as dag:
    
    start = DummyOperator(task_id='start')

    create_table = PostgresOperator(
                                    task_id='create_table',
                                    postgres_conn_id='postgres',
                                    sql="""
                                        CREATE TABLE IF NOT EXISTS users (
                                            firstname TEXT NOT NULL,
                                            lastname TEXT NOT NULL,
                                            country TEXT NOT NULL,
                                            username TEXT NOT NULL,
                                            password TEXT NOT NULL,
                                            email TEXT NOT NULL
                                        );
                                    """
                                )

    insert_data = PostgresOperator(
                                    task_id='insert_data',
                                    postgres_conn_id='postgres',
                                    sql= """
                                        INSERT INTO users VALUES ('Tinmar', 'Andrade','México','tinmar','GuruSat.3','tinmar96@gmail.com')
                                    """  
                                )

    select_data = PostgresOperator(
                                    task_id='select_data',
                                    postgres_conn_id='postgres',
                                    sql="""
                                        SELECT * FROM users
                                    """   
                                )
    
    is_api_available = HttpSensor(
                                    task_id='is_api_available',
                                    http_conn_id='user_api',
                                    endpoint='api/'
                                )

    extract_data = SimpleHttpOperator(
                                    task_id='extract_data',
                                    http_conn_id='user_api',
                                    endpoint='api/',
                                    method='GET',
                                    response_filter=lambda response: json.loads(response.text),
                                    log_response=True   
                                )

    process_user = PythonOperator(
                                    task_id='process_user',
                                    python_callable=_process_user   
                                )

    store_user = PythonOperator(
                                task_id='store_user',
                                python_callable=_store_user   
                            )

    end = DummyOperator(task_id='end')

    start >> create_table >> insert_data >> is_api_available >> extract_data >> process_user >> store_user >> select_data >> end