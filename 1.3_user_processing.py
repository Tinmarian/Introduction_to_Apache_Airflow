from airflow.models import DAG

from datetime import datetime

from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor


default_args={
    "owner":"Tinmar"
}

with DAG(
            '1.3_user_processing',
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

    end = DummyOperator(task_id='end')

    start >> create_table >> insert_data >> select_data >> is_api_available >> end