from airflow.models import DAG

from datetime import datetime

from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
            '1.2_user_processing',
            start_date=datetime(2023,2,16),
            schedule_interval='@daily',
            catchup=False,
            owner='Tinmar',
            tags=['Curso 3', 'Introduction_to_Apache_Airflow']
        ) as dag:
    
    start = DummyOperator(task_id='start')

    create_table = PostgresOperator(
                                    task_id='create_table',
                                    postgres_conn_id = 'postgres',
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
    
    end = DummyOperator(task_id='end')

    start >> create_table >> end