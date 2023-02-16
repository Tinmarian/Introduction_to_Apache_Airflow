from airflow.models import DAG

from datetime import datetime

with DAG(
            'user_processing',
            start_date=datetime(2023,2,16),
            schedule_interval='@daily',
            catchup=False,
            tags=['Curso 3', 'Introduction_to_Apache_Airflow']
        ) as dag:
    None