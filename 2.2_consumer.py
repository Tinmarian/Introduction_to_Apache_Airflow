from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

my_file = Dataset("/tmp/my_file.txt")

with DAG(
            '2.2_consumer',
            catchup=False,
            schedule=[my_file],
            start_date=datetime(2023,2,15),
            tags=['Curso 3', 'Introduction_to_Apache_Airflow'] 
        ):

    @task
    def read_dataset():
        with open(my_file.uri, "r") as f:
            print(f.read())

    read_dataset()