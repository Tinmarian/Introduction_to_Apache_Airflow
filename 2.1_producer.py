from airflow import DAG, Dataset

from airflow.decorators import task

from datetime import datetime

my_file = Dataset("/tmp/my_file.txt")

with DAG(
            '2.1_producer',
            catchup=False,
            schedule='@daily',
            start_date=datetime(2023,2,15),
            tags=['Curso 3', 'Introduction_to_Apache_Airflow']
        ):

    
    @task(outlets=[my_file])
    def update_dataset():
        with open(my_file.uri, "w") as f:
            f.write("producer update")

    update_dataset()