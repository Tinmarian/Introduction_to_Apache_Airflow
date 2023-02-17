from airflow import DAG
from airflow.operators.bash import BashOperator
from Introduction_to_Apache_Airflow.groups.group_downloads import group_downloads
from Introduction_to_Apache_Airflow.groups.group_transforms import group_transforms 
from datetime import datetime
 
with DAG(
        '5_task_group',
        start_date=datetime(2022, 1, 1), 
        schedule_interval='@daily',
        catchup=False,
        tags=['Curso 3', 'Introduction_to_Apache_Airflow']
        ) as dag:
 
    args = {'start_date':dag.start_date, 'schedule_interval':dag.schedule_interval, 'catchup':dag.catchup}

    downloads = group_downloads()
 
    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )
 
    transforms = group_transforms()
    
 
    downloads >> check_files >> transforms