from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from Introduction_to_Apache_Airflow.subdags.subdag_downloads import subdag_downloads
from Introduction_to_Apache_Airflow.subdags.subdag_transforms import subdag_transforms

from datetime import datetime
 
with DAG(
        '4.3_group_dag',
        start_date=datetime(2022, 1, 1), 
        schedule_interval='@daily',
        catchup=False,
        tags=['Curso 3', 'Introduction_to_Apache_Airflow']
        ) as dag:
 
    args = {'start_date':dag.start_date, 'schedule_interval':dag.schedule_interval, 'catchup':dag.catchup}

    downloads = SubDagOperator(
                                task_id='downloads',
                                subdag=subdag_downloads(dag.dag_id, 'downloads', args)                 
                            )
 
    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )
 
    transforms = SubDagOperator(
                                    task_id='transforms',
                                    subdag=subdag_transforms(dag.dag_id,'transforms',args) 
                                )
    
 
    downloads >> check_files >> transforms