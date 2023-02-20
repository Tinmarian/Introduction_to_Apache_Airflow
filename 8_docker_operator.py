from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator

from datetime import datetime

@dag(
    '8_docker_operator',
    start_date=datetime(2023,2,16),
    catchup=False,
    schedule_interval=None,
    tags=['Curso 3', 'Introduction_to_Apache_Airflow']
    )
def docker_dag():

    @task()
    def t1():
        print('Hola')

    t2 = DockerOperator(
                        task_id='t2',
                        image='python:3.10.6-slim-buster',
                        api_version='auto',
                        container_name='task_t2',
                        command='echo "command running in the docker container"',
                        docker_url='unix://var/run/docker.sock',
                        network_mode='bridge',
                        xcom_all=True,
                        retrieve_output=True,
                        auto_remove=True
                        # cpus=
                        # mem_limit='1g', '512m'
                    )
    
    t1() >> t2
    
dag = docker_dag()