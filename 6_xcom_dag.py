from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
 
from datetime import datetime
 
def _t1(ti):
    ti.xcom_push(key='my_key', value=42)
 
def _t2(ti):
    print(ti.xcom_pull(key='my_key'))
    
 
with DAG(
        "6_xcom_dag",
        start_date=datetime(2022, 1, 1), 
        schedule_interval='@daily',
        catchup=False,
        tags=['Curso 3', 'Introduction_to_Apache_Airflow']
        ) as dag:
 
    t1 = PythonOperator(
        task_id='t1',
        python_callable=_t1
    )
 
    t2 = PythonOperator(
        task_id='t2',
        python_callable=_t2
    )
 
    t3 = BashOperator(
        task_id='t3',
        bash_command="echo ''"
    )
 
    t1 >> t2 >> t3