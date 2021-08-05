import boto3
from time import sleep
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta


# simple download task
def download_file():
    """ s3_client = boto3.client('s3')
    s3_client.download_file('airflow-xcom', 'airflow.cfg', 'downloaded_airflow_config.cfg')
    print('@'*100)
    print(open('downloaded_airflow_config.cfg').read())
    print('@'*100) """
    sleep(15)


def start():
   """  print("Downloading started ....") """
   sleep(2)

# default arguments for each task
default_args = {
    'owner': 'krsoni',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


with DAG(
        dag_id="s3_file_processor",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 8, 3),
        },
        catchup=False) as f:


    download_from_s3 = PythonOperator(
        task_id='download_from_s3',
        python_callable=download_file,
        )

    download_from_s3_1 = PythonOperator(
        task_id='download_from_s3_1',
        python_callable=download_file,
        )

    download_from_s3_2 = PythonOperator(
        task_id='download_from_s3_2',
        python_callable=download_file,
        )   

    start = PythonOperator(
        task_id="start",
        python_callable=start,
    )



start >> download_from_s3
start >> download_from_s3_1
start >> download_from_s3_2