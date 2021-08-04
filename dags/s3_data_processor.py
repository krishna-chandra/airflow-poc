from airflow import DAG
from airflow.decorators import task, dag

from datetime import datetime
import pandas as pd

@task
def extract():

    people = {
        'Firstnames': ['James','Corolla','Mark','Eddy'],
        'Lastnames': ['Wick','Leto','Smith','Etwan']
    }

    df = pd.DataFrame(people, columns = ['Firstnames', 'Lastnames'])
    return df

@task
def process(value):
    print(f"Processing: {value}")

@dag(schedule_interval='@daily', start_date=datetime(2021, 8, 24), catchup=False)
def s3_data_processor():
    print("Task started")
    value = extract()
    print("Value extracted")
    process(value)
    print("value processed")

dag = s3_data_processor()