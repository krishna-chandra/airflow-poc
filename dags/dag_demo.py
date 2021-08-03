try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


def function_A(**context):
    print("function_A executed.")
    context['ti'].xcom_push(key='key1', value="function_A value")


def function_B(**context):
    instance = context.get("ti").xcom_pull(key="key1")
    print("this value came from function_A".format(instance))
    
    data = [{"key":"key1","value":"val1"}, { "key":"key2","value":"value2"},]
    df = pd.DataFrame(data=data)
    print('@'*66)
    print(df.head())
    print('@'*66)

    


with DAG(
        dag_id="dag_demo",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 8, 3),
        },
        catchup=False) as f:

    function_A = PythonOperator(
        task_id="function_A",
        python_callable=function_A,
        provide_context=True,
    )

    function_B = PythonOperator(
        task_id="function_B",
        python_callable=function_B,
        provide_context=True,
    )

function_A >> function_B