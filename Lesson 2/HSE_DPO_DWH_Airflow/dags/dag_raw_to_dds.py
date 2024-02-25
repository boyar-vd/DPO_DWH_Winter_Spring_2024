"""Import libs"""
from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd

"""Import args for DAG"""
from python_scripts.config import dag_args

"""Import connection to db"""
from python_scripts.config import engine


def csv_load():
    """Create Pandas DataFrame with data from csv"""
    df = pd.read_csv('/opt/airflow/dags/data/fraud.csv')[:10000]

    """Load data to dwh stg layer"""
    df.to_sql(con=engine, name='csv_fraud_data', schema='stg', if_exists='replace', index=False)


def stg_to_ods():
    engine.execute('''SELECT ods.load_fraud_full();''')


def load_transaction_full():
    pass


def fill_merchants_dict():
    pass


with DAG('load_data_from_csv', # dag_name
         description='dag_tag',
         schedule_interval='* * * * *',  # every minute
         catchup=False,
         default_args=dag_args
         ) as dag:

    task_csv_to_stg = PythonOperator(task_id='csv_to_stg',
                                     python_callable=csv_load)

    task_stg_to_ods = PythonOperator(task_id='stg_to_ods',
                                     python_callable=stg_to_ods)

    task_load_transaction_full = PythonOperator(task_id='load_transaction_full',
                                                python_callable=load_transaction_full)

    task_load_merchants_dict = PythonOperator(task_id='fill_merchants_dict',
                                              python_callable=fill_merchants_dict)

    '''DAG pipeline'''
    task_csv_to_stg >> task_stg_to_ods >> [task_load_transaction_full, task_load_merchants_dict]
