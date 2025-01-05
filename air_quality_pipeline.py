import pandas as pd 
import requests 
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator

api = 'https://donnees.montreal.ca/api/3/action/datastore_search?resource_id=9c7434e9-f5af-4154-991c-293fbd5cb626'

def extraction(): 
    """
    
    """
    r = requests.get(api)
    if r.status_code == 200: 
        e = r.json()
        return e
    else: 
        raise Exception(f"Non-success status code: {r.status_code}")

def transformation(e): 
    """
    
    """
    #   obtaining the needed data (flattening)
    d1 = e['result']
    d2 = (d1['records'])

    #   lists to append the data from the api
    id = []
    section = []
    value = []
    date = []
    hour = []
    for i in d2:
        id.append(i['_id'])
        section.append(i['secteur'])
        value.append(i['valeur'])
        date.append(i['date'])
        hour.append(i['heure'])

    #   changing data types
    df = pd.DataFrame({
                     'id': id, 
                     'section': section,
                     'air_quality': value, 
                     'date': date, 
                     'time': hour
                     })
    df['id'] = df['id'].astype(int)
    df['section'] = df['section'].astype(str)
    df['air_quality'] = df['air_quality'].astype(int)
    df['date'] = pd.to_datetime(df['date'], format ='%Y-%m-%d')
    df['time'] = pd.to_datetime(df['time'], format = '%H').dt.strftime('%H:%M')
    
    #   adding composite_key for uniqueness
    df['composite_key'] = df['id'].astype(str) + '-' + df['date'].astype(str)
    return df

def load(df): 
    current_data = pd.read_csv('/Users/omarkhan/vdm air quality index.csv')
    fifteen_days_filter = date.today() - timedelta(days=15)
    # converting date to date type (reading csvs can convert to a string) 
    current_data['date'] = pd.to_datetime(current_data['date'], format = '%Y-%m-%d')
    current_data = current_data.loc[current_data["date"] > fifteen_days_filter ]
    if not df['composite_key'].isin(current_data['composite_key']).any(): 
        updated_load = pd.concat([current_data, df], ignore_index= True)
        updated_load.to_csv('vdm air quality index.csv', index = False) #   change file path to fit your own

#   DAG WORKFLOW
default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(year=2025,month=1,day=4),
}

etl_dag = DAG(
    'air_quality_pipeline',
    description='This is the orchestration for the air quality index from ville de montreal',
    default_args=default_args,
    schedule='@hourly',
    catchup=False,  # to ensure if failures occur, it will catchup on missed runs
)

extract_task = PythonOperator(
    task_id='extraction',
    python_callable=extraction,
    dag=etl_dag,
)

transformation_task = PythonOperator(
    task_id='transformation',
    python_callable=transformation, 
    dag=etl_dag,
) 

loading_task = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=etl_dag
)

extract_task >> transformation_task >> loading_task
