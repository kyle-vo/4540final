from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import requests
import os
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG(
    dag_id='Pipeline',
    default_args=default_args,
    description='Pipeline to read, clean, analyze, and visualize data from poe.ninja',
    schedule_interval=None
)

urls = {
    'affliction_currency': 'https://drive.google.com/uc?id=13GChRVVkwOZTyv4Ad3wESBH3DSCe_IBA',
    'ancestor_currency': 'https://drive.google.com/uc?id=1NfyV5jFdz-3vmlzHuZGzMh6k8JMR4vSk',
    #'crucible_currency': 'https://drive.google.com/uc?id=1gfFbLsBSTr4kdfZRBqjcYCHiRiz4Rz-R',
    #'sanctum_currency': 'https://drive.google.com/uc?id=1drxNaY3Jcsd7loikCjNrENBtjjsoGjyD'
}

def data_acquisition(urls):
    data_paths = {}
    directory = '/storage/acquire/'
    os.makedirs(directory, exist_ok=True) 
    for table, url in urls.items():
        response = requests.get(url)
        if response.status_code == 200:
            csv_path = os.path.join(directory, f'{table}.csv')
            with open(csv_path, 'w') as csv_file:
                csv_file.write(response.text)
            data_paths[f"{table}"] = csv_path
        else:
            print(f"Failed to download data from {url}")

    return {"status": "success", "data_paths": data_paths}

def data_cleaning(**kwargs):
    ti = kwargs['ti']
    data_paths = ti.xcom_pull(task_ids='AcquireData', key='return_value')['data_paths']
    cleaned_data_paths = {}
    directory = '/storage/clean/'
    os.makedirs(directory, exist_ok=True)
    for table, path in data_paths.items():
        cleaned_data_path = os.path.join(directory, f'{table}_cleaned.csv')
        cleaned_lines = []
        # Cleaning
        with open(path, 'r') as file:
            lines = file.readlines()
            for line in lines:
                fields = line.strip().split(';')[:-1]
                cleaned_fields = []
                for field in fields:
                    cleaned_field = field.strip() if field.strip() else "Missing"
                    cleaned_fields.append(cleaned_field)
                cleaned_line = ','.join(cleaned_fields)
                cleaned_lines.append(cleaned_line)

        with open(cleaned_data_path, 'w') as cleaned_file:
            cleaned_file.write('\n'.join(cleaned_lines))
        cleaned_data_paths[table] = cleaned_data_path

    return {"status": "success", "cleaned_data_paths": cleaned_data_paths}

def data_analysis(**kwargs):
    ti = kwargs['ti']
    data_paths = ti.xcom_pull(task_ids='CleanData', key='return_value')['cleaned_data_paths']
    analysis_results = {}
    
    for table, path in data_paths.items():
        df = pd.read_csv(path)
        
        # Quantitative analysis
        centers = {
            'mean': df['Value'].mean(),
            'median': df['Value'].median(),
            'mode': df['Value'].mode()[0]
        }
        spread = {
            'range': df['Value'].max() - df['Value'].min(),
            'std_dev': df['Value'].std(),
            'variance': df['Value'].var()
        }
        
        # Qualitative analysis
        qualitative = {
            'league_count': df['League'].nunique(),
            'earliest_date': str(df['Date'].min()), 
            'latest_date': str(df['Date'].max()) 
        }
        
        # Get the mean for each unique Get/Pay
        averages = df.groupby(['Get', 'Pay'])['Value'].mean().reset_index()
        average_dict = {f"{row['Get']}, {row['Pay']}": row['Value'] for index, row in averages.iterrows()}
        
        analysis_results[table] = {
            'centers': centers,
            'spread': spread,
            'qualitative_analysis': qualitative,
            'averages': average_dict
        }
        
        # Store DataFrame as JSON
        output_file = f'/storage/{table}_analysis.json'
        with open(output_file, 'w') as f:
            json.dump(analysis_results[table], f)
        
    return {"status": "success", "analysis_results": analysis_results}

def data_visualize(**kwargs):
    ti = kwargs['ti']
    analysis_results = ti.xcom_pull(task_ids='AnalyzeData', key='return_value')['analysis_results']
    
    for table, result in analysis_results.items():
        file_path = ti.xcom_pull(task_ids='AnalyzeData', key=f'{table}_analysis')
        if not file_path:
            continue
        ti.xcom_push(key=f'{table}_analysis', value=file_path)

    return
    
AcquireData = PythonOperator(
    dag=dag,
    task_id='AcquireData',
    python_callable=data_acquisition,
    op_kwargs={'urls': urls}
)

CleanData = PythonOperator(
    dag=dag,
    task_id='CleanData',
    python_callable=data_cleaning
)

AnalyzeData = PythonOperator(
    dag=dag,
    task_id='AnalyzeData',
    python_callable=data_analysis
)

VisualizeData = PythonOperator(
    dag=dag,
    task_id='VisualizeData',
    python_callable=data_visualize
)

AcquireData >> CleanData >> AnalyzeData >> VisualizeData