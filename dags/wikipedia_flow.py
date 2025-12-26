import os
import sys
from datetime import timedelta
from airflow import DAG

from datetime import datetime
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.wikipedia_pipeline import extract_wikipedia_data, transform_wikipedia_data, write_wikipedia_data


dag = DAG(
    dag_id='wikipedia_flow',
    default_args={
        'owner': 'ivory chuks',
        'start_date': datetime(year=2025, month=12, day=25),
    },
    schedule_interval=None,
    catchup=False,
)   

# Extracting data from wikipedia
extract_data_from_wikipedia = PythonOperator(
    task_id="extract_data_from_wikipedia",
    python_callable=extract_wikipedia_data,
    provide_context=True,
    op_kwargs={"url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"},
    retries=3,
    retry_delay=timedelta(minutes=5),
    dag=dag
)

# Preprocessing data
transform_wikipedia_data = PythonOperator(
    task_id='transform_wikipedia_data',
    provide_context=True,
    python_callable=transform_wikipedia_data,
    dag=dag
)


# Write to database
write_wikipedia_data = PythonOperator(
    task_id='write_wikipedia_data',
    provide_context=True,
    python_callable=write_wikipedia_data,
    dag=dag
)

extract_data_from_wikipedia >> transform_wikipedia_data >> write_wikipedia_data