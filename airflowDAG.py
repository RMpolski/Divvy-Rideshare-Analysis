from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from datetime import timedelta
import os
from helperFuncs import divvy_download_func, save_progress_delete_file

##### Some sort of workaround for an oddity of mac/airflow having a requests/urllib bug
from _scproxy import _get_proxy_settings
_get_proxy_settings()
#######

DAG_ID = 'divvy_to_postgres'
CONN_ID = 'postgres_default'  # used the UI to set up the connection to the database

file_url = 'https://divvy-tripdata.s3.amazonaws.com/index.html'  # page where the data can be downloaded
uploaded_filename = 'uploaded_links.txt'  # file listing links to data already uploaded

with DAG(dag_id=DAG_ID,
         start_date=datetime(2021, 1, 5, 18),
         end_date=datetime(2023, 6, 30, 23),
         schedule='0 18 5 * *',  # cron schedule denoting 6pm every fifth day of the month
         catchup=True,
         max_active_runs=1) as dag:

    divvy_download = PythonOperator(task_id='divvy_download',
                                    python_callable=divvy_download_func,
                                    op_kwargs={'uploaded_filename': uploaded_filename,
                                               'file_url': file_url},
                                    retry_delay=timedelta(days=1), retries=20)
        
    upload_postgres = SQLExecuteQueryOperator(task_id='upload_postgres',
                                              conn_id=CONN_ID,
                                              sql='sql/upload_divvy_rides.sql')
    
    save_delete_file = PythonOperator(task_id='save_delete_file',
                                      python_callable = save_progress_delete_file,
                                      op_kwargs={'uploaded_filename': uploaded_filename})
    
    
    """ These two commands were in an effort to make catchup=True work. I have to manually
    trigger a success for the first PREVIOUS using the UI, and the DAG parameters have to 
    have max_active_runs=1. Then these keep only one DAG running at a time sequentially."""
#     PREVIOUS = ExternalTaskSensor(task_id='Previous_Run',
#                                 external_dag_id=DAG_ID,
#                                 external_task_id='All_Tasks_Completed',
#                                 allowed_states=['success'],
#                                 execution_delta=timedelta(minutes=1))
    
#     COMPLETE = EmptyOperator(task_id='All_Tasks_Completed')
    
    # uncomment and place PREVIOUS and COMPLETE in the line if doing catchup.
    # Also, at least with my setup, it required manually changing PREVIOUS to 'success'
    #PREVIOUS >> 
    divvy_download >> upload_postgres >> save_delete_file
    # >> COMPLETE