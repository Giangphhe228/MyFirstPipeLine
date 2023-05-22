# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago
from datetime import datetime
from time import time,ctime
import template
import sys
sys.path.insert(1, '/mnt/f/RnD/ETL/etlCrawlData/')
import ETLfunction

#defining DAG arguments
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'GP',
    'start_date': days_ago(0),
    'email': ['giangphhe7@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    dag_id='shopping-data-pipeline',
    start_date= days_ago(0),
    default_args=default_args,
    description='ETL pipeline shopping data',
    schedule="*/10 * * * *",
    # schedule_interval=timedelta(days=1),
)

# define the tasks
# define the first task named extract
# extract = PythonOperator(
#     task_id='extract',
#     start_time = time(),

#     dag=dag,
# )
# define the second task named transform
transform = PythonOperator(
    task_id='transform',
    python_callable= ETLfunction.deployTheTransformation,
    dag=dag,
)
# define the third task named load
load = PythonOperator(
    task_id='load',
    python_callable= ETLfunction.deployTheLoading,
    dag=dag,
)

email = EmailOperator(
    task_id='email',
    to='giang2282000@gmail.com',
    subject='ETL finish',
    html_content= template.htmlPage2,
    files=['/mnt/f/RnD/ETL/etlCrawlData/logfile.txt'],
    dag=dag,
)

# # https://api.telegram.org/bot5974649095:AAFDkZ2DZumnv5UP4jujKweyBGFrzsTJ6k4/getUpdates
# send_telegram_message = TelegramOperator(
#         task_id='send_telegram_message',
#         telegram_conn_id='telegram_default',
#         token='5974649095:AAFDkZ2DZumnv5UP4jujKweyBGFrzsTJ6k4',
#         chat_id='-970408165',
#         text='oi ban oi',
#         dag=dag,
#     )

# task pipeline
# extract >>
transform >> load >> email
# 
# load >> send_telegram_message
