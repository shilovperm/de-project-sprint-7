from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag

import logging
import pendulum
import boto3

log = logging.getLogger(__name__)

def fetch_s3_file(bucket: str, key: str):
    # сюда поместить код из скрипта для скачивания файла
    AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
    AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"
    
    log.info(bucket)
    log.info(key)
    log.info('/data/' + key)
    
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename='/data/' + key
    ) 

# эту команду надо будет поправить, чтобы она выводила
# первые десять строк каждого файла
bash_command_tmpl = """
echo {{ params.files }}
"""

@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))

def project6_dag_get_data():
    bucket_files = ['group_log.csv']
    
    task1 = PythonOperator(
        task_id=f'fetch_group_log.csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'group_log.csv'},
    )
    
    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
        #params={'files': [f'/data/{f}' for f in bucket_files]}
        params={'files': ' '.join([f'/data/{f}' for f in bucket_files])}
    )

    task1 >> print_10_lines_of_each

get_data = project6_dag_get_data()