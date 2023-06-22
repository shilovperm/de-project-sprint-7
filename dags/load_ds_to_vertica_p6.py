 
import contextlib
import hashlib
import json
from typing import Dict, List, Optional
 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
 
from airflow.decorators import dag
 
import pandas as pd
import pendulum
import vertica_python
 
 
def load_dataset_file_to_vertica(
    dataset_path: str,
    schema: str,
    table: str,
    columns: List[str],
    type_override: Optional[Dict[str, str]] = None,
):
    df = pd.read_csv(dataset_path, dtype=type_override)
    num_rows = len(df)
    print(f'num_rows: {num_rows}')
    
    conn_info = {'host': 'vertica.tgcloudenv.ru',
             'port': 5433,
             'user': 'stv230551',
             'password': 'JaNWhv7ldwUfgER',
             'database': 'dwh' }
    
    vertica_conn = vertica_python.connect(**conn_info)
    columns = ', '.join(columns)
    copy_expr = f"""
        COPY {schema}.{table} ({columns}) 
        FROM LOCAL '{dataset_path}'  
        DELIMITER ',' 
        ENCLOSED BY '"'
    """
    
    print(f"SQL: {copy_expr}")
    
    with contextlib.closing(vertica_conn.cursor()) as cur:
        cur.execute(copy_expr)    
     
    vertica_conn.close()
 
 
@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def project6_dag_load_data_to_staging():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
  
    load_group_log = PythonOperator(
        task_id='load_group_log',
        python_callable=load_dataset_file_to_vertica,
        op_kwargs={
            'dataset_path': '/data/group_log.csv',  # путь к скачанному файлу
            'schema': 'STV230551__STAGING',  # схема, куда загружаем данные
            'table': 'group_log',  # таблица, в которую будем загружать
            'columns': ['group_id', 'user_id', 'user_id_from', 'event', 'datetime'],  # колонки для загрузки
            'type_override': {'group_id': 'Int64', 'user_id': 'Int64', 'user_id_from': 'Int64'},  # преобразования типов при загрузке (опционально)
        },
    )
    

    
    
    start >> [load_group_log] >> end
 
 
load_to_stg = project6_dag_load_data_to_staging()