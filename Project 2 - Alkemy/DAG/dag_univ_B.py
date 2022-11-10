"""DAG created for processing data from postgres using pandas and loading it to Amazon S3"""
import os
from airflow import DAG
import logging
from datetime import datetime
from dotenv import load_dotenv
from airflow.operators import python_operator
from pathlib import Path
from airflow.utils.task_group import TaskGroup
import airflow.example_dags.src.py_functions.queryDatabase_comahue as qc
import airflow.example_dags.src.py_functions.queryDatabase_salvador as qs
import airflow.example_dags.src.py_functions.pandas_comahue as pc
import airflow.example_dags.src.py_functions.pandas_salvador as ps
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

# Logger configuration
logging.basicConfig(
    level=logging.INFO,
    format='%Y-%m-%d - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler('univ_b.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Loading variables from dotenv
load_dotenv()

postgres_conn_id = os.getenv('postgres_conn_id')
s3_bucket = os.getenv('s3_bucket')
path_c = os.getenv('path_c')
path_s = os.getenv('path_s')
auth = os.getenv('auth')
destFolder = os.getenv('destFolder')

logging.info('DAG para univ del Salvador y Comahue')
with DAG(
        "dag_univ_salvador_comahue",
        description="DAG para univ del Salvador y Comahue",
        schedule_interval='@hourly',
        start_date=datetime(2022, 7, 17),
        catchup=False,
) as dag:
    logging.info('Querying database...')
    # Here we extract sql archives for further use
    with TaskGroup(group_id='queries') as querying_database:
        extract_comahue = python_operator.PythonOperator(
            task_id='query_comahue',
            python_callable=qc.queryComahue,
            retries=5
        )

        extract_salvador = python_operator.PythonOperator(
            task_id='query_salvador',
            python_callable=qs.querySalvador,
            retries=5
        )

        extract_comahue >> extract_salvador

    logging.info('Processing with pandas...')
    # Here we perform the queries
    with TaskGroup(group_id='pandas') as pandas_processing:
        pandas_processing_comahue = python_operator.PythonOperator(
            task_id='pandas_comahue',
            python_callable=pc.pandas_processing_comahue
        )

        pandas_processing_salvador = python_operator.PythonOperator(
            task_id='pandas_salvador',
            python_callable=ps.pandas_processing_salvador
        )

        pandas_processing_comahue >> pandas_processing_salvador

    logging.info('Storing results...')
    with TaskGroup(group_id='aws_bucket') as bucket_store:
        aws_bucket_comahue = LocalFilesystemToS3Operator(
            task_id="store_bucket_comahue",
            filename=Path(__file__).parent / "destFolder" / "universidad_del_comahue.txt",
            aws_conn_id="s3",
            dest_key='G255_universidad_del_comahue.txt',
            dest_bucket="cohorte-julio-8972766c",
            replace=True,
        )

        aws_bucket_salvador = LocalFilesystemToS3Operator(
            task_id="store_bucket_salvador",
            filename=Path(__file__).parent / "destFolder" / "universidad_del_salvador.txt",
            aws_conn_id="s3",
            dest_key='G255_universidad_del_salvador.txt',
            dest_bucket="cohorte-julio-8972766c",
            replace=True,
        )

        aws_bucket_comahue >> aws_bucket_salvador

    querying_database >> pandas_processing >> bucket_store
