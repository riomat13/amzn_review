#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from datetime import datetime, timedelta
from functools import partial

from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.custom_common_plugin import (
    FileCheckOperator,
    MoveDataLocalOperator
)
from airflow.operators.etl_plugin import (
    CleanUpTablesOperator,
    DataCheckOperator,
    JSONToPostgresOperator,
    RemoveCacheOperator,
    SetupOperator,
    StagingDataCheckOperator,
    TableStateCheckOperator
)
from airflow.operators.tf_plugin import (
    RemoveTFRecordOperator,
    ReviewToTFRecordOperator,
)

from settings import Config
from workflow.helper import (
    cache_filepaths,
    cache_nofile_found,
    fetch_filepaths_from_cache,
    load_metadata_files,
    load_review_files
)
from workflow.dags import run_model


postgres_conn_id = os.environ.get('AIRFLOW_POSTGRES_CONN_ID')

default_args = {
    'owner': 'user',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'start_date': datetime.today(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': True,
}

# run every 2 hours
dag = DAG(
    'amzn_review_etl_local',
    default_args=default_args,
    description='ETL pipelining on local machine',
    schedule_interval='0 */2 * * *',
    catchup=False
)


# ================================================
#      Operators
# ================================================
DummyOperator.ui_color = '#ededed'
PostgresOperator.ui_color = '#99CCFF'
PythonOperator.ui_color = '#FFCC99'

# --------------------------------------
#   Start/End pipeline
# --------------------------------------
start_operator = SetupOperator(
    task_id='start_pipeline',
    mode='local',
    dag=dag
)

end_operator = DummyOperator(
    task_id='end_pipeline',
    dag=dag
)

# --------------------------------------
#   Tables setup
# --------------------------------------
# create tables if not exist
staging_tables_creation = PostgresOperator(
    task_id='staging_tables_creation',
    sql='sql/table_creation_local/staging_tables_creation.sql',
    postgres_conn_id=postgres_conn_id,
    dag=dag
)

dimension_tables_creation = PostgresOperator(
    task_id='dimension_tables_creation',
    sql='sql/table_creation_local/dimension_tables_creation.sql',
    postgres_conn_id=postgres_conn_id,
    dag=dag
)

fact_table_creation = PostgresOperator(
    task_id='fact_table_creation',
    sql='sql/table_creation_local/fact_table_creation.sql',
    postgres_conn_id=postgres_conn_id,
    dag=dag
)

table_state_check = TableStateCheckOperator(
    task_id='table_state_check',
    conn_id=postgres_conn_id,
    targets=('staging_reviews', 'staging_metadata',
             'reviews', 'users', 'products', 'time'),
    dag=dag
)

# --------------------------------------
#   Staging tables
# --------------------------------------
# upsert into staging tables per product category
json_to_postgres_reviews_ops = []

for category in Config.DATA_CATEGORIES:
    json_to_postgres = JSONToPostgresOperator(
        task_id=f'load_json_files_{category}',
        path=os.path.join(Config.DATA_DIR, 'uploaded',  'reviews', category),
        sql='sql/etl_local/load_to_staging_reviews.sql',
        loader=load_review_files,
        callback=partial(cache_filepaths, key=f'{Config.REVIEW_DATA_CACHE_KEY}_{category}'),
        error_callback=partial(cache_nofile_found, key=f'{Config.REVIEW_DATA_CACHE_KEY}_{category}'),
        dag=dag
    )

    json_to_postgres_reviews_ops.append(json_to_postgres)

json_to_postgres_metadata = JSONToPostgresOperator(
    task_id=f'load_json_files_metadata',
    path=os.path.join(Config.DATA_DIR, 'uploaded', 'metadata'),
    sql='sql/etl_local/load_to_staging_metadata.sql',
    loader=load_metadata_files,
    callback=partial(cache_filepaths, key=Config.METADATA_CACHE_KEY),
    error_callback=partial(cache_nofile_found, key=Config.METADATA_CACHE_KEY),
    dag=dag
)

staging_data_check = StagingDataCheckOperator(
    task_id='staging_data_check',
    conn_id=postgres_conn_id,
    tables=(('staging_reviews', Config.REVIEW_DATA_CACHE_KEY), ('staging_metadata', Config.METADATA_CACHE_KEY)),
    dag=dag
)

cleanup_staging_tables = CleanUpTablesOperator(
    task_id='cleanup_staging_tables',
    conn_id=postgres_conn_id,
    tables=('staging_reviews', 'staging_metadata'),
    dag=dag
)


# TODO: convert these operations to subdag?
#       (load and data check)
# insert data into tables and check data quality
# reviews
# --------------------------------------
#   Fact/Dimension tables
# --------------------------------------
sqlpath = 'sql/etl_local/{}'

staging_to_reviews = PostgresOperator(
    task_id='load_from_staging_to_reviews',
    postgres_conn_id=postgres_conn_id,
    sql=sqlpath.format('staging_to_reviews.sql'),
    dag=dag
)

data_check_reviews = DataCheckOperator(
    task_id='data_quality_check_reviews',
    table='reviews',
    conn_id=postgres_conn_id,
    cache_key=f'{Config.REVIEW_DATA_CACHE_KEY}_count',
    dag=dag
)

# users
staging_to_users = PostgresOperator(
    task_id='load_from_staging_to_users',
    postgres_conn_id=postgres_conn_id,
    sql=sqlpath.format('staging_to_users.sql'),
    dag=dag
)

data_check_users = DataCheckOperator(
    task_id='data_quality_check_users',
    table='users',
    conn_id=postgres_conn_id,
    dag=dag
)

# products
staging_to_products = PostgresOperator(
    task_id='load_from_staging_to_products',
    postgres_conn_id=postgres_conn_id,
    sql=sqlpath.format('staging_to_products.sql'),
    dag=dag
)

data_check_products = DataCheckOperator(
    task_id='data_quality_check_products',
    table='products',
    conn_id=postgres_conn_id,
    dag=dag
)

# time
staging_to_time = PostgresOperator(
    task_id='load_from_staging_to_time',
    postgres_conn_id=postgres_conn_id,
    sql=sqlpath.format('staging_to_time.sql'),
    dag=dag
)

data_check_time = DataCheckOperator(
    task_id='data_quality_check_time',
    table='time',
    conn_id=postgres_conn_id,
    dag=dag
)


move_review_files_ops = []

for category in Config.DATA_CATEGORIES:
    move_review_files = MoveDataLocalOperator(
        task_id=f'move_processed_review_files_{category}',
        from_path=fetch_filepaths_from_cache(f'{Config.REVIEW_DATA_CACHE_KEY}_{category}'),
        to_path=os.path.join(Config.DATA_DIR, 'raw', 'reviews'),
        dag=dag
    )

    move_review_files_ops.append(move_review_files)

move_metadata_files = MoveDataLocalOperator(
    task_id='move_processed_metadata_files',
    from_path=fetch_filepaths_from_cache(Config.METADATA_CACHE_KEY),
    to_path=os.path.join(Config.DATA_DIR, 'raw'),
    dag=dag
)

# --------------------------------------
#   Tensorflow
# --------------------------------------
write_to_tfrecord = ReviewToTFRecordOperator(
    task_id='write_to_tfrecord',
    conn_id=postgres_conn_id,
    dirpath=Config.TF_RECORD_DIR,
    queue='ml_queue',
    dag=dag
)

tf_record_file_check = FileCheckOperator(
    task_id='tf_record_file_creation_check',
    dirpath=Config.TF_RECORD_DIR,
    ext='.tfrecord',
    queue='ml_queue',
    dag=dag
)

train_ml_model = PythonOperator(
    task_id='train_model',
    python_callable=run_model.main,
    queue='ml_queue',
    dag=dag
)

remove_tfrecords = RemoveTFRecordOperator(
    task_id='remove_tfrecord_files',
    keep_ratio=0.1,
    queue='ml_queue',
    dag=dag
)

# --------------------------------------
#   Clean up
# --------------------------------------
clear_cache = RemoveCacheOperator(
    task_id='clear_cache',
    dag=dag,
)

# ================================================
#      Build DAG
# ================================================
# load data to staging tables
start_operator \
    >> [
        staging_tables_creation,
        dimension_tables_creation,
    ] \
    >> fact_table_creation \
    >> table_state_check \
    >> [
        *json_to_postgres_reviews_ops,
        json_to_postgres_metadata
    ] \
    >> staging_data_check


# load data from staging tables to main tables
staging_data_check \
    >> [
        staging_to_users,
        staging_to_products,
        staging_to_time,
    ]

# move processed files from uploaded directory to another directory
staging_data_check \
    >> [
        *move_review_files_ops,
        move_metadata_files,
    ] \
    >> clear_cache \
    >> end_operator

# run moachine learning model
staging_data_check \
    >> write_to_tfrecord \
    >> tf_record_file_check \
    >> train_ml_model \
    >> remove_tfrecords \
    >> end_operator

train_ml_model >> clear_cache

# wait to clean up staging table until write all data out
tf_record_file_check >> cleanup_staging_tables

# check the state and clean up
[
    staging_to_users >> data_check_users,
    staging_to_products >> data_check_products,
    staging_to_time >> data_check_time,
] \
    >> staging_to_reviews \
    >> [
        data_check_reviews,
        cleanup_staging_tables
    ] \
    >> clear_cache \
    >> end_operator
