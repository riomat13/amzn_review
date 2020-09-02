#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from datetime import datetime, timedelta

from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.custom_common_plugin import (
    FileCheckOperator
)
from airflow.operators.etl_plugin import (
    CleanUpTablesOperator,
    DataCheckOperator,
    #LoadStagingToTableOperator,
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
from workflow.dags import run_model


redshift_conn_id = os.environ.get('AIRFLOW_REDSHIFT_CONN_ID')

default_args = {
    'owner': 'user',
    'depends_on_past': True,
    'start_date': datetime.today(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': True
}

# run at 0:00 on every Sunday
dag = DAG(
    'amzn_review_etl_aws',
    default_args=default_args,
    description='ETL pipelining on AWS',
    schedule_interval='0 7 * * *',
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
    mode='redshift',
    set_variable_keys={
        's3_bucket': Config.AWS['S3_BUCKET'],
        'access_key_id': Config.AWS['ACCESS_KEY_ID'],
        'secret_access_key': Config.AWS['SECRET_KEY'],
        'aws_region': Config.AWS['REGION'],
        'review_jsonpath': 'staging_reviews_jsonpath.json',
        'metadata_jsonpath': 'staging_metadata_jsonpath.json',
    },
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
    sql='sql/table_creation/staging_tables_creation.sql',
    postgres_conn_id=redshift_conn_id,
    dag=dag
)

link_table_creation = PostgresOperator(
    task_id='link_table_creation',
    sql='sql/table_creation/link_table_creation.sql',
    postgres_conn_id=redshift_conn_id,
    dag=dag
)

dimension_tables_creation = PostgresOperator(
    task_id='dimension_tables_creation',
    sql='sql/table_creation/dimension_tables_creation.sql',
    postgres_conn_id=redshift_conn_id,
    dag=dag
)

fact_table_creation = PostgresOperator(
    task_id='fact_table_creation',
    sql='sql/table_creation/fact_table_creation.sql',
    postgres_conn_id=redshift_conn_id,
    dag=dag
)

climate_tables_creation = PostgresOperator(
    task_id='climate_tables_creation',
    sql='sql/table_creation/climate_tables_creation.sql',
    postgres_conn_id=redshift_conn_id,
    dag=dag
)


table_state_check = TableStateCheckOperator(
    task_id='table_state_check',
    conn_id=redshift_conn_id,
    targets=('staging_reviews', 'staging_product_metadata',
             'staging_climate', 'staging_stations',
             'reviews', 'users', 'products', 'climate', 'stations', 'time'),
    dag=dag
)


# --------------------------------------
#   Amazon Review
# --------------------------------------
sqlpath = 'sql/etl/amazon/{}'

# upsert into staging tables per product category
json_to_redshift_reviews = PostgresOperator(
    task_id='load_json_files_reviews',
    sql=sqlpath.format('load_to_staging_reviews.sql'),
    postgres_conn_id=redshift_conn_id,
    dag=dag
)

json_to_redshift_product_metadata = PostgresOperator(
    task_id='load_json_files_product_metadata',
    sql=sqlpath.format('load_to_staging_product_metadata.sql'),
    postgres_conn_id=redshift_conn_id,
    dag=dag
)

staging_data_check = StagingDataCheckOperator(
    task_id='staging_data_check',
    conn_id=redshift_conn_id,
    tables=('staging_reviews', 'staging_product_metadata',
            'staging_climate', 'staging_stations'),
    dag=dag
)

# load staging tables to main tables
staging_to_reviews = PostgresOperator(
    task_id='load_from_staging_to_reviews',
    postgres_conn_id=redshift_conn_id,
    sql=sqlpath.format('staging_to_reviews.sql'),
    dag=dag
)

data_check_reviews = DataCheckOperator(
    task_id='data_quality_check_reviews',
    table='reviews',
    conn_id=redshift_conn_id,
    expected=0,
    dag=dag
)

# users
staging_to_users = PostgresOperator(
    task_id='load_from_staging_to_users',
    postgres_conn_id=redshift_conn_id,
    sql=sqlpath.format('staging_to_users.sql'),
    dag=dag
)

data_check_users = DataCheckOperator(
    task_id='data_quality_check_users',
    table='users',
    conn_id=redshift_conn_id,
    expected=0,
    dag=dag
)

# products
staging_to_products = PostgresOperator(
    task_id='load_from_staging_to_products',
    postgres_conn_id=redshift_conn_id,
    sql=sqlpath.format('staging_to_products.sql'),
    dag=dag
)

data_check_products = DataCheckOperator(
    task_id='data_quality_check_products',
    table='products',
    conn_id=redshift_conn_id,
    expected=0,
    dag=dag
)

# time
staging_reviews_to_time = PostgresOperator(
    task_id='load_from_staging_reviews_to_time',
    postgres_conn_id=redshift_conn_id,
    sql=sqlpath.format('staging_reviews_to_time.sql'),
    dag=dag
)

# --------------------------------------
#   Climate data
# --------------------------------------
sqlpath = 'sql/etl/climate/{}'

# upload to staging tables
json_to_redshift_climate = PostgresOperator(
    task_id='load_json_files_climate',
    sql=sqlpath.format('load_to_staging_climate.sql'),
    postgres_conn_id=redshift_conn_id,
    dag=dag
)

json_to_redshift_stations = PostgresOperator(
    task_id='load_json_files_station_metadata',
    sql=sqlpath.format('load_to_staging_station_metadata.sql'),
    postgres_conn_id=redshift_conn_id,
    dag=dag
)

# load staging tables to main tables
# climate
staging_to_climate = PostgresOperator(
    task_id='load_from_staging_climate',
    postgres_conn_id=redshift_conn_id,
    sql=sqlpath.format('staging_to_climate.sql'),
    dag=dag
)

data_check_climate = DataCheckOperator(
    task_id='data_quality_check_climate',
    table='climate',
    conn_id=redshift_conn_id,
    expected=0,
    dag=dag
)

# stations
staging_to_stations = PostgresOperator(
    task_id='load_from_staging_stations',
    postgres_conn_id=redshift_conn_id,
    sql=sqlpath.format('staging_to_stations.sql'),
    dag=dag
)

data_check_stations = DataCheckOperator(
    task_id='data_quality_check_stations',
    table='stations',
    conn_id=redshift_conn_id,
    expected=0,
    dag=dag
)

staging_climate_to_time = PostgresOperator(
    task_id='load_from_staging_climate_to_time',
    postgres_conn_id=redshift_conn_id,
    sql=sqlpath.format('staging_climate_to_time.sql'),
    dag=dag
)

# --------------------------------------
#   Link table
# --------------------------------------
data_check_time = DataCheckOperator(
    task_id='data_quality_check_time',
    table='time',
    conn_id=redshift_conn_id,
    expected=0,
    dag=dag
)


# --------------------------------------
#   Clean up
# --------------------------------------
# clean up all staging tables
cleanup_staging_tables = CleanUpTablesOperator(
    task_id='cleanup_staging_table',
    conn_id=redshift_conn_id,
    tables=['staging_reviews', 'staging_product_metadata',
            'staging_climate', 'staging_stations'],
    dag=dag
)

# --------------------------------------
#   Tensorflow
# --------------------------------------
# preprocess for ML model
write_to_tfrecord = ReviewToTFRecordOperator(
    task_id='write_to_tfrecord',
    conn_id=redshift_conn_id,
    dirpath=Config.TF_RECORD_DIR,
    dag=dag
)

tf_record_file_check = FileCheckOperator(
    task_id='tf_record_file_creation_check',
    dirpath=Config.TF_RECORD_DIR,
    ext='.tfrecord',
    dag=dag
)

# skip ML training
train_ml_model = PythonOperator(
    task_id='train_model',
    python_callable=lambda: None,
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
# --------------------------------------
#   Staging tables
# --------------------------------------
start_operator >> staging_tables_creation >> table_state_check

# --------------------------------------
#   Amazon reviews
# --------------------------------------
start_operator \
    >> link_table_creation \
    >> dimension_tables_creation \
    >> fact_table_creation \
    >> table_state_check \
    >> json_to_redshift_reviews \
    >> staging_data_check

table_state_check \
    >> json_to_redshift_product_metadata \
    >> staging_data_check \
    >> [
        staging_to_users,
        staging_to_products,
        staging_reviews_to_time,
    ]

# --------------------------------------
#   Climate data
# --------------------------------------
link_table_creation \
    >> climate_tables_creation \
    >> table_state_check \
    >> [
        json_to_redshift_climate,
        json_to_redshift_stations
    ] \
    >> staging_data_check

# load data from staging tables to main tables
staging_data_check \
    >> [
        staging_to_climate,
        staging_to_stations,
        staging_climate_to_time
    ]

# write out to tfrecord files
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
    staging_to_climate >> data_check_climate,
    staging_to_stations >> data_check_stations,
    staging_reviews_to_time >> data_check_time,
    staging_climate_to_time >> data_check_time,
] \
    >> staging_to_reviews \
    >> [
        data_check_reviews,
        cleanup_staging_tables,
    ] \
    >> clear_cache \
    >> end_operator
