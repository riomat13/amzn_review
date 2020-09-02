#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

import concurrent.futures
from datetime import datetime
import glob
import gzip
import json
import subprocess
import time
from typing import List, Callable

import psycopg2

from airflow.models import Variable
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.utils.db import create_session
from airflow.secrets import get_variable

from settings import Config, get_logger, ROOT_DIR

from workflow.conn import update_connection, ConnectionConfig
from workflow.helper import redis_session
from workflow.plugins.operators.commons import CallbackMixin, VariableMixin
from workflow.exceptions import (
    DataCheckFailed,
    NoRecordsFoundError,
    ProcessFailedError,
    TableDoesNotExistError,
)


logger = get_logger('workflow.plugins.operators.etl')


def _setup_local():
    "Create directory used for processing if not exists."""
    for cat in Config.DATA_CATEGORIES:
        path = os.path.join(Config.DATA_DIR, 'uploaded', 'reviews', cat)
        if not os.path.isdir(path):
            os.makedirs(path)

        path = os.path.join(Config.DATA_DIR, 'raw', 'reviews', cat)
        if not os.path.isdir(path):
            os.makedirs(path)

        path = os.path.join(Config.DATA_DIR, 'staging', 'reviews', cat)
        if not os.path.isdir(path):
            os.makedirs(path)

    path = os.path.join(Config.DATA_DIR, 'raw', 'metadata')
    if not os.path.isdir(path):
        os.makedirs(path)


class SetupOperator(BaseOperator):
    """Check initial state and setup database."""

    ui_color = '#ededed'

    @apply_defaults
    def __init__(self,
                 mode: str,
                 set_variable_keys: dict = None,
                 *args,
                 **kwargs):
        """Setup workflow.

        Update `conn_id` to current state.

        Args:
            mode: str
                run mode either `local` or `redshift`
            set_variable_keys: dict (default: None)
                variable key name to be set on Airflow Variable
                if not set, take value from `value` name of the dict from env

                example:
                    In `.env` file,
                        AIRFLOW_SOME_PARAMTER=setup
                    and provide set_variable_keys={'some_param': 'AIRFLOW_SOME_PARAMTER',...},
                    then,
                        key='some_param', value='setup' will be registered on
                        Airflow Variable.
        """
        mode = mode.lower()
        if mode not in ('local', 'redshift'):
            raise ValueError('Invalid `mode`. Choose from either `local` or `redshift`')

        super(SetupOperator, self).__init__(*args, **kwargs)
        self.mode = mode
        self.set_variable_keys = set_variable_keys or {}

    def execute(self, context):
        # check if table is created
        # if not, create them
        logger.info('Setting up operator')

        with redis_session() as r:
            # TODO: find other way to handle data race (round-robin?)
            while r.keys('*'):
                logger.info('Not finished previous run, wait for 300 seconds.')
                time.sleep(300)

        start = time.perf_counter()

        if self.mode == 'local':
            _setup_local()
            db_cfg = Config.DATABASE
            conn_id = os.getenv('AIRFLOW_POSTGRES_CONN_ID')
        elif self.mode == 'redshift':
            db_cfg = Config.AWS['REDSHIFT']
            conn_id = os.getenv('AIRFLOW_REDSHIFT_CONN_ID')

        cfg = ConnectionConfig(
            conn_id=conn_id,
            host=db_cfg['HOST'],
            login=db_cfg['USERNAME'],
            password=db_cfg['PASSWORD'],
            schema=db_cfg['DB_NAME'],
            port=db_cfg['PORT']
        )

        update_connection(cfg)

        for key, val in self.set_variable_keys.items():
            logger.info(f'Setting key="{key}" to Airflow Variable')

            variable = get_variable(key=key)
            if variable is None:
                variable = Variable(key=key)
                variable.set_val(value=val)

                with create_session() as sess:
                    sess.add(variable)

        end = time.perf_counter()
        logger.info(f'Process Time [{self.__class__.__name__}]: {end-start:.3f} sec.')


class TableStateCheckOperator(BaseOperator):

    ui_color = '#FFFFFF'

    @apply_defaults
    def __init__(self,
                 conn_id: str,
                 targets: List[str],
                 *args, **kwargs):
        """Test table exists or is created.

        Args:
            conn_id: str
                connection_id to PostgreSQL registered on Airflow
        """
        super(TableStateCheckOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.targets = targets

    def execute(self, context):
        db_hook = PostgresHook(postgres_conn_id=self.conn_id)

        records = db_hook.get_records(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        )

        tables = {table for table, *_ in records}

        for target_table in self.targets:
            if target_table not in tables:
                raise TableDoesNotExistError(f'"{target_table}" does not exist')

            records = db_hook.get_records(f'SELECT COUNT(*) FROM {target_table}')

            # store data count before storing
            with redis_session() as r:
                r.set(f'count_{target_table}_before', records[0][0])

        logger.info('Passed all tables existence and cached initial states')


class NaiveJSONToPostgresOperator(BaseOperator):
    """Load json file and copy to PostgreSQL.
    This can be used for local operation.
    """

    template_ext = ('.sql',)
    template_fields = ('sql',)

    ui_color = '#CCFFFF'

    @apply_defaults
    def __init__(self,
                 path: str,
                 sql: str,
                 conn_id: str,
                 loader: Callable,
                 *args, **kwargs):
        """Naive implementation for loading json files.

        This is only for test purpose.
        """
        super(NaiveJSONToPostgresOperator, self).__init__(*args, **kwargs)

        self.path = path
        self.sql = sql
        self.conn_id = conn_id
        self.loader = loader

    def execute(self, context):
        start = time.perf_counter()

        hook = PostgresHook(postgres_conn_id=self.conn_id)

        files = glob.iglob(os.path.join(self.path, '*.json.gz'))

        count = failed = 0

        for data in self.loader(files):
            count += 1
            try:
                hook.run(self.sql, parameters=data)
            except Exception:
                failed += 1

        end = time.perf_counter()
        logger.info(f'Finished data insertion: Success - {failed} Failed - {count}')
        logger.info(f'Process Time [{self.__class__.__name__}]: {end-start:.3f} sec.')


class JSONToPostgresOperator(CallbackMixin, VariableMixin, BaseOperator):
    """Load json file and copy to PostgreSQL.
    This can be used for local operation.
    """

    template_ext = ('.sql',)
    template_fields = ('sql',)

    ui_color = '#CCFFFF'

    @apply_defaults
    def __init__(self,
                 path: str,
                 sql: str,
                 loader: Callable,
                 *args, **kwargs):
        """Load data from json files to PostgreSQL.

        Data must be gzipped json files (`.json.gz`)

        Args:
            path: str
                path to directory storing json files
            sql: str
                path to sql file or query
            loader: function
            category: str
                category name of data
                the file path format must follow `${path}/${category}/*.json.gz`
            callback: function
                if given, execute the callback for each load
            error_callback: function
                if given, execute the callback when `callback` failed to execute
            variable_keys(optional): str or list of str
                set variable keys registered on airflow server if use
        """
        super(JSONToPostgresOperator, self).__init__(*args, **kwargs)

        self.path = path
        self.sql = sql
        self.loader = loader

    def execute(self, context):
        start = time.perf_counter()

        conn = psycopg2.connect(Config.DB_DSN)
        cur = conn.cursor()

        files = glob.glob(os.path.join(self.path, '*.json.gz'))
        if files:
            logger.info(f'Total {len(files)} file objects found.')
        else:
            logger.info(f'No file objects found. Skip the operation.')
            self.error_callback()
            return

        BATCH_SIZE = 100000

        # grouping files
        file_batches = [files[idx:idx+BATCH_SIZE] for idx in range(0, len(files), BATCH_SIZE)]
        available = min(os.cpu_count(), len(file_batches))

        count = 0

        logger.info(f'Number of max workers: {available}')

        try:
            with concurrent.futures.ProcessPoolExecutor(max_workers=available) as executor:
                for batch, batch_files in zip(executor.map(self.loader, file_batches), file_batches):
                    try:
                        psycopg2.extras.execute_values(cur, self.sql, batch)
                        count += len(batch)
                    except (KeyError, ValueError) as e:
                        # TODO: handle invalid data and retry?
                        logger.error('Invalid data is captured')
                        raise
                    except Exception as e:
                        logger.error(f'{type(e).__name__}: {e}')
                        logger.error(f'Failed to insert: {batch}')
                        raise
                    #else:
                    #    self.callback(filepaths=batch_files)

            conn.commit()
        except Exception as e:
            logger.error(f'Failed to commit: {type(e).__name__}: {e}')
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

        end = time.perf_counter()
        logger.info(f'Finished data insertion: Success - {count} Failed - {len(files)-count}')
        logger.info(f'Process Time [{self.__class__.__name__}]: {end-start:.3f} sec.')


class PostgresQueryOperator(VariableMixin, BaseOperator):
    """Load json file and copy to PostgreSQL.
    This can be used for local operation.

    This is almost identical to `PostgresOperator`.
    The only difference is less logging.
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)

    ui_color = '#CCFFFF'

    @apply_defaults
    def __init__(self,
                 conn_id: str,
                 sql: str,
                 parameters,
                 *args, **kwargs):
        """Load data from S3 bucket to staging table.

        Args:
            conn_id: str
                connection_id to PostgreSQL registered on Airflow Connection
            sql: str or path to `.sql` file
                sql query for the task
            parameters
                parameters to be passed to PostgreSQL query
            variable_keys(optional): str or list of str
                set variable keys registered on airflow server if use
        """
        super(PostgresQueryOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.sql = sql

    def execute(self, context):
        logger.info(f'Executing: {self.sql}')
        start = time.perf_counter()
        db_hook = PostgresHook(postgres_conn_id=self.conn_id)
        db_hook.run(self.sql)
        end = time.perf_counter()
        logger.info(f'Process Time [{self.__class__.__name__}]: {end-start:.3f} sec.')


# TODO: add argument for table name for flexibility
class StagingDataCheckOperator(VariableMixin, BaseOperator):
    """Check data of staging table."""

    ui_color = '#FFFFFF'

    def __init__(self,
                 conn_id: str,
                 tables: List[str] or List[List[str]],
                 *args, **kwargs):
        """Check data state in staging table.

        Args:
            conn_id: str
                connection_id to PostgreSQL registered on Airflow
            tables: list(str) or list(list(str))
                table list to check data,
                if list of list is provided, the seconda value of the inner list
                should be a key the data is stored in cache
                if key name is wrong, tested only data existence
            variable_keys(optional): str or list of str
                set variable keys registered on airflow server if use
        """
        super(StagingDataCheckOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.tables = tables

    def execute(self, context):
        logger.info(f'Checking data for staging table')

        db_hook = PostgresHook(postgres_conn_id=self.conn_id)

        with redis_session() as r:
            for table in self.tables:
                if isinstance(table, str):
                    key = None
                else:
                    table, key = table

                records = db_hook.get_records(f'SELECT COUNT(*) FROM {table}')

                if not records:
                    raise DataCheckFailed(f'No records found in "{table}"')

                expected = 0

                if key is not None:
                    # TODO: remove table name specific operation
                    if table == 'staging_reviews':
                        for cat in Config.DATA_CATEGORIES:
                            expected += int(r.get(f'{key}_{cat}_count') or 0)
                        r.set(f'{key}_count', expected)
                    else:
                        # can be empty
                        expected = int(r.get(f'{key}_count') or 0)

                if expected:
                    if records[0][0] != expected:
                        raise DataCheckFailed(
                            f'Data size in "{table}" did not match expectation '
                            f'- expected: {expected} - actual {records[0][0]}'
                        )
                else:
                    if not records[0][0]:
                        raise DataCheckFailed(f'No data is stored in database')

                logger.info(f'Passed data quality check for "{table}" - Data count: {records[0][0]}')


class DataCheckOperator(VariableMixin, BaseOperator):

    ui_color = '#FFFFFF'

    def __init__(self,
                 conn_id: str,
                 table: str,
                 cache_key: str = None,
                 *args, **kwargs):
        """Operator to transfer data from staging table to fact table.

        Args:
            conn_id: str
                connection_id registered on Airflow to be used
                for connecting to Database
            table: str
                table name
            cache_key: str
                if provided, target count is fetched from cache table
                and check exact match,
                otherwise check only data count is non-zero
        """
        super(DataCheckOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.table = table
        self.cache_key = cache_key

    def execute(self, context):
        logger.info(f'Checking data for "{self.table}"')

        db_hook = PostgresHook(postgres_conn_id=self.conn_id)
        records = db_hook.get_records(f'SELECT COUNT(*) FROM {self.table}')

        if not records:
            raise NoRecordsFoundError(f'No records found in {self.table}')

        if self.cache_key is not None:
            with redis_session() as r:
                added_count = records[0][0] - int(r.get(f'count_{self.table}_before') or 0)

                target = int(r.get(f'{self.cache_key}'))

            if added_count != target:
                raise DataCheckFailed(f'Data count does not match: {records[0][0]} != {target}')

            logger.info('Exact match to target count')
        elif records[0][0]:
            # only check data exists in table
            # this is used for table which may have duplicates such as users, products etc.
            with redis_session() as r:
                added_count = records[0][0] - int(r.get(f'count_{self.table}_before') or 0)

            logger.info(f'Found at least one record in table "{self.table}"')
            logger.info(f'Added data count:{added_count}')
        else:
            raise NoRecordsFoundError(f'No records found in {self.table}')

        logger.info(f'Passed data quality check for "{self.table}"')
