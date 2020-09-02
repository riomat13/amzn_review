#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import glob
import random
from typing import List

from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook

from settings import Config, get_logger
from workflow.helper import redis_session

logger = get_logger('workflow.plugins.operators.cleaner')


class CleanUpTablesOperator(BaseOperator):

    ui_color = '#FFFFCC'

    def __init__(self,
                 conn_id: str,
                 tables: List[str],
                 *args, **kwargs):
        """Drop all tables
        """
        super(CleanUpTablesOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id

        if isinstance(tables, str):
            tables = [tables]
        self.tables = tables

    def execute(self, context):
        db_hook = PostgresHook(postgres_conn_id=self.conn_id)
        success = True

        for table in self.tables:
            try:
                db_hook.run(f'DROP TABLE IF EXISTS {table}')
            except Exception:
                logger.info(f'Failed to drop "{table}"')
            else:
                logger.info('Success to drop table "{table}"')


class RemoveFilesOperator(BaseOperator):

    ui_color = '#FFFFCC'

    def __init__(self,
                 paths: List[str] = None,
                 dirpath: str = None,
                 *args,
                 **kwargs):
        """Remove all files in directory."""
        if paths is None and dirpath is None:
            raise ValueError('Either `paths` or `dirpath` must be provided')

        if paths is not None:
            if dirpath is not None:
                logger.warning('Both `paths` and `dirpath` are provided. Ignore `dirpath`.')

            self.files = paths
        else:
            self.files = None
            self.dirpath = dirpath

        super(RemoveFilesOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        # randomly keep files for next iteration
        if self.files is None:
            self.files = map(lambda f: f.is_file(), os.scandir(dirpath))

        for f in self.files:
            os.remove(f)


class RemoveTFRecordOperator(BaseOperator):

    ui_color = '#FFFFCC'

    def __init__(self, keep_ratio=0.0, *args, **kwargs):
        """Remove all files in directory.
        if `keep_ratio` is set to float between 0.0 and 1.0,
        keep files by the ratio."""
        if keep_ratio < 0.0  or 1.0 < keep_ratio:
            raise ValueError('`keep_ratio` must be between 0.0 and 1.0')

        super(RemoveTFRecordOperator, self).__init__(*args, **kwargs)
        self.keep = keep_ratio

    def execute(self, context):
        if self.keep:
            # keep all files and no removal is run
            if self.keep == 1.0:
                return

            # randomly keep files for next iteration
            files = glob.glob(os.path.join(Config.TF_RECORD_DIR, '*.tfrecord'))
            random.shuffle(files)

            files = files[int(len(files) * self.keep):]
            for f in files:
                os.remove(f)
        else:
            # remove entire tfrecord files
            os.system(f'rm -f {Config.TF_RECORD_DIR}/*.tfrecord')


class RemoveCacheOperator(BaseOperator):

    ui_color = '#FFFFCC'

    def __init__(self, *args, **kwargs):
        super(RemoveCacheOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        with redis_session() as r:
            logger.info(f'Cache list: {r.keys("*")}')
            r.flushall()
