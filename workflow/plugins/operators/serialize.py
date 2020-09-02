#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import tensorflow as tf

from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults

from settings import Config, get_logger
from amzn_review.tasks.writer import TFRecordReviewDataWriter
from rating_model.models import get_tokenizer

from workflow.plugins.operators.commons import VariableMixin
from workflow.exceptions import (
    InvalidRecordsFoundError,
    NoRecordsFoundError
)

logger = get_logger('workflow.plugins.operators.serialize')


class ReviewToTFRecordOperator(VariableMixin, BaseOperator):

    ui_color = '#FFCC99'

    @apply_defaults
    def __init__(self,
                 conn_id: str,
                 dirpath: str,
                 *args, **kwargs):
        """Write review data to TFRecord files.

        Args:
            conn_id: str
                connection_id to PostgreSQL registered on Airflow
            dirpath: str
                path to directory to store tfrecord files
            variable_keys(optional): str or list of str
                set variable keys registered on airflow server if use
        """
        super(ReviewToTFRecordOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.dirpath = dirpath

    def execute(self, context):
        db_hook = PostgresHook(postgres_conn_id=self.conn_id)
        tokenizer = get_tokenizer()

        records = db_hook.get_records('SELECT review_text, rating FROM staging_reviews WHERE review_text IS NOT NULL;')

        logger.info(records)

        # sanity check
        if not records:
            raise NoRecordsFoundError('No records found')
        if len(records[0]) != 2:
            raise InvalidRecordsFoundError('Invalid record to be extracted')

        # convert text to token as dict to be serializable
        # TODO: change max_length to be more flexible or not specify and train model by stochastic
        #records.sort(key=lambda x: len(x[0].split()))
        records = map(lambda row: (tokenizer(row[0], max_length=100, padding='max_length', truncation=True).data, row[1]), records)

        writer = TFRecordReviewDataWriter(dirpath=self.dirpath, batch_size=Config.TF_RECORD_BATCH_SIZE)
        writer(records)

        logger.info('Wrote to tfrecord files')
