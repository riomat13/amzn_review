#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

import logging
import random

import unittest
from unittest.mock import patch

from amzn_review.tasks.writer import TFRecordReviewDataWriter

logging.getLogger().setLevel(level=logging.ERROR)


class CalledCounter:

    def __init__(self):
        self.called_count = 0
        self.args = []
        self.kwargs = []

    def call(self, *args, **kwargs):
        self.called_count += 1
        self.args.append(args)
        self.kwargs.append(kwargs)


class TFRecordWriterTest(unittest.TestCase):

    @patch('amzn_review.tasks.writer.os.makedirs')
    @patch('amzn_review.tasks.writer.tf.reshape', side_effect=lambda *x: x)
    @patch('amzn_review.tasks.writer.tf.py_function', side_effect=lambda *x: x)
    @patch('amzn_review.tasks.writer.tf.io.TFRecordWriter')
    def test_writer_handle_list_to_write_record_by_batch(self, MockWriter, *unused):

        batch_size = 4
        writer = TFRecordReviewDataWriter(
            dirpath='',
            batch_size=batch_size
        )
        counter = CalledCounter()

        def generate_random_token():
            length = random.randint(3, 10)
            return {
                'input_ids': [random.randint(1, 100) for _ in range(length)],
                'token_type_ids': [1] * length,
                'attention_mask': [0] * length,
            }

        data = [(generate_random_token(), random.randint(1, 5)) for _ in range(10)]

        target_file_count = (len(data) + batch_size - 1) // batch_size

        mock_context = MockWriter.return_value.__enter__
        mock_context.return_value.write.side_effect = counter.call

        writer(data)

        # number of times to open writer
        self.assertEqual(target_file_count, mock_context.call_count)

        # total number of times to write rows
        self.assertEqual(counter.called_count, len(data))


if __name__ == '__main__':
    unittest.main()
