#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

import unittest
from unittest.mock import patch

import datetime
import json

from airflow import DAG
from airflow.models import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.decorators import apply_defaults

from workflow.plugins.operators.commons import (
    CallbackMixin,
    VariableMixin,
    InvalidDataFormatError
)


DEFAULT_DATE = datetime.datetime(year=2020, month=1, day=1)


class MockVariableOperator(VariableMixin, BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(MockVariableOperator, self).__init__(*args, **kwargs)


class MockCallbackOperator(CallbackMixin, BaseOperator):

    @apply_defaults
    def __init__(self, counter=None, *args, **kwargs):
        super(MockCallbackOperator, self).__init__(*args, **kwargs)
        self.counter = counter

    def execute(self, context):
        if self.counter is not None:
            self.counter += 1
        self.callback()


class VariableMixinTest(unittest.TestCase):

    def setUp(self):
        self.dag = DAG(
            'test_variable_mixin',
            default_args={'start_date': DEFAULT_DATE},
        )

    @patch('workflow.plugins.operators.commons.get_variable')
    def test_load_data_and_set_to_operator(self, mock_get):

        variables = {
            'test_key1': 'test_value1',
            'test_key2': 'test_value2',
            'test-key3': 'test_value3',
        }

        mock_get.return_value = json.dumps(variables)

        op = MockVariableOperator(
            task_id='test1',
            variable_keys='test',
            dag=self.dag
        )

        for key, val in variables.items():
            # hyphen must be converted to underscore
            self.assertEqual(getattr(op, key), val)

    @patch('workflow.plugins.operators.commons.get_variable')
    def test_handle_invalid_data_by_raising_exception(self, mock_get):
        mock_get.return_value = json.dumps('key1')

        with self.assertRaises(InvalidDataFormatError):
            op = MockVariableOperator(
                task_id='test1',
                variable_keys='test',
                dag=self.dag
            )

        mock_get.return_value = json.dumps(['key1', 'value1'])

        with self.assertRaises(InvalidDataFormatError):
            MockVariableOperator(
                task_id='test2',
                variable_keys='test',
                dag=self.dag
            )

        mock_get.return_value = json.dumps([['key1', 'value1'], ['key2', 'value2']])

        with self.assertRaises(InvalidDataFormatError):
            MockVariableOperator(
                task_id='test3',
                variable_keys='test',
                dag=self.dag
            )

class CallbackMixinTest(unittest.TestCase):

    def setUp(self):
        self.dag = DAG(
            'test_callback_mixin',
            default_args={'start_date': DEFAULT_DATE},
        )

    def run_task(self, op):
        ti = TaskInstance(task=op, execution_date=DEFAULT_DATE)
        ti.run(ignore_ti_state=True)

    def test_run_execute_without_passing_callback(self):
        class Counter:
            def __init__(self):
                self.count = 0

            def __add__(self, val):
                self.count += val

        counter = Counter()
        op = MockCallbackOperator(task_id='test_no_callback',
                                  counter=counter,
                                  dag=self.dag)
        self.run_task(op)

        self.assertEqual(counter.count, 1)


    def test_call_back_is_called_after_execution(self):
        count = 0

        def test_fn():
            nonlocal count
            count += 1

        op = MockCallbackOperator(task_id='test_callback', dag=self.dag, callback=test_fn)
        self.run_task(op)

        self.assertEqual(count, 1)


if __name__ == '__main__':
    unittest.main()
