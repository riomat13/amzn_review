#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

import glob
import json
import shutil

from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.secrets import get_variable

from settings import get_logger
from workflow.helper import makedir, redis_session
from workflow.exceptions import  (
    InvalidDataFormatError,
    ProcessFailedError
)


logger = get_logger('workflow.plugins.operators.commons')


class VariableMixin(object):

    def __init__(self, *args, **kwargs):
        if 'variable_keys' in kwargs:
            variable_keys = kwargs.pop('variable_keys')
            self.load_variables(variable_keys)

        super(VariableMixin, self).__init__(*args, **kwargs)

    def load_variables(self, keys: str or List[str]):
        """Take variables from registered json formatted variables
        and store them to operator's attributes.

        If variable is empty or inappropriately formatted,
        raise exception.

        Example:
            Set value with following format on airflow:
                key:   somple_key
                value: {
                          "timeout": 20,
                          "count": 10
                        }

            and you can get value by:
                >>> operator = SomeCustomOperator(variable_keys='sample_key')
                >>> operator.timeout == 20
                True
                >>> operator.count == 10
                True
        """
        if keys is None:
            return

        if isinstance(keys, str):
            # make iterable
            keys = (keys,)

        for key in keys:
            variables = get_variable(key.replace('-', '_'))

            if variables is None:
                raise InvalidDataFormatError(f'"{key}" is not registered.')

            variables = json.loads(variables)
            if not isinstance(variables, dict):
                raise InvalidDataFormatError(f'"{variables}" is not key-value pairs.')

            for k, v in variables.items():
                setattr(self, k, v)


def _entity(*args, **kwargs):
    return args, kwargs


class CallbackMixin(object):

    def __init__(self, *args, **kwargs):
        if 'callback' in kwargs:
            self.callback = kwargs.pop('callback')
        else:
            self.callback = _entity

        if 'error_callback' in kwargs:
            self.error_callback = kwargs.pop('error_callback')
        else:
            self.error_callback = _entity

        super(CallbackMixin, self).__init__(*args, **kwargs)

    def callback(self, *args, **kwargs):
        pass


class FileCheckOperator(VariableMixin, BaseOperator):

    ui_color = '#FFFFFF'

    @apply_defaults
    def __init__(self,
                 dirpath: str,
                 ext: str = None,
                 expected: int = 0,
                 save_as: str = None,
                 *args, **kwargs):
        """Count files in given directory which have the specified extension.

        Args:
            dirpath: str
                path to directory containing files to count
            ext: str
                extension of files to search if provided
            expected: int (default: 0)
            save_as: str (default: None)
                if set, store the count with this variable as key in cache
            variable_keys(optional): str or list of str
                set variable keys registered on airflow server if use
        """
        super(FileCheckOperator, self).__init__(*args, **kwargs)

        self.dirpath = dirpath
        self.ext = ext
        self.expected = expected
        self.save_key = save_as

    def execute(self, context):
        logger.info(f'Checking "*.{self.ext}" files in "{self.dirpath}"')

        with os.scandir(self.dirpath) as it:
            if self.ext is not None:
                count = len([
                    f for f in it
                    if f.is_file() and os.path.splitext(f.name)[-1] == self.ext
                ])
            else:
                count = len([f for f in it if f.is_file()])

        if not self.expected:
            if not count:
                raise DataCheckFailed('Not found files')
            logger.info(f'Found {count} files.')
        else:
            if count != self.expected:
                raise DataCheckFailed(
                    'File count does not match as expected: '
                    f'expected - {self.expected} actual - {count}'
                )

        if self.save_key is not None:
            with redis_session() as r:
                r.set(self.save_key, count)
            logger.info(f'Cached the file count: {self.save_key}={count}')

        logger.info('File count check passed')


# TODO: include different machine/server as destination to move data
class MoveDataLocalOperator(VariableMixin, BaseOperator):

    ui_color = '#FFCCFF'

    @apply_defaults
    def __init__(self,
                 from_path: str or List[str],
                 to_path: str,
                 *args, **kwargs):
        """Move file objects to target path.
        This can be moved from either local or other location in S3.

        Args:
            from_path: str or List[str]
                filepath(s) to move
            to_path: str
                target directory path
            variable_keys(optional): str or list of str
                set variable keys registered on airflow server if use
        """
        super(MoveDataLocalOperator, self).__init__(*args, **kwargs)

        makedir(to_path)

        if isinstance(from_path, str):
            if os.path.isdir(from_path):
                self.files = glob.glob(os.path.join(from_path, '*'))
            elif os.path.isfile(from_path):
                self.files = (from_path,)
            else:
                raise ValueError('Cound not find `from_path`')
        else:
            self.files = from_path

        self.to_path = to_path

    def execute(self, context):
        moved_cnt = 0

        for f in self.files:
            category = os.path.basename(os.path.dirname(f))
            shutil.move(f, os.path.join(self.to_path, category, os.path.basename(f)))
            moved_cnt += 1

        logger.info(f'Moved {moved_cnt} files to "{self.to_path}"')


class MoveDataInS3Operator(VariableMixin, BaseOperator):

    ui_color = '#FFCCFF'

    @apply_defaults
    def __init__(self,
                 from_key: str,
                 to_key: str,
                 *args, **kwargs):
        """Move file objects inside S3 bucket.

        Args:
            from_key: str
            to_key: str
                prefix key name in S3 bucket
            variable_keys(optional): str or list of str
                set variable keys registered on airflow server if use
        """
        super(MoveDataInS3Operator, self).__init__(*args, **kwargs)

        self.from_key = from_key
        self.to_key = to_key

    def execute(self, context):
        # to savely move files, first copy the files and then remove orignal files
        self.sub_proc = subprocess.Popen(
            ['aws', 's3', 'mv',
             f's3://{Config.AWS["S3_BUCKET"]}/{self.from_key}/',
             f's3://{Config.AWS["S3_BUCKET"]}/{self.to_key}/',
             '--recursive'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        self.sub_proc.wait()

        if self.sub_proc.returncode:
            raise ProcessFailedError('Failed to move files')

        logger.info(f'Moved files from "{self.from_path}" to "{self.to_path}"')

    def on_kill(self):
        if self.sub_proc and hasattr(self.sub_proc, 'pid'):
            logger.info('Terminating the process')
            os.killpg(os.getpgid(self.sub_proc.pid), signal.SIGTERM)
