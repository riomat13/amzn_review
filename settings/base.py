#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import inspect

_dirname = os.path.dirname
ROOT_DIR = _dirname(_dirname(__file__))


class _DatabaseDSN(object):
    
    def __init__(self):
        self.dsn = None

    def __get__(self, instance, instance_type):
        if self.dsn is None:
            keys = (
                ('dbname', instance_type.DATABASE.get('DB_NAME')),
                ('user', instance_type.DATABASE.get('USERNAME')),
                ('password', instance_type.DATABASE.get('PASSWORD')),
                ('host', instance_type.DATABASE.get('HOST')),
                ('port', instance_type.DATABASE.get('PORT'))
            )
            self.dsn = ' '.join(f'{key}={prop}' for key, prop in keys)
        return self.dsn


class Config(object):
    MODE = 'DEVELOPMENT'

    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_DIR = os.path.join(ROOT_DIR, 'logs')

    # path to where all review data is stored
    DATA_DIR = os.getenv('DATA_DIR', os.path.join(ROOT_DIR, 'data'))
    # categories used for this project
    DATA_CATEGORIES = os.getenv('DATA_CATEGORIES', '').split(',')
    DATA_UPLOADER_WORKER = int(os.getenv('DATA_UPLOADER_WORKER', 2))

    # only consider to use postgresql
    DATABASE = {
        'DRIVER': 'postgresql+psycopg2',
        'DB_NAME': os.getenv('POSTGRES_DB'),
        'USERNAME': os.getenv('POSTGRES_USER'),
        'PASSWORD': os.getenv('POSTGRES_PASSWORD'),
        'HOST': os.getenv('POSTGRES_HOST', 'localhost'),
        'PORT': int(os.getenv('POSTGRES_PORT')),
    }

    DB_DSN = _DatabaseDSN()

    REDIS = {
        'HOST': os.getenv('REDIS_HOST'),
        'PORT': os.getenv('REDIS_PORT'),
        'DB': int(os.getenv('REDIS_DB', 0))
    }

    REVIEW_DATA_CACHE_KEY = 'loaded_review_files'
    METADATA_CACHE_KEY = 'loaded_metadata_files'

    AWS = {
        'ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID'),
        'SECRET_KEY': os.getenv('AWS_SECRET_ACCESS_KEY'),
        'REGION': os.getenv('AWS_REGION'),
        'VPC_SECURITY_GROUP': os.getenv('AWS_VPC_SECURITY_GROUP', 'default').split(','),
        'S3_BUCKET': os.getenv('AWS_S3_BUCKET'),
        'IAM_ROLE': os.getenv('AWS_IAM_ROLE'),
        'IAM_ROLE_ARN': os.getenv('AWS_IAM_ROLE_ARN'),
        'REDSHIFT': {
            'USERNAME': os.getenv('AWS_REDSHIFT_USER', 'appuser'),
            'PASSWORD': os.getenv('AWS_REDSHIFT_PASSWORD'),
            'DB_CLUSTER_IDENTIFIER': os.getenv('AWS_REDSHIFT_CLUSTER_IDENTIFIER', 'redshift-cluster-1'),
            'DB_NAME': os.getenv('AWS_REDSHIFT_DB_NAME', 'dev'),
            'NODE_TYPE': os.getenv('AWS_REDSHIFT_NODE_TYPE', 'dc2.large'),
            'NUMBER_OF_NODES': int(os.getenv('AWS_REDSHIFT_NUMBER_OF_NODES', 2)),
            'HOST': os.getenv('AWS_REDSHIFT_HOST'),
            'PORT': int(os.getenv('AWS_REDSHIFT_PORT', 5439)),
            'ARN': os.getenv('AWS_REDSHIFT_ARN'),
        },
    }

    TF_RECORD_DIR =  os.getenv('TF_RECORD_DIR')
    TF_RECORD_BATCH_SIZE = 32

    MODEL_CACHE_DIR = os.getenv('TRANSFORMER_MODEL_CACHE_DIR',
                                os.path.expanduser('~/.cache/transformer'))

    AIRFLOW = {
        'POSRGRES_CONN_ID': os.getenv('AIRFLOW_POSTGRES_CONN_ID'),
        'REDSHIFT_CONN_ID': os.getenv('AIRFLOW_REDSHIFT_CONN_ID')
    }

    @classmethod
    def test_config(cls):
        if cls.MODE != 'TEST':
            cls.MODE = 'TEST'
            cls.LOG_LEVEL = 'ERROR'

            # supress logs from tensorflow
            os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

            # set sqlite as primary database for test
            os.environ['AIRFLOW__CORE__SQL_ALCHEMY_CONN'] = 'sqlite:////tmp/test.db'
            os.environ['AIRFLOW__CORE__EXECUTOR'] = 'SequentialExecutor'
            os.environ['AIRFLOW__CORE__LOGGING_LEVEL'] = 'ERROR'

            from airflow.utils import db
            print('Initializing sqlite db for test...')
            db.initdb()

        return cls

    @classmethod
    def from_config(cls, config: dict):
        # TODO: nested settings
        for k, v in config.items():
            if isinstance(v, dict):
                if (d := getattr(cls, k, None)) is not None:
                    d.update(v)
            else:
                setattr(cls, k, v)

        return cls

    @classmethod
    def from_config_cls(cls,
                        config: object,
                        prefix: str = None,
                        allow_override: bool = False,
                        params_only: bool = True):
        if prefix is not None:
            prefix = prefix.upper()

        for attr in dir(config):
            if attr.startswith('_'):
                continue

            if not allow_override and getattr(cls, attr, None) is not None:
                continue

            prop = getattr(config, attr)

            # if not function(callable), make it upper case
            if inspect.isfunction(prop) or inspect.ismethod(prop):
                if params_only:
                    continue
            else:
                attr = attr.upper()
                if prefix is not None:
                    attr = f'{prefix}__{attr}'

            setattr(cls, attr, prop)

    @classmethod
    def print_all(cls):
        """Print all properties."""
        def clean_format(prop, level=0):
            if not isinstance(prop, dict):
                if isinstance(prop, str):
                    prop = f'"{prop}"'
                return prop

            offset = '\t' * level

            prop = '\n'.join(
                ['{'] + [f'\t{offset}{k}: {clean_format(v, level+1)}' for k, v in prop.items()] + [f'  {offset + "}"}']
            )
            return prop

        for key in dir(cls):
            if key.startswith('_') or \
                    inspect.isfunction((prop := getattr(cls, key))) or \
                    inspect.ismethod(prop):
                continue

            prop = clean_format(prop)
            print(f'  {key:<20}: {prop}')


# load settings from rating_model if binded
try:
    from rating_model.settings import Config as ModelConfig
    Config.from_config_cls(ModelConfig, allow_override=False)
except ImportError:
    pass


for dir_type in ('DATA_DIR', 'LOG_DIR', 'MODEL_DIR'):
    dirpath = getattr(Config, dir_type)
    if dirpath is not None and not os.path.isdir(dirpath):
        os.makedirs(dirpath)
