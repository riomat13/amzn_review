#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from airflow.plugins_manager import AirflowPlugin

from .operators.cleaner import (
    CleanUpTablesOperator,
    RemoveCacheOperator,
    RemoveFilesOperator,
    RemoveTFRecordOperator
)
from .operators.commons import (
    FileCheckOperator,
    MoveDataLocalOperator,
)
from .operators.etl import (
    DataCheckOperator,
    JSONToPostgresOperator,
    NaiveJSONToPostgresOperator,
    PostgresQueryOperator,
    SetupOperator,
    StagingDataCheckOperator,
    TableStateCheckOperator
)
from .operators.serialize import ReviewToTFRecordOperator


class CustomCommonPlugin(AirflowPlugin):
    name = 'custom_common_plugin'

    hooks = []

    operators = [
        FileCheckOperator,
        MoveDataLocalOperator,
        RemoveFilesOperator
    ]

    executors = []


class CustomETLPlugin(AirflowPlugin):
    name = 'etl_plugin'

    hooks = []

    operators = [
        CleanUpTablesOperator,
        DataCheckOperator,
        JSONToPostgresOperator,
        NaiveJSONToPostgresOperator,
        PostgresQueryOperator,
        RemoveCacheOperator,
        SetupOperator,
        StagingDataCheckOperator,
        TableStateCheckOperator
    ]

    executors = []


class CustomTFPlugin(AirflowPlugin):
    name = 'tf_plugin'

    hooks = []

    operators = [
        RemoveTFRecordOperator,
        ReviewToTFRecordOperator
    ]

    executors = []
