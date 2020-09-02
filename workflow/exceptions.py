#!/usr/bin/env python3
# -*- coding: utf-8 -*-


class BaseOperatorError(Exception):
    """Base exception for airflow task."""


class TableDoesNotExistError(BaseOperatorError):
    """Table does not exist in database."""


class NoRecordsFoundError(BaseOperatorError):
    """No records found in database."""


class InvalidRecordsFoundError(BaseOperatorError):
    """Exctacted records are invalid."""


class DataCheckFailed(BaseOperatorError):
    """Failed check data."""


class InvalidDataFormatError(BaseOperatorError):
    """Given data is not in expected format nor type."""


class ProcessFailedError(BaseOperatorError):
    """Failed process in task."""
