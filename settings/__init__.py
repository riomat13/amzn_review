#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging

from .base import Config, ROOT_DIR


if not os.path.isdir(Config.LOG_DIR):
    os.makedirs(Config.LOG_DIR)


def get_logger(name, level=None):
    # TODO: __name__ will be absolute path if run via airflow
    # check it and set file name as `module name + '.log'`
    if level is None:
        level = Config.LOG_LEVEL
    else:
        level = level.upper()

    if level not in ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'):
        raise ValueError('Invalid log level')

    logger = logging.getLogger(name)
    level = getattr(logging, level)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s] %(message)s')

    logger.setLevel(level)

    fh = logging.FileHandler(os.path.join(Config.LOG_DIR, f'{name}.log'))
    fh.setLevel(level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    if Config.MODE != 'TEST':
        sh = logging.StreamHandler()
        sh.setLevel(level)
        sh.setFormatter(formatter)
        logger.addHandler(sh)

    return logger


__all__ = ['ROOT_DIR', 'Config', 'get_logger']
