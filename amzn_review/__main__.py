#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import warnings

import itertools
import subprocess

import argcomplete

from amzn_review.cli import get_parser
from amzn_review.aws import (
    create_redshift_cluster,
    redshift_role_creation,
    show_cluster_info,
)

from settings import Config, get_logger


logger = get_logger('amzn_review.main')


def _handle_app(args):
    if args.type == 'config':
        if args.subtype == 'show':
            print('\nMain App Configuration')
            print('----------------------\n')
            Config.print_all()


def _handle_db(args):
    # TODO
    warnings.warn('Main database on local. This is not implemented yet')


def _handle_data_uploader(args):
    if args.type == 'run':
        if args.local:
            print('Data uploader in local')
            command = ['pipenv', 'run', 'python', 'amzn_review/data/uploader.py', '--mode', 'local']

            with open(os.path.join(Config.LOG_DIR, 'main.uploader.run_local.log'), 'a') as f:
                proc = subprocess.Popen(command, stdout=f, stdin=f)
        else:
            print('Data uploader to S3')
            command = ['pipenv', 'run', 'python', 'amzn_review/data/uploader.py', '--mode', 'aws']

            with open(os.path.join(Config.LOG_DIR, 'main.uploader.run.log'), 'a') as f:
                proc = subprocess.Popen(command, stdout=f, stdin=f)

        proc.wait()


def _handle_airflow(args):
    # TODO
    warnings.warn('This is not implemented yet. Use `airflow` cli instead.')

def _handle_AWS(args):
    # update AWS access key and secret key
    if args.aws_access_key is not None:
        Config.AWS['ACCESS_KEY_ID'] = args.aws_access_key

    if args.aws_secret_key is not None:
        Config.AWS['SECRET_KEY'] = args.aws_secret_key

    if args.type == 'iam':
        if args.subtype == 'role':
            if args.create:
                role = redshift_role_creation()
                logger.info(f'Role created: {role}')
    elif args.type in ('rs', 'redshift'):
        if args.subtype == 'cluster':
            if args.key == 'info':
                show_cluster_info()
            elif args.key == 'create':
                create_redshift_cluster(update=args.no_update)


def main():
    """Handle cli arguments."""
    parser = get_parser()

    # add autocomplete for arguments
    argcomplete.autocomplete(parser)

    args = parser.parse_args()

    if args.app == 'main':
        _handle_app(args)
    elif args.app == 'db':
        _handle_db(args)
    elif args.app == 'uploader':
        _handle_data_uploader(args)
    elif args.app == 'airflow':
        _handle_airflow(args)
    elif args.app == 'aws':
        _handle_AWS(args)


if __name__ == '__main__':
    main()
