#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse

from settings import Config


def _main_app_parser(subparser):
    app_parser = subparser.add_parser(
        'main',
        help='Main application'
    )
    app_sub = app_parser.add_subparsers(dest='type')

    app_config = app_sub.add_parser(
        'config',
        help='App configuration'
    )
    app_config_sub = app_config.add_subparsers(dest='subtype')
    app_config_sub.add_parser('show', help='show configurations')

    # ----------------------------
    #   Database (local)
    # ----------------------------
    database = subparser.add_parser(
        'db',
        help='Database for main app'
    )
    database.add_argument('initdb', action='store_true',
                          help='initialize application database')
    database.add_argument('--local', action='store_true',
                          help='database for local')
    database.add_argument('resetdb', action='store_true',
                          help='reset application database')

    # ----------------------------
    #   Data uploader (AWS S3)
    # ----------------------------
    uploader = subparser.add_parser(
        'uploader',
        help='Upoad file to server'
    )
    uploader_sub = uploader.add_subparsers(dest='type')
    uploader_run = uploader_sub.add_parser(
        'run',
        help='start upload server'
    )
    uploader_run.add_argument('--local', action='store_true',
                              help='run for local machine')
    # TODO: can be indicated the default directory
    #uploader_run.add_argument('-d', '--data', type=str,
    #                          default=Config.DATA_DIR,
    #                          help='data directory path')
    # TODO: add choice for categories
    #uploader_run.add_argument('-c', '--category', type=str,
    #                          default=Config.DATA_CATEGORIES,
    #                          help='set categories to upload(comma separated)')


def _airflow_parser(subparser):
    airflow_parser = subparser.add_parser(
        'airflow',
        help='Airflow configuration'
    )

    airflow_sub = airflow_parser.add_subparsers(dest='type')

    af_webserver = airflow_sub.add_parser(
        'webserver',
        help='run airflow webserver'
    )
    af_webserver.add_argument('action', type=str, choices=['start', 'stop'],
                              help='start/stop airflow webserver')

    af_scheduler = airflow_sub.add_parser(
        'scheduler',
        help='run airflow scheduler'
    )
    af_scheduler.add_argument('action', type=str, choices=['start', 'stop'],
                              help='start/stop airflow scheduler')


def _aws_parser(subparser):

    def add_aws_key_args(parser):
        """Add key arguments to parser."""
        parser.add_argument('--aws-access-key', type=str,
                            help='set AWS access key ID')
        parser.add_argument('--aws-secret-key', type=str,
                            help='set AWS secret key')

    aws_parser = subparser.add_parser(
        'aws',
        help='AWS configuration'
    )
    aws_sub = aws_parser.add_subparsers(dest='type')

    # ----------------------------
    #   IAM
    # ----------------------------
    iam = aws_sub.add_parser(
        'iam',
        help='IAM handler',
    )
    subiam = iam.add_subparsers(dest='subtype')
    iam_role = subiam.add_parser(
        'role',
        help='IAM role',
    )
    iam_role.add_argument('--create', action='store_true',
                          help='create new IAM role')

    # ----------------------------
    #   Redshift
    # ----------------------------
    redshift = aws_sub.add_parser(
        'redshift',
        help='Redshift handler',
        aliases=['rs']
    )
    redshift_sub = redshift.add_subparsers(dest='subtype')

    rs_cluster = redshift_sub.add_parser(
        'cluster',
        help='Redshift cluster setup'
    )

    rs_cluster_sub = rs_cluster.add_subparsers(dest='key')

    # print cluster information
    rs_cluster_info = rs_cluster_sub.add_parser(
        'info',
        help='Redshift cluster information'
    )
    add_aws_key_args(rs_cluster_info)

    # cluster creation
    rs_cluster_creation = rs_cluster_sub.add_parser(
        'create',
         help='create Redshift cluster'
    )
    add_aws_key_args(rs_cluster_creation)

    rs_cluster_creation.add_argument(
        '--no-update', action='store_false',
         help='do not create/update connection in airflow'
    )
    # TODO: add cluster configuration arguments
    #rs_cluster_creation.add_argument('--cluster-name')
    #rs_cluster_creation.add_argument('-U', '--user')
    #rs_cluster_creation.add_argument('-W', '--password')
    #rs_cluster_creation.add_argument('-d', '--dbname')
    #rs_cluster_creation.add_argument('-n', '--num-cluster')


def get_parser():
    parser = argparse.ArgumentParser(prog='amzn_review', allow_abbrev=False)
    subparser = parser.add_subparsers(dest='app')
    subparser.required = True

    _main_app_parser(subparser)
    _airflow_parser(subparser)
    _aws_parser(subparser)

    return parser
