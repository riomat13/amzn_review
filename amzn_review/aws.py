#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from collections import namedtuple
from functools import partialmethod
import json
import time

import boto3
import botocore
from botocore.exceptions import ClientError

from settings import Config, get_logger

logger = get_logger(__name__, level='DEBUG')


Role = namedtuple('Role', 'client,role,arn')


class AWS(object):
    """AWS client factory.

    Used variable set in configuration
    (Config object and/or `.env` file)

    Example usage:
        >>> client = AWS.get_client('s3')
    """

    _client = partialmethod(
        boto3.client,
        region_name=Config.AWS['REGION'],
        aws_access_key_id=Config.AWS['ACCESS_KEY_ID'],
        aws_secret_access_key=Config.AWS['SECRET_KEY']
    )

    @staticmethod
    def get_client(client_type: str,
                   *args, **kwargs):
        """Get AWS client.

        Args:
            client_type: str
                target client type
                (currently implemented only for `S3`, `iam` and `redshift`)

        Returns:
            client: boto3 client based on `client_type`
        """
        client_type = client_type.lower()
        if client_type not in ('s3', 'iam', 'redshift'):
            raise ValueError('Invalid client type. Must choose from ("s3", "iam", "redshift").')

        client = AWS._client(client_type)
        return client


def redshift_role_creation() -> None:
    print('Sorry, not implemented yet')


def show_cluster_info(props=None) -> None:
    """Print cluster information on stdout."""
    if props is None:
        client = AWS.get_client('redshift')
        try:
            props = client.describe_clusters(
                ClusterIdentifier=Config.AWS['REDSHIFT']['DB_CLUSTER_IDENTIFIER']
            )['Clusters'][0]
        except ClientError as err:
            if err.response['Error']['Code'] != 'ClusterNotFound':
                raise
            print('Cluster not found')
            return

    print('===================================')
    print('  Redshift Cluster Information')
    print('-----------------------------------')
    prop_keys = ['ClusterIdentifier', 'NodeType', 'ClusterStatus',
                 'MasterUsername', 'DBName',
                 'Endpoint', 'NumberOfNodes', 'VpcId']

    for key in prop_keys:
        # skip some keys if the cluster is not ready and lack the keys
        if key not in props:
            continue
        print(f'  {key+":":<18} {props[key]}')


def create_redshift_cluster(client=None,
                            update=True) -> None:
    """Create Redshift cluster via boto3.

    Args:
        client: botocore.client.Redshift
            client instantiated by `boto3.client('redshift')`
            if not provided, create one with env parameters
        update: bool (default: True)
            update connection on Airflow variable.
            if airflow is not associated with this, skip the operation.

    Returns:
        None
    """
    client = client or AWS.get_client('redshift')

    redshift_cfg = Config.AWS['REDSHIFT']
    num_nodes = redshift_cfg['NUMBER_OF_NODES']

    try:
        if num_nodes > 1:
            client.create_cluster(
                DBName=redshift_cfg['DB_NAME'],
                ClusterIdentifier=redshift_cfg['DB_CLUSTER_IDENTIFIER'],
                ClusterType='multi-node',
                NodeType=redshift_cfg['NODE_TYPE'],
                NumberOfNodes=num_nodes,
                MasterUsername=redshift_cfg['USERNAME'],
                MasterUserPassword=redshift_cfg['PASSWORD'],
                VpcSecurityGroupIds=Config.AWS['VPC_SECURITY_GROUP'],
                Port=redshift_cfg['PORT']
                #IamRoles=[Config.AWS['IAM_ROLE_ARN'],]
            )
        else:
            client.create_cluster(
                DBName=redshift_cfg['DB_NAME'],
                ClusterIdentifier=redshift_cfg['DB_CLUSTER_IDENTIFIER'],
                ClusterType='single-node',
                NodeType=redshift_cfg['NODE_TYPE'],
                MasterUsername=redshift_cfg['USERNAME'],
                MasterUserPassword=redshift_cfg['PASSWORD'],
                VpcSecurityGroupIds=Config.AWS['VPC_SECURITY_GROUP'],
                Port=redshift_cfg['PORT']
                #IamRoles=[Config.AWS['IAM_ROLE_ARN'],]
            )

        logger.info('Creating cluster...')

    except ClientError as err:
        if err.response['Error']['Code'] != 'ClusterAlreadyExists':
            raise

        # skip if cluster already exists
        return client.describe_clusters(
            ClusterIdentifier=redshift_cfg['DB_CLUSTER_IDENTIFIER']
        )['Clusters'][0]
    else:
        timeout = 300

        props = client.describe_clusters(
            ClusterIdentifier=redshift_cfg['DB_CLUSTER_IDENTIFIER']
        )['Clusters'][0]

        # wait for the cluster get ready
        print('Waiting for the cluster available...')

        while props['ClusterStatus'] != 'available' and timeout > 0:
            time.sleep(1.0)
            timeout -= 1
            props = client.describe_clusters(
                ClusterIdentifier=redshift_cfg['DB_CLUSTER_IDENTIFIER']
            )['Clusters'][0]

        if not timeout:
            logger.info('TIme out to wait for the cluster available.')
        else:
            logger.info('Cluster created!')

    show_cluster_info(props)

    if update:
        try:
            from workflow.conn import ConnectionConfig, update_connection
        except ImportError:
            logger.warning('Airflow related module is not associated with this')
        else:
            cfg = ConnectionConfig(
                conn_id=Config.AIRFLOW['REDSHIFT_CONN_ID'],
                host=props.get('Endpoint', {}).get('Address'),
                login=props.get('MasterUsername'),
                password=redshift_cfg['PASSWORD'],
                schema=props.get('DBName'),
                port=redshift_cfg['PORT']
            )
            update_connection(cfg)
