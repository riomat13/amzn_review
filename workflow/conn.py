#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Handle connections in Airflow."""

from dataclasses import dataclass

from settings import get_logger, Config

logger = get_logger('workflow.conn')


@dataclass
class ConnectionConfig(object):
    conn_id: str
    host: str
    login: str
    password: str
    schema: str
    port: int


class AirflowConnection:
    def __init__(self):
        self.Connection = None
        self.create_session = None

    def _set_airflow_connection(self):
        from airflow.models import Connection
        from airflow.utils.db import create_session

        self.Connection = Connection
        self.create_session = create_session

    def add_connection(self, cfg: ConnectionConfig) -> None:
        """Create new connection to access from Airflow.
        
        Only create connections which "Conn Type" is "Postgres".
        """
        if not isinstance(cfg, ConnectionConfig):
            raise ValueError('Input must be `ConnectionConfig` instance')

        if self.Connection is None:
            self._set_airflow_connection()

        conn = self.Connection(conn_id=cfg.conn_id,
                               conn_type='postgres',
                               host=cfg.host,
                               login=cfg.login,
                               password=cfg.password,
                               schema=cfg.schema,
                               port=cfg.port)

        with self.create_session() as sess:
            if not (sess.query(self.Connection).filter(self.Connection.conn_id == cfg.conn_id).first()):
                sess.add(conn)
                logger.info(f'Created connection `conn_id`={cfg.conn_id}')
            else:
                logger.warning(f'Connection `conn_id`={cfg.conn_id} already exists')


    def update_connection(self, cfg: ConnectionConfig) -> None:
        """Update connection if exists, otherwise create new one."""
        if self.Connection is None:
            self._set_airflow_connection()

        with self.create_session() as sess:
            conn = sess.query(self.Connection) \
                    .filter(self.Connection.conn_id == cfg.conn_id) \
                    .first()

            if conn is not None:
                conn.host = cfg.host
                conn.login = cfg.login
                conn.password = cfg.password
                conn.schema = cfg.schema
                conn.port = cfg.port
                sess.add(conn)
                logger.info(f'Updated connection: "{cfg.conn_id}"')
            else:
                add_connection(cfg)


    def delete_connection(self, conn_id: str):
        pass


_Inst = AirflowConnection()
add_connection = _Inst.add_connection
update_connection = _Inst.update_connection
