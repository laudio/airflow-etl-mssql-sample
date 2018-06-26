# -*- coding: utf-8 -*-
import os
import pyodbc
import logging
import subprocess

from airflow.hooks.dbapi_hook import DbApiHook


class BcpHook(DbApiHook):
    '''
    Interact with Microsoft SQL Server through bcp
    '''

    conn_name_attr = 'mssql_conn_id'
    default_conn_name = 'mssql_default'
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super(BcpHook, self).__init__(*args, **kwargs)
        self.schema = kwargs.pop('schema', None)

    def get_conn(self):
        '''
        Returns a mssql connection details object
        '''
        return self.get_connection(self.mssql_conn_id)

    def run_bcp(self, cmd):
        logging.info("Running command: {0}".format(cmd))
        proc = subprocess.Popen(
           ' '.join(cmd), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env={**os.environ}
        )
        outs, errs = proc.communicate()
        logging.info("Output:")
        print(outs)
        logging.info("Stderr:")
        print(errs)
        if proc.returncode != 0:
            raise Exception("Process failed: {0}".format(proc.returncode))

    def add_conn_details(self, cmd, conn):
        conn_params = ['-S', conn.host, '-U', conn.login,
                       '-P', conn.password, '-d', conn.schema]
        cmd.extend(conn_params)

    def generate_format_file(self, table_name, format_file):
        # Generate format file first:
        conn = self.get_conn()
        cmd = ['/opt/mssql-tools/bin/bcp', table_name, 'format', 'nul',
               '-c', '-f', format_file.name, '-t,']
        self.add_conn_details(cmd, conn)
        self.run_bcp(cmd)

    def import_data(self, format_file, data_file, table_name):
        # Generate format file first:
        conn = self.get_conn()
        cmd = ['/opt/mssql-tools/bin/bcp', table_name, 'in', data_file, '-f', format_file]
        self.add_conn_details(cmd, conn)
        self.run_bcp(cmd)


class MsSqlHook(DbApiHook):
    """
    Interact with Microsoft SQL Server.
    """

    conn_name_attr = 'mssql_conn_id'
    default_conn_name = 'mssql_default'
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super(MsSqlHook, self).__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)
        self.conn = None

    def get_conn(self):
        """
        Returns a mssql connection object
        """
        if self.conn:
            return self.conn

        conn = self.get_connection(self.mssql_conn_id)
        conn_str = "DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={0};PORT={1};DATABASE={2};UID={3};PWD={4}".format(
            conn.host, conn.port, conn.schema, conn.login, conn.password)
        self.conn = pyodbc.connect(conn_str)
        return self.conn

    def set_autocommit(self, conn, autocommit):
        conn.autocommit = autocommit
