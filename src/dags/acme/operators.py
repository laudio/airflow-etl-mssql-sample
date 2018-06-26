# -*- coding: utf-8 -*-

import csv
import logging
import tempfile


from datetime import date
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from acme.hooks import MsSqlHook, BcpHook, SqlcmdHook


class MsSqlDataImportOperator(BaseOperator):
    """
    Imports synthethic data into a table on MSSQL
    """
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            table_name,
            data_file,
            mssql_conn_id='mssql_default',
            *args, **kwargs):
        super(MsSqlDataImportOperator, self).__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.table_name = table_name
        self.data_file = data_file

    def execute(self, context):
        hook = BcpHook(mssql_conn_id=self.mssql_conn_id)
        with tempfile.NamedTemporaryFile(prefix='format', delete=False) as format_file:
            logging.info('Generating format file')
            hook.generate_format_file(self.table_name, format_file)

            logging.info(
                'Importing data file {} to SQL server'.format(self.data_file))
            hook.import_data(format_file.name, self.data_file, self.table_name)


class MsSqlImportFromQueryOperator(BaseOperator):
    '''
    Executes sql code in a MS SQL database and inserts into another
    '''

    template_fields = (
        'sql', 'parameters', 'table',
        'pre_operator', 'post_operator'
    )
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            sql,
            table,
            src_conn_id='mssql_default',
            dest_conn_id='mssql_default',
            pre_operator=None,
            post_operator=None,
            parameters=None,
            *args, **kwargs):
        super(MsSqlImportFromQueryOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.table = table
        self.src_conn_id = src_conn_id
        self.dest_conn_id = dest_conn_id
        self.pre_operator = pre_operator
        self.post_operator = post_operator
        self.parameters = parameters

    def execute(self, context):
        logging.info('Executing: ' + str(self.sql))
        src_db = MsSqlHook(mssql_conn_id=self.src_conn_id)
        dest_db = MsSqlHook(mssql_conn_id=self.dest_conn_id)

        logging.info("Transferring query results into target table.")
        conn = src_db.get_conn()
        cursor = conn.cursor()

        cursor.execute(self.sql)

        values = ["('{}', '{}', '{}')".format(x[0], x[1], x[2])
                  for x in cursor.fetchall()]

        insert_sql = 'INSERT INTO employee_otfn (employee_id, date, is_on_the_floor) VALUES ' + ', '.join(
            values)
        c = dest_db.get_conn().cursor()
        c.execute('DELETE FROM employee_otfn')
        logging.info('INSERT SQL: ' + insert_sql)
        c.execute(insert_sql)
        c.commit()
        logging.info("Done.")


class MsSqlOperator(BaseOperator):
    """
    Executes sql code in a specific Microsoft SQL database
    :param mssql_conn_id: reference to a specific mssql database
    :type mssql_conn_id: string
    :param sql: the sql code to be executed
    :type sql: string or string pointing to a template file with .sql extension
    :param database: name of database which overwrite defined one in connection
    :type database: string
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, sql, mssql_conn_id='mssql_default', parameters=None,
            autocommit=False, database=None, *args, **kwargs):
        super(MsSqlOperator, self).__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.sql = sql
        self.parameters = parameters
        self.autocommit = autocommit
        self.database = database

    def execute(self, context):
        logging.info('Executing: %s', self.sql)
        hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id,
                         schema=self.database)
        hook.run(self.sql, autocommit=self.autocommit,
                 parameters=self.parameters)



class SqlcmdOperator(BaseOperator):
    """
    Executes sql code in a specific Microsoft SQL database using sqlcmd tool
    :param mssql_conn_id: reference to a specific mssql database
    :type mssql_conn_id: string
    :param sql: the sql code to be executed
    :type sql: string (SQL query)
    :param database: name of database which overwrite defined one in connection
    :type database: string
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, sql, mssql_conn_id='mssql_default', parameters=None,
            autocommit=False, database=None, *args, **kwargs):
        super(SqlcmdOperator, self).__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.sql = sql
        self.parameters = parameters
        self.autocommit = autocommit
        self.database = database

    def execute(self, context):
        logging.info('Executing: {}'.format(self.sql))
        hook = SqlcmdHook(mssql_conn_id=self.mssql_conn_id,
                         schema=self.database)

        hook.exec_sql_query(self.sql)


class SqlcmdFilesOperator(BaseOperator):
    """
    Executes sql code in a specific Microsoft SQL database using sqlcmd tool
    :param mssql_conn_id: reference to a specific mssql database
    :type mssql_conn_id: string
    :param sql: the sql files to be executed
    :type sql: string representing path to sql file or their list
    :param database: name of database which overwrite defined one in connection
    :type database: string
    """

    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, sql, mssql_conn_id='mssql_default', parameters=None,
            autocommit=False, database=None, *args, **kwargs):
        super(SqlcmdFilesOperator, self).__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.sql = sql
        self.parameters = parameters
        self.autocommit = autocommit
        self.database = database

    def execute(self, context):
        logging.info('Executing: {}'.format(str(self.sql)))
        hook = SqlcmdHook(mssql_conn_id=self.mssql_conn_id,
                         schema=self.database)

        hook.exec_sql_files(self.sql)
