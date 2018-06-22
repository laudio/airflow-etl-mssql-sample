# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from datetime import date
from acme.hooks.bcp_hook import BcpHook
from acme.hooks.mssql_hook import MsSqlHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import tempfile
import csv


class MsSqlImportOperator(BaseOperator):
    """
    Imports synthethic data into a table on MSSQL
    """
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            table_name,
            generate_synth_data,
            mssql_conn_id='mssql_default',
            *args, **kwargs):
        super(MsSqlImportOperator, self).__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.table_name = table_name
        self.generate_synth_data = generate_synth_data

    def get_column_list(self, format_file_name):
        col_list = []
        with open(format_file_name, 'r') as format_file:
            version = format_file.readline()
            num_cols = int(format_file.readline())
            for i in range(0, num_cols):
                new_col = format_file.readline()
                row = new_col.split(" ")
                row = [x for x in row if x is not '']
                col_list.append(row[6])
        return col_list

    def execute(self, context):
        hook = BcpHook(mssql_conn_id=self.mssql_conn_id)
        with tempfile.NamedTemporaryFile(prefix='format', delete=False) as format_file:
            logging.info('Generating format file')
            hook.generate_format_file(self.table_name, format_file)
            logging.info('Retrieving column list')
            col_list = self.get_column_list(format_file.name)
            logging.info(
                'Generating synthetic data using column list: {0}'.format(col_list))
            data = self.generate_synth_data(col_list)
            with tempfile.NamedTemporaryFile(prefix='data', mode='wb', delete=False) as data_file:
                logging.info('Writing data to temp file')
                csv_writer = csv.writer(data_file, delimiter=',')
                csv_writer.writerows(data)
                data_file.flush()
                logging.info('Importing data to SQL server')
                hook.import_data(format_file.name,
                                 data_file.name, self.table_name)


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
        # self.generate_synth_data = generate_synth_data

    # def get_column_list(self, format_file_name):
    #     col_list = []
    #     with open(format_file_name, 'r') as format_file:
    #         version = format_file.readline()
    #         num_cols = int(format_file.readline())
    #         for i in range(0, num_cols):
    #             new_col = format_file.readline()
    #             row = new_col.split(" ")
    #             row = [x for x in row if x is not '']
    #             col_list.append(row[6])
    #     return col_list

    def execute(self, context):
        hook = BcpHook(mssql_conn_id=self.mssql_conn_id)
        with tempfile.NamedTemporaryFile(prefix='format', delete=False) as format_file:
            logging.info('Generating format file')
            hook.generate_format_file(self.table_name, format_file)
            # logging.info('Retrieving column list')
            # col_list = self.get_column_list(format_file.name)
            # logging.info('Generating synthetic data using column list: {0}'.format(col_list))
            # data = self.generate_synth_data(col_list)

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
