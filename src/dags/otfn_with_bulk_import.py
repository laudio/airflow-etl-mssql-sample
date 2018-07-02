# -*- coding: utf-8 -*-

from __future__ import print_function

import airflow
import logging

from datetime import datetime, timedelta
from airflow import models
from airflow.settings import Session
from airflow.models import Variable

from etl.operators import MsSqlOperator, MsSqlDataImportOperator, MsSqlImportFromQueryOperator, SqlcmdFilesOperator


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True
}

dag = airflow.DAG(
    'otfn_with_bulk_import',
    schedule_interval='@daily',
    default_args=args,
    template_searchpath=Variable.get('sql_path'),
    max_active_runs=1
)

t0 = MsSqlOperator(
    task_id='clear_timesheet_data',
    sql='DELETE FROM timesheet',
    mssql_conn_id='mssql_datalake',
    dag=dag
)

t1 = SqlcmdFilesOperator(
    task_id='import_timesheet_data',
    mssql_conn_id='mssql_datalake',
    sql='sql/bulk_import_file.sql',
    env={
        'data_file_path': Variable.get('file_path'),
        'format_file_path': Variable.get('format_file_path')
    },
    dag=dag
)

t2 = SqlcmdFilesOperator(
    task_id='otfn_with_link_servers',
    sql='sql/otfn_with_link_server.sql',
    env={'SERVER': 'ETL_SERVER', 'DATABASE': 'datalake'},
    mssql_conn_id='mssql_app',
    dag=dag
)

t0 >> t1 >> t2
