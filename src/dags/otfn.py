# -*- coding: utf-8 -*-

from __future__ import print_function

import airflow
import logging

from datetime import datetime, timedelta
from airflow import models
from airflow.settings import Session
from airflow.models import Variable

from acme.operators import MsSqlOperator, MsSqlDataImportOperator, MsSqlImportFromQueryOperator


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True
}

dag = airflow.DAG(
    'otfn',
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

t1 = MsSqlDataImportOperator(
    task_id='import_timesheet_data',
    table_name='timesheet',
    data_file=Variable.get('data_file_path') + '/timesheet.csv',
    mssql_conn_id='mssql_datalake',
    dag=dag
)

t2 = MsSqlImportFromQueryOperator(
    task_id='otfn',
    table='employee_otfn',
    src_conn_id='mssql_datalake',
    dest_conn_id='mssql_app',
    sql='otfn.sql',
    dag=dag
)

t0 >> t1 >> t2
