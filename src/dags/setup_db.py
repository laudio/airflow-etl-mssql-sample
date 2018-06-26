# -*- coding: utf-8 -*-

from __future__ import print_function
import airflow
import logging
from datetime import datetime, timedelta
from airflow import models
from airflow.settings import Session
from airflow.models import Variable
from acme.operators import MsSqlOperator, SqlcmdOperator, SqlcmdFilesOperator


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True
}

tmpl_search_path = Variable.get('sql_path')

dag = airflow.DAG(
    'setup_db',
    schedule_interval="@once",
    default_args=args,
    template_searchpath=tmpl_search_path,
    max_active_runs=1
)

# Teardown - drop all tables/schema if they exist
t0 = MsSqlOperator(
    task_id='otfn_teardown',
    sql='teardown.sql',
    mssql_conn_id='mssql_datalake',
    dag=dag
)

# Create timesheet table (Data Lake)
t1 = MsSqlOperator(
    task_id='create_timesheet_table',
    sql='create_timesheet_table.sql',
    mssql_conn_id='mssql_datalake',
    dag=dag
)

# Create otfn table (Application)
t2 = SqlcmdFilesOperator(
    task_id='create_otfn_table',
    sql='sql/create_otfn_table.sql',
    mssql_conn_id='mssql_app',
    dag=dag
)

# Execution Order
t0 >> t1 >> t2
