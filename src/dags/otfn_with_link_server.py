# -*- coding: utf-8 -*-
import airflow
from airflow.models import Variable

from acme.operators import MsSqlOperator, MsSqlDataImportOperator, SqlcmdFilesOperator


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True
}

dag = airflow.DAG(
    'otfn_with_link_server',
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

t2 = SqlcmdFilesOperator(
    task_id='otfn_with_link_servers',
    sql='sql/otfn_with_link_server.sql',
    env={'SERVER': '64849c5b8900', 'DATABASE': 'datalake'},
    mssql_conn_id='mssql_app',
    dag=dag
)

t0 >> t1 >> t2
