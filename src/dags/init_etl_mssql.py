# -*- coding: utf-8 -*-

from __future__ import print_function
import os
import airflow
import logging

from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow import models
from airflow.settings import Session
from etl.operators import MsSqlOperator


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True
}


def initialize_etl():
    logging.info('Creating connections, pool and sql path')

    session = Session()

    def create_new_conn(session, attributes):
        new_conn = models.Connection()
        new_conn.conn_id = attributes.get("conn_id")
        new_conn.conn_type = attributes.get('conn_type')
        new_conn.host = attributes.get('host')
        new_conn.port = attributes.get('port')
        new_conn.schema = attributes.get('schema')
        new_conn.login = attributes.get('login')
        new_conn.set_password(attributes.get('password'))

        session.add(new_conn)
        session.commit()

    new_var = models.Variable()
    new_var.key = "sql_path"
    new_var.set_val("/usr/local/airflow/sql")
    session.add(new_var)
    session.commit()

    new_var = models.Variable()
    new_var.key = "data_file_path"
    new_var.set_val("/usr/local/airflow/data")
    session.add(new_var)
    session.commit()

    session.close()


dag = airflow.DAG(
    'init_etl_mssql',
    schedule_interval="@once",
    default_args=args,
    max_active_runs=1
)

initialize = PythonOperator(
    task_id='initialize_etl_mssql',
    python_callable=initialize_etl,
    provide_context=False,
    dag=dag
)
