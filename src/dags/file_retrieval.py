import airflow
from airflow.models import Variable
from airflow.operators import SlackAPIPostOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash_operator import BashOperator


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True
}

dag = airflow.DAG(
    'file_retrieval',
    schedule_interval='@daily',
    default_args=args,
    template_searchpath=Variable.get('sql_path'),
    max_active_runs=1
)

t0 = BashOperator(
    bash_command='timesheet_dir={ftp_dir} /usr/local/airflow/.local/bin/fab -H {host} --user {username} -f /usr/local/airflow/scripts/fabfile.py get_timesheet_file'.format(
        ftp_dir=Variable.get('ftp_dir'),
        host=Variable.get('ftp_host_ip'),
        username=Variable.get('sftp_username')
    ),
    task_id='get_file',
    dag=dag
)

t1 = SlackAPIPostOperator(
    channel=Variable.get('slack_channel'),
    trigger_rule=TriggerRule.ALL_SUCCESS,
    token=Variable.get('slack_token'),
    username='airflow',
    text='Timesheet file retrieval action complete.',
    attachments=[
        {
            'text': 'Timesheet file retrieval successful.',
            'color': '#449944',
            'mrkdwn_in': ['text']
        }
    ],
    task_id='slack_success',
    dag=dag
)

t2 = SlackAPIPostOperator(
    channel=Variable.get('slack_channel'),
    trigger_rule=TriggerRule.ONE_FAILED,
    token=Variable.get('slack_token'),
    username='airflow',
    text='Timesheet file retrieval action failed.',
    attachments=[
        {
            'text': 'Timesheet file retrieval failed.',
            'color': '#CC4444',
            'mrkdwn_in': ['text']
        }
    ],
    task_id='slack_failure',
    dag=dag
)

t0 >> t1
t0 >> t2
