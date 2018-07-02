import os
from fabric.api import cd, get, run


def get_timesheet_file():
    with cd(os.environ['timesheet_dir']):
        get('timesheet.csv', '/tmp')
