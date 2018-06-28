# airflow-etl-mssql-sample

Sample Project with Airflow with MS SQL.

## Usage
Spin up the containers.
```bash
$ ./run.sh
```

## Environment Variables

 * `AIRFLOW_DB_DATA_PATH` - Path to store airflow's db (postgres) data.
 * `AIRFLOW_DB` - Airflow database name.
 * `AIRFLOW_DB_USER` - Airflow database user.
 * `AIRFLOW_DB_PASSWORD` - Airflow database Password.
 * `AIRFLOW_DB_EXT_PORT` - Airflow database exported (external) port.
 * `FERNET_KEY` / `AIRFLOW__CORE__FERNET_KEY` - Fernet key used for Airflow cryptography and encryption.
 * `AIRFLOW_WEB_EXT_PORT` - Airflow web application (UI) exposed external port.


