# airflow-etl-mssql-sample

Sample Project with Airflow with MS SQL.

## Usage

Spin up the containers.

```bash
# Run this command in airflow-etl-mssql-sample directory
$ ./run.sh
```

## Airflow Variables

* `HOST_IP`: IP address of the SQL Server host

## Airflow Connections

* MSSQL Datalake connection:
  `conn_id`: "mssql_datalake",
  `conn_type`: "MS SQL Server",
  `host`: `HOST_IP`,
  `port`: 1433,
  `schema`: "datalake",
  `login`: "sa",
  `password`: "Th1sS3cret!"

* MSSQL App connection:
  `conn_id`: "mssql_app",
  `conn_type`: "MS SQL Server",
  `host`: `HOST_IP`,
  `port`: 1433,
  `schema`: "app",
  `login`: "sa",
  `password`: "Th1sS3cret!"

## Environment Variables

* `AIRFLOW_DB_DATA_PATH` - Path to store airflow's db (postgres) data.
* `AIRFLOW_DB` - Airflow database name.
* `AIRFLOW_DB_USER` - Airflow database user.
* `AIRFLOW_DB_PASSWORD` - Airflow database Password.
* `AIRFLOW_DB_EXT_PORT` - Airflow database exported (external) port.
* `FERNET_KEY` / `AIRFLOW__CORE__FERNET_KEY` - Fernet key used for Airflow cryptography and encryption.
* `AIRFLOW_WEB_EXT_PORT` - Airflow web application (UI) exposed external port.
