# airflow-etl-mssql-sample

Sample Project with Airflow with MS SQL.

Uses this [laudio/airflow-mssql](https://github.com/laudio/docker-airflow-mssql) docker image for airflow.

## Setup and Configuration

#### Configure
Create a new configuration file `airflow.cfg`.
```bash
 $ cp src/config/airflow.cfg.example src/config/airflow.cfg
```

#### Setup First User
Run it first.
```
$ docker-compose up
```

Now in a separate terminal run `python` in the running airflow container.
```
$ docker-compose exec airflow python
```

Run the following in the prompt to create a new user for airflow.
```bash
Python 3.6.5 (default, Jun  6 2018, 02:51:26)
[GCC 6.3.0 20170516] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> import airflow
>>> from airflow import models, settings
>>> from airflow.contrib.auth.backends.password_auth import PasswordUser
>>> user = PasswordUser(models.User())
>>> user.username = 'username'
>>> user.email = 'user@example.com'
>>> user._set_password = 'password'
>>> session = settings.Session()
>>> session.add(user)
>>> session.commit()
>>> session.close()
>>> exit()
```

#### Login to the UI
Now that you've setup a user you can go to the airflow UI http://0.0.0.0:8081 and login.

## Usage

Spin up the containers.

```bash
$ docker-compose up
```


## Airflow Variables

* `HOST_IP`: IP address of the SQL Server host

## Airflow Connections

* MSSQL Datalake connection:
  * `conn_id`: "mssql_datalake",
  * `conn_type`: "MS SQL Server",
  * `host`: `HOST_IP`,
  * `port`: 1433,
  * `schema`: "datalake",
  * `login`: "sa",
  * `password`: "Th1sS3cret!"

* MSSQL App connection:
  * `conn_id`: "mssql_app",
  * `conn_type`: "MS SQL Server",
  * `host`: `HOST_IP`,
  * `port`: 1433,
  * `schema`: "app",
  * `login`: "sa",
  * `password`: "Th1sS3cret!"

## Environment Variables

* `AIRFLOW_DB_DATA_PATH` - Path to store airflow's db (postgres) data.
* `AIRFLOW_DB` - Airflow database name.
* `AIRFLOW_DB_USER` - Airflow database user.
* `AIRFLOW_DB_PASSWORD` - Airflow database Password.
* `AIRFLOW_DB_EXT_PORT` - Airflow database exported (external) port.
* `FERNET_KEY` / `AIRFLOW__CORE__FERNET_KEY` - Fernet key used for Airflow cryptography and encryption.
* `AIRFLOW_WEB_EXT_PORT` - Airflow web application (UI) exposed external port.
