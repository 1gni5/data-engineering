from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.postgres_operator import PostgresOperator
import pandas as pd
import datetime
import requests
import csv
import airflow

from datetime import date

BASE_FOLDER = "/opt/airflow/dags/"

# Define the default arguments for the DAG
default_args = {
    "start_date": airflow.utils.dates.days_ago(0),
    "concurrency": 1,
    "schedule_interval": None,
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=1),
}

# Create the DAG object
user_age_trend_dag = DAG(
    dag_id="user_age_trend",
    default_args=default_args,
    catchup=False,
)


def compute_age_average():
    # Load previous age average + catch errors
    try:
        with open(BASE_FOLDER + "age_average.txt", "r") as file:
            age_average = float(file.read())
    except:
        age_average = 0

    # Load the users CSV file
    with open(BASE_FOLDER + "users.csv", "r") as file:
        reader = csv.DictReader(file)
        users = list(reader)

    # Compute age from dob
    def compute_age(dob):
        today = date.today()
        return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))

    # Compute the average age
    new_age_average = sum(
        [
            compute_age(
                datetime.datetime.strptime(user["dob"], "%Y-%m-%dT%H:%M:%S.%fZ")
            )
            for user in users
        ]
    ) / len(users)

    # Write the new age average
    with open(BASE_FOLDER + "age_average.txt", "w") as file:
        file.write(str(new_age_average))

    return (
        "increase_age_average"
        if new_age_average > age_average
        else "decrease_age_average"
    )


age_average_task = BranchPythonOperator(
    task_id="age_average",
    python_callable=compute_age_average,
    dag=user_age_trend_dag,
)

# Define a sensor to detect the CSV file's creation
file_sensor_task = FileSensor(
    start_date=airflow.utils.dates.days_ago(0),
    task_id="file_sensor",
    filepath=BASE_FOLDER + "users.csv",
    poke_interval=10,
)

def _create_user_query():
    df = pd.read_csv(f'{BASE_FOLDER}users.csv')
    with open("/opt/airflow/dags/user.sql", "w") as f:
        df_iterable = df.iterrows()
        f.write(
            "CREATE TABLE IF NOT EXISTS user_csv (\n"
            "name VARCHAR(255),\n"
            "email VARCHAR(255),\n"
            "dob VARCHAR(255),\n"
            "gender VARCHAR(255)\n"
            ");\n"
        )
        for index, row in df_iterable:
            name = row['name']
            email = row['email']
            dob = row['dob']
            gender = row['gender']
            f.write(
                "INSERT INTO user_csv VALUES ("
                f"'{name}', '{email}', '{dob}', '{gender}'"
                ");\n"
            )

        f.close()


create_user_query_task = PythonOperator(
    task_id='create_user_query',
    dag=user_age_trend_dag,
    python_callable=_create_user_query,
    trigger_rule='all_success',
)

increase_age_average_task = DummyOperator(
    task_id="increase_age_average",
    dag=user_age_trend_dag,
)

decrease_age_average_task = DummyOperator(
    task_id="decrease_age_average",
    dag=user_age_trend_dag,
)

insert_character_query_task = PostgresOperator(
    task_id='insert_character_query',
    dag=user_age_trend_dag,
    postgres_conn_id='postgres_default',
    sql='user.sql',
    trigger_rule='all_success',
    autocommit=True
)

join_tasks = DummyOperator(
    task_id='coalesce_transformations',
    dag=user_age_trend_dag,
    trigger_rule='none_failed'
)

end = DummyOperator(
    task_id='end',
    dag=user_age_trend_dag,
    trigger_rule='none_failed'
)

# Set up the task dependencies

file_sensor_task >> [create_user_query_task,age_average_task]
create_user_query_task >> insert_character_query_task
age_average_task >> [increase_age_average_task, decrease_age_average_task]
[increase_age_average_task, decrease_age_average_task,insert_character_query_task] >> join_tasks >> end

