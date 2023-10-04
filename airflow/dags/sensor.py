from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python_operator import BranchPythonOperator
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

increase_age_average_task = DummyOperator(
    task_id="increase_age_average",
    dag=user_age_trend_dag,
)

decrease_age_average_task = DummyOperator(
    task_id="decrease_age_average",
    dag=user_age_trend_dag,
)

# Set up the task dependencies
(
    file_sensor_task
    >> age_average_task
    >> [increase_age_average_task, decrease_age_average_task]
)
