from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import datetime
import requests
import csv
import airflow

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
crm_generator_dag = DAG(
    dag_id="crm_generator",
    default_args=default_args,
    catchup=False,
)


# Function to fetch 5 users from the API and save them to a CSV file
def fetch_and_save_users():
    api_url = "https://randomuser.me/api/?results=5"
    response = requests.get(api_url)
    if response.status_code == 200:
        users = response.json()["results"]
        with open(BASE_FOLDER + "users.csv", "w", newline="") as csvfile:
            fieldnames = ["name", "email", "dob", "gender"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for user in users:
                writer.writerow(
                    {
                        "name": user["name"]["first"] + " " + user["name"]["last"],
                        "email": user["email"],
                        "dob": user["dob"]["date"],
                        "gender": user["gender"],
                    }
                )
    else:
        raise Exception("Failed to fetch user data from the API.")


# Define the task to fetch and save users
fetch_and_save_users_task = PythonOperator(
    task_id="fetch_and_save_users",
    python_callable=fetch_and_save_users,
    dag=crm_generator_dag,
)

end = DummyOperator(
    task_id="end",
    dag=crm_generator_dag,
    trigger_rule="none_failed",
)

# Set the order of tasks in the DAG
fetch_and_save_users_task >> end
