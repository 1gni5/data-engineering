import requests
import json
import csv
import math
from collections import namedtuple
import airflow
import datetime
from airflow import DAG
from dataclasses import dataclass
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from sys import path

BASE_FOLDER = "/opt/airflow/dags/"


def distance(alat, alon, blat, blon) -> float:
    """
    Calculate the distance between two points on the Earth's surface
    """

    # Radius of the Earth in kilometers
    R = 6371.0

    # Convert latitudes and longitudes from degrees to radians
    slat = math.radians(alat)
    slon = math.radians(alon)
    olat = math.radians(blat)
    olon = math.radians(blon)

    # Haversine formula
    dlon = olon - slon
    dlat = olat - slat
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(slat) * math.cos(olat) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c

    return distance


# Define default_args as a dictionary
default_args_dict = {
    "start_date": airflow.utils.dates.days_ago(0),
    "concurrency": 1,
    "schedule_interval": None,
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=1),
}

# Create a DAG object
iss_dag = DAG(
    dag_id="iss_dag",
    default_args=default_args_dict,
    catchup=False,
)


def fetch_iss_location():
    # Fetch ISS location data
    iss_url = "http://api.open-notify.org/iss-now.json"
    response = requests.get(iss_url)

    if response.status_code == 200:
        iss_data = response.json()

        # Save the data to a file
        with open(BASE_FOLDER + "iss_location.json", "w") as file:
            json.dump(iss_data, file)
    else:
        print("Failed to fetch ISS location data.")

    return response.status_code


def find_closest_country():
    # Read country data from the CSV file using DictReader
    countries = []
    with open(
        BASE_FOLDER + "data/countries.csv", mode="r", encoding="utf-8-sig"
    ) as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            countries.append(row)

    # Load the ISS position data
    with open(BASE_FOLDER + "iss_location.json", "r") as file:
        iss_data = json.load(file)

    # Extract ISS position from JSON
    iss_lat, iss_lon = (
        iss_data["iss_position"]["latitude"],
        iss_data["iss_position"]["longitude"],
    )

    # Find the closest country
    closest_country = None
    closest_distance = float("inf")

    for country in countries:
        current_distance = distance(
            float(iss_lat),
            float(iss_lon),
            float(country["latitude"]),
            float(country["longitude"]),
        )
        if current_distance < closest_distance:
            closest_country = country
            closest_distance = current_distance

    # Write the result to a file
    with open(BASE_FOLDER + "closest_country.json", "w") as file:
        json.dump(closest_country, file)


fetch_iss_location_task = PythonOperator(
    task_id="fetch_iss_location",
    python_callable=fetch_iss_location,
    dag=iss_dag,
)

find_closest_country_task = PythonOperator(
    task_id="find_closest_country",
    python_callable=find_closest_country,
    dag=iss_dag,
)

def _create_country_query():
    with open(f'{BASE_FOLDER}/closest_country.json', 'r') as f:
        closest_country = json.load(f)
        with open("/opt/airflow/dags/country.sql", "w") as f:
            f.write(
                "CREATE TABLE IF NOT EXISTS country (\n"
                "name VARCHAR(255),\n"
                "continent VARCHAR(255),\n"
                "latitude VARCHAR(255),\n"
                "longitude VARCHAR(255)\n"
                ");\n"
            )

            name = closest_country['name']
            continent = closest_country['continent']
            latitude = closest_country['latitude']
            longitude = closest_country['longitude']
            
            f.write(
                "INSERT INTO country VALUES ("
                #",".join(row.dict().values())
                f"'{name}', '{continent}', '{latitude}', '{longitude}'"
                ");\n"
            )
            
            f.close()

create_country_query_task = PythonOperator(
    task_id='create_country_query',
    dag=iss_dag,
    python_callable=_create_country_query,
    trigger_rule='all_success',
)

insert_country_query = PostgresOperator(
    task_id='insert_country_query',
    dag=iss_dag,
    postgres_conn_id='postgres_default',
    sql='country.sql',
    trigger_rule='all_success',
    autocommit=True
)

end = DummyOperator(
    task_id="end",
    dag=iss_dag,
    trigger_rule="none_failed",
)

fetch_iss_location_task >> find_closest_country_task >> create_country_query_task >> insert_country_query >> end
