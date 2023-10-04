import airflow
import datetime
import urllib.request as request
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

import json
import requests
from faker import Faker
import random

default_args_dict = {
    'start_date': datetime.datetime(2020, 6, 25, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dnd_dag = DAG(
    dag_id='dnd_dag',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)

def _generate_characters(output_folder: str, epoch: str):

	random.seed(epoch)
	file_name = output_folder + "/characters.json"
	character_data = []

	for i in range(5):
		name = Faker().name()

		race_data = requests.get('https://www.dnd5eapi.co/api/races').json()
		class_data = requests.get('https://www.dnd5eapi.co/api/classes').json()
		class_of = random.choice(class_data['results'])['index']

		character = {
			'name': name,
			'attributes': f"[{','.join([str(x) for x in [random.randint(6, 18), random.randint(2, 18), random.randint(2, 18), random.randint(2, 18), random.randint(2, 18), random.randint(2, 18)]])}]",
			'race': random.choice(race_data['results'])['index'],
			'languages': "french",
			'class': class_of,
			'level': str(random.randint(1, 3))
		}

		character_data.append(character)

		print(character_data[i])

	with open(file_name, 'w') as json_file:
		json.dump(character_data, json_file, indent=4)

	print(f'Data saved to {file_name}')

task_one = PythonOperator(
    task_id='generate_characters',
    dag=dnd_dag,
    python_callable=_generate_characters,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
        "epoch": "{{ execution_date.int_timestamp }}"
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

def _generate_spells(output_folder: str, epoch: str):

	random.seed(epoch)
	file_name = output_folder + "/characters.json"
	spells = []

	with open(file_name, 'r') as json_file:
		data = json.load(json_file)

	for character in data:
		class_data = requests.get('https://www.dnd5eapi.co/api/classes/' + character['class']).json()
		print(character)
		if "spells" in class_data:
			print("The 'spells' key exists in the JSON object.")
			spells_data = requests.get('https://www.dnd5eapi.co/api/classes/' + character['class'] + '/spells').json()
			number_spells = int(character['level']) + 3
			for i in range(number_spells):
				spells.append(random.choice(spells_data['results'])['index'])
				print(spells[i])
		else:
			print("The 'spells' key does not exist in the JSON object.")
		#character['spells'] = ["Fireball", "Magic Missile"]  # Add a "spells" attribute with a list of spells
		new_seed = random.randint(100000, 1000000000)
		random.seed(epoch + str(new_seed))

	#with open(file_name, 'w') as json_file:
		#json.dump(data, json_file, indent=4)

	print(f'Data saved to {file_name}')

task_two = PythonOperator(
    task_id='generate_spells',
    dag=dnd_dag,
    python_callable=_generate_spells,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
        "epoch": "{{ execution_date.int_timestamp }}"
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

def _create_character_query(output_folder: str):
	df = pd.read_json(output_folder+"/characters.json", orient = "records")
	with open("/opt/airflow/dags/character.sql", "w") as f:
		df_iterable = df.iterrows()
		f.write(
			"CREATE TABLE IF NOT EXISTS character (\n"
			"name VARCHAR(255),\n"
			"attributes VARCHAR(255),\n"
			"race VARCHAR(255),\n"
			"languages VARCHAR(255),\n"
			"class VARCHAR(255),\n"
			"level VARCHAR(255)\n"
			");\n"
		)

		for index, row in df_iterable:
			name = row['name']
			attributes = row['attributes']
			race = row['race']
			languages = row['languages']
			classs = row['class']
			level = row['level']
			
			f.write(
				"INSERT INTO character VALUES ("
				#",".join(row.dict().values())
				f"'{name}', '{attributes}', '{race}', '{languages}', '{classs}', '{level}'"
				");\n"
			)
		
		f.close()

task_three = PythonOperator(
    task_id='create_character_query',
    dag=dnd_dag,
    python_callable=_create_character_query,
    op_kwargs={
        'output_folder': '/opt/airflow/dags'
    },
    trigger_rule='all_success',
)

task_four = PostgresOperator(
    task_id='insert_character_query',
    dag=dnd_dag,
    postgres_conn_id='postgres_default',
    sql='character.sql',
    trigger_rule='all_success',
    autocommit=True
)

end = DummyOperator(
    task_id='end',
    dag=dnd_dag,
    trigger_rule='none_failed',
    depends_on_past=False
)

task_one >> task_two >> task_three >> task_four >> end