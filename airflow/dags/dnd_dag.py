import json
import requests
from random import choice, randint, sample

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from faker import Faker

BASE_DIRECTORY = "/opt/airflow/dags"
NUMBER_OF_CHARACTERS = 5

# DAG default arguments
default_args = {
	'start_date': airflow.utils.dates.days_ago(0),
	'concurrency': 1,
	'retries': 0
}

# Generate the DAG
dnd_dag = DAG(
    dag_id='dnd_dag',
    default_args=default_args,
    catchup=False,
)

def gen_base_character() -> dict:
	"""
	Generate a random base character.
	"""
	
	# Load races and classes from API
	races = requests.get('https://www.dnd5eapi.co/api/races').json()['results']
	classes = requests.get('https://www.dnd5eapi.co/api/classes').json()['results']
	languages = requests.get('https://www.dnd5eapi.co/api/languages').json()['results']

	# Initialize faker
	fake = Faker()

	# Generate a character and return it
	return {
		'name': fake.name(),
		'attributes': str([
			randint(6, 18),
			randint(2, 18),
			randint(2, 18),
			randint(2, 18),
			randint(2, 18),
			randint(2, 18)
		]),
		"race": choice(races)['index'],
		"class": choice(classes)['index'],
		"languages": choice(languages)['index'],
		"level": str(randint(1, 3))
	}

def gen_base_characters(n: int) -> list:
	"""
	Generate a list of n base characters.
	"""

	# Generate n characters
	characters = [gen_base_character() for _ in range(n)]

	# Write characters to file
	with open(f'{BASE_DIRECTORY}/characters.json', 'w') as f:
		json.dump(characters, f, indent=4)


generate_base_task = PythonOperator(
    task_id='generate_characters',
    python_callable=gen_base_characters,
	op_kwargs={
		'n': NUMBER_OF_CHARACTERS
	},
    dag=dnd_dag,
)

def gen_spells(character: dict) -> dict:
	"""
	Generate spell for the given character and augment the character dict.
	"""

	# Load spells from API
	spells = requests.get(f'https://www.dnd5eapi.co/api/classes/{character["class"]}/spells').json()['results']

	# Generate a spell and return it
	number_of_spells = int(character['level']) + 3
	selected_spells = []

	if len(spells) > number_of_spells:
		selected_spells = sample(spells, number_of_spells)
	else:
		selected_spells = spells

	# Add spells to character
	character['spells'] = str([spell['index'] for spell in selected_spells]).replace("'", "")

	return character

def gen_proficiencies(character: dict) -> dict:
	"""
	Generate proficiencies for the given character and augment the character dict.
	"""

	# Load proficiencies from API
	proficiency_choices = requests.get(f'https://www.dnd5eapi.co/api/classes/{character["class"]}/').json()['proficiency_choices'][0]

	number_of_proficiencies = proficiency_choices['choose']
	proficiencies = sample(proficiency_choices['from']['options'], number_of_proficiencies)
	proficiencies = [proficiency['item']['index'] for proficiency in proficiencies]

	# Add proficiencies to character
	character['proficiencies'] = str(proficiencies).replace("'", "")

	return character

def gen_spells_for_characters() -> None:
	"""
	Load characters from json file, generate spells for each character and write the augmented characters back to the file.
	"""

	# Load characters from file
	with open(f'{BASE_DIRECTORY}/characters.json', 'r') as f:
		characters = json.load(f)

	# Generate spells for each character
	characters = [gen_spells(character) for character in characters]
	print(characters)

	# Write characters to file
	with open(f'{BASE_DIRECTORY}/characters.json', 'w') as f:
		json.dump(characters, f, indent=4)

def gen_proficiencies_for_characters() -> None:
	"""
	Load characters from json file, generate proficiencies for each character and write the augmented characters back to the file.
	"""

	# Load characters from file
	with open(f'{BASE_DIRECTORY}/characters.json', 'r') as f:
		characters = json.load(f)

	# Generate spells for each character
	characters = [gen_proficiencies(character) for character in characters]
	print(characters)

	# Write characters to file
	with open(f'{BASE_DIRECTORY}/characters.json', 'w') as f:
		json.dump(characters, f, indent=4)

generate_spells_task = PythonOperator(
	task_id='generate_spells',
	python_callable=gen_spells_for_characters,
	dag=dnd_dag,
)

generate_proficiencies_task = PythonOperator(
	task_id='generate_proficiencies',
	python_callable=gen_proficiencies_for_characters,
	dag=dnd_dag,
)

def insert_characters_into_db() -> None:
	"""
	Load characters from json file and insert them into the database.
	"""

	# Load characters from file
	with open(f'{BASE_DIRECTORY}/characters.json', 'r') as f:
		characters = json.load(f)

	with open(f'{BASE_DIRECTORY}/characters.sql', 'w') as f:

		# Create table
		f.write("""
		  	DROP TABLE IF EXISTS character;
			CREATE TABLE character (
				name VARCHAR(255),
				attributes VARCHAR(255),
				race VARCHAR(255),
				languages VARCHAR(255),
				class VARCHAR(255),
		  		spells VARCHAR(255),
		  		proficiencies VARCHAR(255),
				level VARCHAR(255)
		  	);
		""")

		# Insert characters into database
		for character in characters:
			f.write(f"""
				INSERT INTO character VALUES (
					'{character['name']}',
					'{character['attributes']}',
					'{character['race']}',
					'{character['languages']}',
					'{character['class']}',
					'{character['spells']}',
					'{character['proficiencies']}',
					'{character['level']}'
				);	
		   """)
			
		# Close and save file
		f.close()

generate_sql_task = PythonOperator(
	task_id='insert_characters',
	python_callable=insert_characters_into_db,
	dag=dnd_dag,
)

insert_characters_into_db_task = PostgresOperator(
    task_id='insert_character_query',
    dag=dnd_dag,
    postgres_conn_id='postgres_default',
    sql='characters.sql',
    trigger_rule='all_success',
    autocommit=True
)

end = DummyOperator(
    task_id='end',
    dag=dnd_dag,
    trigger_rule='none_failed',
)

generate_base_task >> generate_spells_task >> generate_proficiencies_task >> generate_sql_task >> insert_characters_into_db_task >> end