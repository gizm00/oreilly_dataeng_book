from airflow import get_current_context
from airflow import dag, task

import hiaas_tasks as tasks

@dag
def extract_species_service():

    @task
    def acquire_data():
        context = get_current_context()
        source_bucket = context["params"].get('bucket')
        data_location = tasks.acquire_data(source_bucket)
        return data_location

    @task
    def extract_species_basic(data_location):
        temp_location = tasks.extract_species(temp_location)
        return temp_location

    @task
    def extract_species_ml(data_location):
        temp_location = tasks.extract_species(temp_location)
        return temp_location

    @task
    def save_to_db(data_location):
        context = get_current_context()
        db = context["params"].get('db')
        cust_id = context["params"].get('cust_id')
        tasks.save_to_db(data_location, db, cust_id)

