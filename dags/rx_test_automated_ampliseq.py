from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from os.path import join
import textwrap
import pendulum
import csv
import os

# Constants for file paths
BASE_PATH = "/home/ruicatxiao/mbio_af_branch/local_testing_data_config"
PROVENANCE_PATH = f"{BASE_PATH}/processed_studies_provenance.csv"
ALL_STUDIES_PATH = f"{BASE_PATH}/amplicon_studies.csv"

def load_processed_studies():
    """Load processed studies from a CSV file into a dictionary."""
    processed_studies = {}
    if os.path.exists(PROVENANCE_PATH):
        with open(PROVENANCE_PATH, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                processed_studies[row['study']] = {'timestamp': row['timestamp'], 'code_revision': row['code_revision']}
    return processed_studies

def create_dag():
    default_args = {
        'start_date': pendulum.datetime(2021, 1, 1, tz="UTC")
    }

    with DAG(
        dag_id="rx_test_automated_ampliseq",
        schedule_interval=None,
        default_args=default_args,
        catchup=False,
    ) as dag:
        # Load the processed studies outside of any task
        processed_studies = load_processed_studies()

        with TaskGroup("nextflow_tasks", tooltip="Nextflow processing tasks") as nextflow_tasks, \
             TaskGroup("rscript_tasks", tooltip="R script processing tasks") as rscript_tasks:

            if os.path.exists(ALL_STUDIES_PATH):
                with open(ALL_STUDIES_PATH, "r") as file:
                    next(file)  # Skip header
                    for line in file:
                        study, path = line.strip().split(",")
                        current_timestamp = os.path.getmtime(path)
                        process_study = study not in processed_studies or processed_studies[study]['timestamp'] != str(current_timestamp)

                        if process_study:
                            templated_command_nextflow = """
                                export TOWER_ACCESS_TOKEN=eyJ0aWQiOiA5NTExfS5mNmZmN2NhYjgwMjliZTgzYzE5NjkyMGY2MGMwODE3OTA5NWJmYzNj; \
                                nextflow run nf-core/ampliseq -with-tower -r 2.9.0 \
                                -params-file {{ params.study_params_path }} \
                                -work-dir {{ params.study_work_dir }} --input {{ params.study_samplesheet_path }} \
                                --outdir {{ params.study_out_path }} -profile docker
                            """

                            templated_command_rscript = """
                                Rscript /data/MicrobiomeDB/mbio_airflow_dags/bin/ampliseq_postProcessing.R \
                                {{ params.study }} {{ params.study_out_path }}
                            """

                            nextflow_task = BashOperator(
                                task_id=f'nextflow_{study}',
                                bash_command=templated_command_nextflow,
                                params={
                                    'study_params_path': f"{path}/nf-params.json",
                                    'study_work_dir': f"{path}/work",
                                    'study_samplesheet_path': f"{path}/samplesheet.csv",
                                    'study_out_path': f"{path}/out"
                                },
                                task_group=nextflow_tasks
                            )

                            rscript_task = BashOperator(
                                task_id=f'rscript_{study}',
                                bash_command=templated_command_rscript,
                                params={
                                    'study': study,
                                    'study_out_path': f"{path}/out"
                                },
                                task_group=rscript_tasks
                            )

                            nextflow_task >> rscript_task
            else:
                raise FileNotFoundError(f"Studies file not found: {ALL_STUDIES_PATH}")

        return dag

# Define the DAG by calling the function
ampliseq_pipeline = create_dag()