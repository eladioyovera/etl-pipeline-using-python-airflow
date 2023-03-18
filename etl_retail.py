from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

from datetime import datetime

args = {"owner": "eladio", "start_date": days_ago(1), "cpus":4, "ram":2048, "disk":2048
        }

dag = DAG(
    dag_id='etl_retail',
    default_args=args,
    start_date=days_ago(1),
    schedule_interval='@once',
)
with dag:
    run_script_ingest = BashOperator(
        task_id='run_script_ingest',
        bash_command='python /home/eladio/airflow/dags/project_retail/ingest.py',
    )
    run_script_transform = BashOperator(
        task_id='run_script_transform',
        bash_command='python /home/eladio/airflow/dags/project_retail/transform.py',
    )

    run_script_ingest >> run_script_transform