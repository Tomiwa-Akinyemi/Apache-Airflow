from datetime import datetime
from airflow.models import DAG,Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from bots.MongodbHelper import insert_single_doc


default_args={
    "owner":"AkTomiwa",
    "start_date":datetime(2023,4,26)
}

dag = DAG(
    'Chicago_Crime_Pipeline',
    default_args=default_args,
    schedule_interval=None
)

automation_task = BashOperator(
    task_id='Extract_And_Save',
    bash_command='python /opt/airflow/dags/bots/SaveDataToPostgre.py',
    dag=dag
)


automation_task_2 = BashOperator(
    task_id='Transform_And_Load',
    bash_command='python /opt/airflow/dags/bots/TransformPostgreData.py',
    dag=dag
)


automation_task >> automation_task_2 

