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

# start_task = DummyOperator(
#     task_id='start_Auto',
#     dag=dag
# )

automation_task = BashOperator(
    task_id='Extract_And_Save',
    #bash_command= 'mkdir C:/Users/Admin/Desktop/DA_Project/DAP/Airflow/test',
    bash_command='python /opt/airflow/dags/bots/SaveDataToPostgre.py',
    dag=dag
)


automation_task_2 = BashOperator(
    task_id='Transform_And_Load',
    #bash_command= 'mkdir C:/Users/Admin/Desktop/DA_Project/DAP/Airflow/test',
    bash_command='python /opt/airflow/dags/bots/TransformPostgreData.py',
    dag=dag
)


# automation_task_3 = BashOperator(
#     task_id='TransformData',
#     #bash_command= 'mkdir C:/Users/Admin/Desktop/DA_Project/DAP/Airflow/test',
#     bash_command='python /opt/airflow/dags/bots/MongodbHelper.py',
#     dag=dag
# )

# with DAG(
#     dag_id="AutoMan",
#     default_args=default_args,
#     schedule_interval=None) as dag:

#     saveToAnotherTable=PythonOperator(
#         task_id = "moveTable",
#         python_callable= insert_single_doc("ChicagoCrime1",{"ID":3637267463,"Case Number":"JF352712","Date":"08/10/2022 16:00","Primary Type":"ASSAULT","Latitude":41.78033068,"Longitude":-87.68489178,"Location":"(41.780330681, -87.684891779)"})
#     )

 #saveToAnotherTable

automation_task >> automation_task_2 #>> automation_task_3

