from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.bash_operator import BashOperator
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

from datetime import datetime, timedelta

default_args = {
    'owner': 'marco',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 4),
    'email': ['email@mail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG('mysql2mysql', default_args=default_args, schedule_interval='@daily')

qry_truncate_staging = """
truncate st_adventure.st_contact
"""

t1 = MySqlOperator(
    sql=qry_truncate_staging,
    mysql_conn_id='mysql_adventure',
    task_id='truncating_staging',
    dag=dag)

qry_populate_staging = """
    insert into st_adventure.st_contact
    (select
        ContactID ,
        FirstName ,
        MiddleName ,
        LastName ,
        EmailAddress ,
        Phone 
    from adventureworks.contact c)
"""


t2 = MySqlOperator(
    sql=qry_populate_staging,
    mysql_conn_id='mysql_adventure',
    task_id='populating_staging',
    dag=dag)

bash_command = """
spark-submit $AIRFLOW_HOME/dags/spark_jobs/process_etl.py
"""


t3 = BashOperator(
    task_id='proccessing_data',
    depends_on_past=False,
    bash_command=bash_command,
    dag=dag
)

# t3 = SparkSubmitOperator(
#     task_id='test_spark',
#     application='$AIRFLOW_HOME/dags/spark_jobs/process_etl.py',
#     start_date=datetime(2020, 6, 4)
# )

t1 >> t2 >> t3
