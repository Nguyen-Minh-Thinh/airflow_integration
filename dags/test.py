from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import random 


dag = DAG(
    dag_id = 'Hello_World',
    start_date = datetime(2024, 12, 23)
)

def printOut():
    print("Hello World. This is an airflow integration!")


starting_task = BashOperator(
    task_id = 'starting_task',
    bash_command = 'echo "Airflow started."',
    dag = dag   
)

print_out = PythonOperator(
    task_id = 'print_out',
    python_callable = printOut,
    dag = dag
)

finishing_task = BashOperator(
    task_id = 'finishing_task',
    bash_command = 'echo "Airflow finished."',
    dag = dag
)

starting_task >> print_out >> finishing_task