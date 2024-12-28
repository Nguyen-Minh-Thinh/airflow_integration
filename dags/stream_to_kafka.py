from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
# from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
# from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from datetime import datetime 

dag = DAG(
    dag_id = "Stream_to_kafka",
    start_date = datetime(2024, 12, 23)
)

def stream_data():
    from kafka import KafkaProducer
    import requests 
    producer = KafkaProducer(
        bootstrap_servers = 'kafka:29092',
        value_serializer = lambda v: v.encode("utf-8")
    )

    response = requests.get("https://randomuser.me/api")
    response = response.text

    producer.send('test_airflow_integration', response)
    producer.flush()


starting_task = BashOperator(
    task_id = "starting_task",
    bash_command = "echo 'Airflow started.'",
    dag = dag   
)

streaming_task = PythonOperator(
    task_id = "stream_data_to_kafka",
    python_callable = stream_data,
    dag = dag
)

finishing_task = BashOperator(
    task_id = "finishing_task",
    bash_command = "echo 'Airflow finished.'"
)

starting_task >> finishing_task
