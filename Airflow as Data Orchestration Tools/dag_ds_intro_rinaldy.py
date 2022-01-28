from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import datetime
import requests
import json

default_arguments = {
    "owner": "rinaldy w",
    "start_date": datetime(2021, 10, 26)
}

dag = DAG(
    dag_id="dag_ds_intro_rinaldy",
    default_args=default_arguments
)

# operators
start = DummyOperator(
    task_id="start",
    dag=dag
)

def retrieve_data():
    url="https://jsonplaceholder.typicode.com/todos"


    response = requests.get(url)
    html_page = response.content
    json_page = html_page.decode('utf8').replace("\n", '')
    data = json.loads(json_page)
    s = json.dumps(data, indent=4, sort_keys=True)
    
    filename="todo_list.json"
    with open(filename, "w") as file:
        file.write(s)



python_task = PythonOperator(
    task_id="print_sentence",
    python_callable=retrieve_data,
    dag=dag
)


bash_task = BashOperator(
    task_id="print_status",
    bash_command="echo 'Data retrieved and successfully saved to file'",
    dag=dag
)

# dependencies
start >> python_task >> bash_task 