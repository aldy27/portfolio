from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from datetime import datetime

default_arguments = {
    "owner": "rinaldy w",
    "start_date": datetime(2021, 10, 26)
}

dag = DAG(
    dag_id="dag_choose_model_rinaldy",
    default_args=default_arguments
)

# operators
start = DummyOperator(
    task_id="start",
    dag=dag
)

def _choose_best_model():
    accuracy = 7

    if accuracy > 5:
        return "accurate"
    else:
        return "inaccurate"


task_1 = BranchPythonOperator(
    task_id="choose_best_model",
    python_callable=_choose_best_model,
    dag=dag
)

task_2 = DummyOperator(
    task_id="accurate",
    dag=dag
)

task_3 = DummyOperator(
    task_id="inaccurate",
    dag=dag
)

# dependencies
start >> task_1 >> [task_2, task_3]