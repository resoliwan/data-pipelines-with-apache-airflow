import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="01_unscheduled", start_date=pendulum.today("UTC"), schedule_interval=None
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "make -p /tmp/data/events &&"
        "curl -o /tmp/data/events.json http://localhost:5001/events"
    ),
    dag=dag,
)
