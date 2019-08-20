import datetime as dt

import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(dag_id="chapter3_3", schedule_interval="@daily", start_date=dt.datetime(year=2019, month=1, day=1))

fetch_events = BashOperator(
    task_id="fetch_events", bash_command="curl -o data/events.json https://localhost:5000/events", dag=dag
)


def _calculate_stats(input_path, output_path):
    """Calculates event statistics."""
    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index()
    stats.to_csv(output_path, index=False)


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={"input_path": "data/events.json", "output_path": "data/stats.csv"},
    dag=dag,
)

fetch_events >> calculate_stats
