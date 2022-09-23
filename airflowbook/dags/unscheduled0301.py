from pathlib import Path

import pandas as pd
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="01_unscheduled", start_date=pendulum.today("UTC"), schedule_interval=None
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "make -p /tmp/data &&"
        "curl -o /tmp/data/events_{{ds}}.json http://localhost:5001/events?"
        "start_date={{ds}}&"
        "end_date={{next_ds}}"
    ),
    dag=dag,
)
# fetch_events.dag = dag


def _calculate_stats(input_path: str, output_path: str) -> pd.DataFrame:
    Path(output_path).parent.mkdir(exist_ok=True)

    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index()

    stats.to_csv(output_path, index=False)

    return stats


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={
        "input_path": "/tmp/data/events_{{ds}}.json",
        "output_path": "/tmp/data/events_{{ds}}.csv",
    },
)
calculate_stats.dag = dag

fetch_events >> calculate_stats
