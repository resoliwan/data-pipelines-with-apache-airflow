import os
from urllib import request

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="listing04", start_date=pendulum.yesterday(), schedule_interval="@hourly"
)


def _get_data(year, month, day, hour, output_path, overwrite, **context):
    is_exist = os.path.isfile(output_path)
    if not is_exist or overwrite:
        url = (
            "https://dumps.wikimedia.org/other/pageviews/"
            f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
        )
        file_name, _ = request.urlretrieve(url, output_path)
        return file_name

    return output_path


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={
        "year": "{{ logical_date.year }}",
        "month": "{{ logical_date.month }}",
        "day": "{{ logical_date.day }}",
        "hour": "{{ logical_date.hour }}",
        "output_path": "/tmp/wikipageviews.gz",
        "overwrite": False,
    },
    dag=dag,
)
