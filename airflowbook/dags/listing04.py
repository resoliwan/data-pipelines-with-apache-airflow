import logging
import os
from urllib import request

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

dag = DAG(
    dag_id="listing04",
    start_date=pendulum.yesterday(),
    schedule_interval="@hourly",
    catchup=False,
)


def _get_data(year, month, day, hour, output_path, skip_if_file_exist, **context):
    is_exist = os.path.isfile(output_path)
    if not is_exist or not skip_if_file_exist:
        url = (
            "https://dumps.wikimedia.org/other/pageviews/"
            f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
        )
        # url = "https://dumps.wikimedia.org/other/pageviews/2022/2022-09/projectviews-20220901-000000"

        try:
            file_name, _ = request.urlretrieve(url, output_path)
        except Exception as e:
            print(e)
            logger.error(f"fail to download at {url}")
            raise e
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
        "skip_if_file_exist": True,
    },
    dag=dag,
)


extract_gz = BashOperator(
    task_id="extract_gz", bash_command="gunzip --force /tmp/wikipageviews.gz"
)
#
# get_data >> extract_gz
