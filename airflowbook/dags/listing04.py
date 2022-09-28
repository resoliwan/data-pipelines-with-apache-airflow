import logging
import os
from urllib import request

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

logger = logging.getLogger(__name__)

dag = DAG(
    dag_id="listing04",
    start_date=pendulum.yesterday(),
    schedule="@hourly",
    catchup=False,
    template_searchpath="/tmp",
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
    task_id="extract_gz", bash_command="gunzip --force --keep /tmp/wikipageviews.gz"
)


def _get_pageviews(input_path, pagenames):
    result = dict.fromkeys(pagenames, 0)
    with open(input_path, "r") as f:
        for line in f:
            domain_code, page_title, view_count, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_count

    return result


def _create_pageview_sql(output_path, pageviews, logical_date):
    with open(output_path, "w") as f:
        for pagename, view_count in pageviews.items():
            f.write(
                "INSERT INTO pageview_counts VALUES ("
                f"'{pagename}', '{view_count}', '{logical_date}'"
                ");\n"
            )
    return output_path


def _fetch_pageviews(input_path, output_path, pagenames, logical_date):
    pageviews = _get_pageviews(input_path, pagenames)
    _create_pageview_sql(output_path, pageviews, logical_date)


fetch_pageviews = PythonOperator(
    task_id="get_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={
        "input_path": "/tmp/wikipageviews",
        "output_path": "/tmp/pageviewsql.sql",
        "pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"},
    },
    dag=dag,
)

write_to_postgres = PostgresOperator(
    task_id="write_to_postgres",
    postgres_conn_id="postgres_mock",
    # check template_searchpath
    sql="pageviewsql.sql",
    dag=dag,
)
#
# get_data >> extract_gz
