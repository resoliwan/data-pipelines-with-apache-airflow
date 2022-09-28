import datetime
import os
import uuid

import arrow
import pandas as pd
import pendulum
import pytest
from airflow.models.connection import Connection
from airflow.operators.bash import BashOperator
from airflow.utils.session import NEW_SESSION
from airflowbook.dags.listing04 import (
    _create_pageview_sql,
    _get_data,
    _get_pageviews,
    dag,
    extract_gz,
    fetch_pageviews,
    get_data,
    write_to_postgres,
)
from airflowbook.operators.op import HelloBash


def test__get_data():
    output_path = "/tmp/data/test_wiki.gz"
    file_name = _get_data(
        "2022", "09", "25", "01", output_path, skip_if_file_exist=True
    )
    assert file_name == output_path
    assert os.path.isfile(output_path)


def get_rendered_task(session, a_dag, operator):
    dr = a_dag.get_last_dagrun(session, True)
    # dr = a_dag.create_dagrun(
    #     run_id="test1",
    #     state="running",
    #     session=session,
    # )
    ti = dr.get_task_instance(operator.task_id)
    task = a_dag.get_task(ti.task_id)
    ti.refresh_from_task(task)
    context = ti.get_template_context(ignore_param_exceptions=False)
    rendered_task = task.render_template_fields(context)
    return rendered_task


@pytest.mark.usefixtures("reset_airflowdb")
def test_get_data_operator(session):
    get_data.run(end_date=pendulum.yesterday(), test_mode=True, session=session)
    task = get_rendered_task(session, dag, get_data)
    assert os.path.isfile(task.op_kwargs["output_path"])


@pytest.mark.usefixtures("reset_airflowdb")
def test_extract_gz(session):
    # create file for extract
    get_data.run(end_date=pendulum.yesterday(), test_mode=True, session=session)
    extract_gz.execute(context={})
    assert os.path.isfile("/tmp/wikipageviews.gz")


@pytest.mark.usefixtures("reset_airflowdb")
def test__get_pageviews(session):
    input_path = "/tmp/wikipageviews"
    if not os.path.isfile(input_path):
        get_data.run(end_date=pendulum.yesterday(), test_mode=True, session=session)
        extract_gz.execute(context={})

    pagenames = {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}
    result = _get_pageviews(input_path, pagenames)
    for page_name, view_count in result.items():
        assert page_name
        assert int(view_count) > -1


@pytest.mark.usefixtures("reset_airflowdb")
def test_fetch_pageviews(session):
    input_path = "/tmp/wikipageviews"
    if not os.path.isfile(input_path):
        get_data.run(end_date=pendulum.yesterday(), test_mode=True, session=session)
        extract_gz.execute(context={})

    fetch_pageviews.run(end_date=pendulum.yesterday(), test_mode=True, session=session)
    task = get_rendered_task(session, dag, fetch_pageviews)

    assert os.path.isfile(task.op_kwargs["output_path"])


def test_create_pageview_sql():
    output_path = "/tmp/pageviewsql.sql"
    pagenames = {"Google": 1, "Facebook": 2}

    _create_pageview_sql(output_path, pagenames, pendulum.today())
    with open(output_path, "r") as f:
        for line in f:
            assert len(line) > 10


@pytest.mark.usefixtures("reset_airflowdb")
def test_write_to_postgres(session):
    # check .envrc export AIRFLOW_CONN_MY_POSTGRES='postgres://airflow:airflow@localhost:5432/airflow'
    # create table /Users/younlee/temp_workspace/data-pipelines-with-apache-airflow/chapter04_younlee/scripts/create_table.sql
    # verify exist of pageviews
    # if it is not exit you can change name of _create_pageview_sql
    fetch_pageviews.run(end_date=pendulum.yesterday(), test_mode=True, session=session)
    write_to_postgres.run(
        end_date=pendulum.yesterday(), test_mode=True, session=session
    )
