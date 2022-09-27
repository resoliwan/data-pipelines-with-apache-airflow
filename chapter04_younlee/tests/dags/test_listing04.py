import datetime
import os
import uuid

import arrow
import pandas as pd
import pytest
from airflow.operators.bash import BashOperator
from airflow.utils.session import NEW_SESSION
from airflowbook.dags.listing04 import _get_data, dag, get_data
from airflowbook.operators.op import HelloBash


def test__get_data():
    output_path = "/tmp/data/test_wiki.gz"
    file_name = _get_data("2022", "09", "25", "01", output_path, overwrite=False)
    assert file_name == output_path
    assert os.path.isfile(output_path)


def get_rendered_task(session, a_dag, task_id):
    dr = a_dag.create_dagrun(
        run_id="test1",
        state="running",
        session=session,
    )
    ti = dr.get_task_instance(task_id)
    task = a_dag.get_task(ti.task_id)
    ti.refresh_from_task(task)
    context = ti.get_template_context(ignore_param_exceptions=False)
    rendered_task = task.render_template_fields(context)
    return rendered_task


@pytest.mark.usefixtures("reset_airflowdb")
def test_get_data_operator(session):
    task = get_rendered_task(session, dag, get_data.task_id)
    get_data.run(test_mode=True)
    assert os.path.isfile(task.op_kwargs["output_path"])
