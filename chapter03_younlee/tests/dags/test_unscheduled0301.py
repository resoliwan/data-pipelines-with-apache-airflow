import datetime
import os

import arrow
import pandas as pd
import pytest
from airflow.operators.bash import BashOperator
from airflow.utils.session import NEW_SESSION
from airflowbook.dags.unscheduled0301 import (
    _calculate_stats,
    calculate_stats,
    dag,
    fetch_events,
)
from airflowbook.operators.op import HelloBash


def test_bash_operator():
    task = HelloBash(task_id="test_bash_id", bash_command="echo 'hello'")
    result = task.execute(context={})
    assert result == "hello"


@pytest.mark.usefixtures("reset_airflowdb")
def test_fetch_event_operator(session):
    dr = dag.create_dagrun(
        run_id="test1",
        execution_date=datetime.datetime(2019, 1, 1),
        data_interval=(
            datetime.datetime(2019, 1, 1, tzinfo=datetime.timezone.utc),
            datetime.datetime(2019, 1, 2, tzinfo=datetime.timezone.utc),
        ),
        state="running",
        session=session,
    )
    ti = dr.task_instances[0]
    ti.refresh_from_task(dag.get_task(ti.task_id))
    context = ti.get_template_context(ignore_param_exceptions=False)
    ti.render_templates(context=context)

    fetch_events.run(test_mode=True)
    assert os.path.isfile(f"/tmp/data/events_{context['ds']}.json")


@pytest.mark.usefixtures("reset_airflowdb")
def test_calculate_stats(session):
    dr = dag.create_dagrun(
        run_id="test1",
        execution_date=datetime.datetime(2019, 1, 1),
        data_interval=(
            datetime.datetime(2019, 1, 1, tzinfo=datetime.timezone.utc),
            datetime.datetime(2019, 1, 2, tzinfo=datetime.timezone.utc),
        ),
        state="running",
        session=session,
    )
    ti = dr.task_instances[1]
    ti.refresh_from_task(dag.get_task(ti.task_id))
    context = ti.get_template_context(ignore_param_exceptions=False)
    ti.render_templates(context=context)

    df = _calculate_stats(
        calculate_stats.op_kwargs["input_path"],
        calculate_stats.op_kwargs["output_path"],
    )
    assert df.size > 0


@pytest.mark.usefixtures("reset_airflowdb")
def test_calculate_stats_operator(session):
    dr = dag.create_dagrun(
        run_id="test1",
        execution_date=datetime.datetime(2019, 1, 1),
        data_interval=(
            datetime.datetime(2019, 1, 1, tzinfo=datetime.timezone.utc),
            datetime.datetime(2019, 1, 2, tzinfo=datetime.timezone.utc),
        ),
        state="running",
        session=session,
    )
    ti = dr.task_instances[0]
    ti.refresh_from_task(dag.get_task(ti.task_id))
    context = ti.get_template_context(ignore_param_exceptions=False)

    fetch_events.run(test_mode=True)
    calculate_stats.run(test_mode=True)
    df = pd.read_csv(f"/tmp/data/stats_{context['ds']}.csv")
    assert df.size > 0
