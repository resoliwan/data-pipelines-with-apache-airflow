import pandas as pd
from airflow.operators.bash import BashOperator
from airflow.utils.session import NEW_SESSION
from airflowbook.dags.unscheduled0301 import (
    _calculate_stats,
    calculate_stats,
    fetch_events,
)
from airflowbook.operators.op import HelloBash


def test_bash_operator():
    task = HelloBash(task_id="test_bash_id", bash_command="echo 'hello'")
    result = task.execute(context={})
    assert result == "hello"


@pytest.mark.usefixtures("reset_airflowdb")
def test_fetch_event_operator(test_dag):
    fetch_events.run(test_mode=True)
    # fetch_events.execute(context={})
    assert False


def test_calculate_stats():
    df = _calculate_stats("/tmp/data/events.json", "/tmp/data/events.csv")
    assert df.size > 0


def test_calculate_stats_operator():
    # fetch_events.execute(context={})
    # calculate_stats.execute(context={})
    fetch_events.run(ignore_ti_state=True, test_mode=True)
    calculate_stats.run(ignore_ti_state=True, test_mode=True)
    df = pd.read_csv("/tmp/data/stats.csv")
    assert False
    assert df.size > 0
