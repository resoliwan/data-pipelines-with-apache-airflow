from airflow.operators.bash import BashOperator
from airflowbook.dags.unscheduled0301 import fetch_events
from airflowbook.operators.op import HelloBash


def test_bash_operator():
    task = HelloBash(task_id="test_bash_id", bash_command="echo 'hello'")
    result = task.execute(context={})
    assert result == "hello"


def test_fetch_event_operator():
    fetch_events.execute(context={})
