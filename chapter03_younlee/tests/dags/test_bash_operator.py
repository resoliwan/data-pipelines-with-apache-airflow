from airflow.operators.bash import BashOperator


def test_bash_operator():
    task = BashOperator(task_id="test_bash_id", bash_command="echo 'hello'")
    result = task.execute(context={})
    assert result == "hello"
