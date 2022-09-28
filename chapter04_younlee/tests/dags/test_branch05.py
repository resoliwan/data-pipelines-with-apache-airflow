import datetime as dt

import pendulum
import pytest
from airflow.executors.debug_executor import DebugExecutor
from airflow.utils import cli as cli_utils
from airflow.utils import timezone
from airflowbook.dags.branch05 import dag, pick_erp, start


@pytest.mark.usefixtures("reset_airflowdb")
def test_start():
    start.run()


@pytest.mark.usefixtures("reset_airflowdb")
def test_pick_erp():
    pick_erp.run()


@pytest.mark.usefixtures("reset_airflowdb")
def test_dag(session):
    # /Users/younlee/temp_workspace/airflow/airflow/cli/commands/dag_command.py.dag_test
    dag.run(
        executor=DebugExecutor(),
    )
