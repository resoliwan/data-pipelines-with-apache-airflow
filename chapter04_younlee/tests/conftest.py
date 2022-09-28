import datetime
import uuid

import pytest
from airflow import settings
from airflow.models import DAG, BaseOperator, DagRun, TaskInstance, Trigger
from airflow.utils.session import create_session


@pytest.fixture
def test_dag():
    return DAG(
        dag_id=str(uuid.uuid4()),
        default_args={"owner": "airflow", "start_date": datetime.datetime(2015, 1, 1)},
        schedule_interval="@daily",
    )


@pytest.fixture
def session():

    with create_session() as session:
        yield session
        session.rollback()


def clear_db_runs():
    with create_session() as session:
        session.query(Trigger).delete()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()
        session.commit()


@pytest.fixture
def reset_airflowdb():
    clear_db_runs()
