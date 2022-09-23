import datetime
import uuid

import pytest
from airflow import settings
from airflow.models import DAG, BaseOperator, DagRun


@pytest.fixture
def test_dag():
    return DAG(
        dag_id=str(uuid.uuid4()),
        default_args={"owner": "airflow", "start_date": datetime.datetime(2015, 1, 1)},
        schedule_interval="@daily",
    )


@pytest.fixture
def reset_airflowdb():
    session = settings.Session()
    session.query(DagRun).delete()
    session.commit()
