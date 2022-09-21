import airflow.utils.dates
from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator

dag = DAG(
    dag_id="01_younlee",
    description="younlee desc",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@daily",
)

a_op = DummyOperator(task_id="a_op_id", dag=dag)
b_op = DummyOperator(task_id="b_op_id", dag=dag)
c_op = DummyOperator(task_id="c_op_id", dag=dag)
e_op = DummyOperator(task_id="e_op_id", dag=dag)
f_op = DummyOperator(task_id="f_op_id", dag=dag)

a_op >> b_op
c_op >> e_op
[b_op, e_op] >> f_op
