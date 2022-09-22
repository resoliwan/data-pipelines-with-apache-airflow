import airflow.utils.dates
import pendulum
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator

dag = DAG(
    dag_id="01_younlee",
    description="younlee desc",
    start_date=pendulum.today("UTC"),
    schedule_interval="@daily",
)

a_op = EmptyOperator(task_id="a_op_id", dag=dag)
b_op = EmptyOperator(task_id="b_op_id", dag=dag)
c_op = EmptyOperator(task_id="c_op_id", dag=dag)
e_op = EmptyOperator(task_id="e_op_id", dag=dag)
f_op = EmptyOperator(task_id="f_op_id", dag=dag)

a_op >> b_op
c_op >> e_op
[b_op, e_op] >> f_op
