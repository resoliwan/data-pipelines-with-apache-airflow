import pendulum
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator


def _pick_erp_system(erp_change_date, **context):
    if context["logical_date"] < pendulum.today():
        return "fetch_sales_old"
    else:
        return "fetch_sales_new"


with DAG(
    dag_id="branch05", start_date=pendulum.yesterday(), schedule="@daily", catchup=False
) as dag:
    start = EmptyOperator(task_id="start")

    pick_erp = BranchPythonOperator(
        task_id="pick_erp",
        python_callable=_pick_erp_system,
        op_kwargs={
            "erp_change_date": "{{logical_date}}",
        },
    )
    join_erp_branch = EmptyOperator(task_id="join_erp_branch")
    fetch_sales_new = EmptyOperator(task_id="fetch_sales_new")
    clean_sales_new = EmptyOperator(task_id="clean_sales_new")

    fetch_sales_old = EmptyOperator(task_id="fetch_sales_old")
    clean_sales_old = EmptyOperator(task_id="clean_sales_old")

    fetch_weather = EmptyOperator(task_id="fetch_weather")
    clean_weather = EmptyOperator(task_id="clean_weather")

    join_datasets = EmptyOperator(task_id="join_datasets")
    train_model = EmptyOperator(task_id="train_model")

    deploy_model = EmptyOperator(task_id="deploy_model")

    start >> pick_erp

    pick_erp >> [fetch_sales_new, fetch_sales_old]
    fetch_sales_new >> clean_sales_new
    fetch_sales_old >> clean_sales_old
    [clean_sales_new, clean_sales_old] >> join_erp_branch

    fetch_weather >> clean_weather

    [join_erp_branch, clean_weather] >> join_datasets
    join_datasets >> train_model
    train_model >> deploy_model
