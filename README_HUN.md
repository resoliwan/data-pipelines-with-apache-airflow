# Airflow test
- DB init
    - Set absolute path of AIRFLOW_HOME at .envrc
        - 아래와 같이 세팅
            - sql_alchemy_conn = sqlite:////users/younlee/temp_workspace/data-pipelines-with-apache-airflow/test_airflow_home/airflow.db
    - Go to home and execute
        - ```airflow db init ```
