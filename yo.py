from datetime import timedelta
import pendulum

from airflow.decorators import dag, task

default_args = {
    "owner": "nifiservice",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
    "params": {
        "amount": 20
    }
}

@dag(
    dag_id='yo_dag',
    default_args=default_args,
    schedule=None,
    catchup=False,
    concurrency=10,
    start_date=pendulum.yesterday("Europe/Moscow"),
    max_active_runs=1,
    tags=['facts', 'rkeeper', 'dt1'],
)
def yo_dag():
    
    @task
    def get_restaurants(**kwargs):
        amount = kwargs["params"]["amount"]
        return [n for n in range(amount)]
    
    @task
    def connect(number):
        import logging
        log = logging.getLogger('airflow.task')
        
        import pyodbc
        from airflow.models import Variable
        
        user = Variable.get("restaurant_DB_user")
        password = Variable.get("SQL_Server_password")
        host = f"restaurant_host_{number}"
        
        log.info(host)
        
        # conn_string = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={host};DATABASE=RK_Central;UID={user};PWD={password};TrustServerCertificate=yes;"
        # Дальше грузим
        # ...
    
    (
        connect.expand(number=get_restaurants())
    )
    
yo_dag = yo_dag()
