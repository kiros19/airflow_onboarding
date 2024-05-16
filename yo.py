from datetime import timedelta
import pendulum

from airflow.decorators import dag, task

default_args = {
    "owner": "nifiservice",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
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
    def yo():
        import logging
        log = logging.getLogger('airflow.task')
        log.info('Yo!')
     
    @task
    def no():
        import logging
        log = logging.getLogger('airflow.task')
        log.info('No(') 
       
    (
        yo()
        >> no()
    )
    
yo_dag = yo_dag()