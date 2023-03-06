import datetime as dt

from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id='taskflow_kubernetes',
    start_date=dt.datetime(2021, 3, 1),
    schedule_interval='@once',
    catchup=False
) as dag:

    @task.kubernetes(
        image="python:3.10-slim-buster",
        in_cluster=True,
        namespace="airflow-system"
        )
    def sleep_for_some_time(sleep_time):
        import time

        print("Hello from k8s pod")
        time.sleep(sleep_time)
        return sleep_time

    @task.kubernetes(image="python:3.10-slim-buster", namespace="airflow-system", in_cluster=True)
    def print_pattern(n):
        #n = 5
        for i in range(0, n):
            # inner loop to handle number of columns
            # values changing acc. to outer loop
            for j in range(0, i + 1):
                # printing stars
                print("* ", end="")

            # ending line after each row
            print("\r")

    # execute_in_k8s_pod_instance = execute_in_k8s_pod()
    # print_pattern_instance = print_pattern(execute_in_k8s_pod_instance)

    output = sleep_for_some_time(2)
    print_pattern(output)
