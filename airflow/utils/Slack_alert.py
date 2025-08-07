# airflow/utils/slack_alert.py
import os, sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__),'..','..'))
import requests

def slack_fail_alert(context):
    slack_webhook_url = os.environ.get("SLACK_WEBHOOK_URL")

    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url

    text = f"""
    :rotating_light: *Airflow DAG 실패!*
    *DAG*: {dag_id}
    *Task*: {task_id}
    *Execution Time*: {execution_date}
    *Log URL*: <{log_url}|Click here>
    """

    requests.post(slack_webhook_url, json={"text": text})

def slack_success_alert(context):
    slack_webhook_url = os.environ.get("SLACK_WEBHOOK_URL")

    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url

    text = f"""
    :white_check_mark: *Airflow DAG 성공!*
    *DAG*: {dag_id}
    *Task*: {task_id}
    *Execution Time*: {execution_date}
    *Log URL*: <{log_url}|Click here>
    """

    requests.post(slack_webhook_url, json={"text": text})
