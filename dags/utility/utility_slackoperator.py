""" utility_slackoperator.py

These are alert functions that utilize the Slack operators. Trying from here - https://github.com/tszumowski/airflow_slack_operator

"""
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

SLACK_CONN_ID = "conn_slack"


def task_success_slack_alert(context):
    """
    Callback task that can be used in DAG to alert of successful task completion

    Args:
        context (dict): Context variable passed in from Airflow

    Returns:
        None: Calls the SlackWebhookOperator execute method internally

    """
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :large_green_circle: [dev airflow] Task Succeeded! 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )

    success_alert = SlackWebhookOperator(
        task_id="slack_test",
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )

    return success_alert.execute(context=context)


def task_fail_slack_alert(context):
    """
    Callback task that can be used in DAG to alert of failure task completion

    Args:
        context (dict): Context variable passed in from Airflow

    Returns:
        None: Calls the SlackWebhookOperator execute method internally

    """
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )

    failed_alert = SlackWebhookOperator(
        task_id="slack_test",
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )

    return failed_alert.execute(context=context)
