import requests
import traceback
import json
import os
import boto3
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable

# Change the name of webhook connection as you set in airflow.
GCHAT_CONNECTION = "gchat_webhook"

def extract_error_message(message: str) -> str:
    """
    Extracts and cleans an error message from a dbt result message.
    Removes newlines and trims compiled code reference.
    """
    try:
        if not message or ":" not in message:
            return (message or "No message provided").replace("\n", "").strip()

        if "compiled Code at" in message:
            first_colon = message.find(":")
            compiled_index = message.find("compiled Code at")
            error_part = message[first_colon + 1 : compiled_index]
        else:
            error_part = message.split(":", 1)[1]

        return error_part.replace("\n", "").strip()
    except Exception:
        return "Failed to parse error message"
    
def get_dbt_summary(run_results: dict) -> str:
    """
    Returns a summary string of dbt results
    """
    if not run_results or "results" not in run_results:
        return "No results"

    status_counts = {"success": 0, "warn": 0, "error": 0, "fail": 0, "skipped": 0}
    for result in run_results["results"]:
        status = result.get("status", "").lower()
        if status in status_counts:
            status_counts[status] += 1

    total = sum(status_counts.values())

    return (
        f"PASS={status_counts['success']} "
        f"WARN={status_counts['warn']} "
        f"ERROR={status_counts['error'] + status_counts['fail']} "
        f"SKIP={status_counts['skipped']} "
        f"TOTAL={total}"
    )

def extract_run_results(run_results: dict) -> str:
    """
    Parses a dbt run_results dictionary and returns a formatted string 
    of all failed or errored results.

    Format: 'unique_id -> error message' (one per line).
    Returns an empty string if no failures or invalid input.
    """
    if not run_results:
        print("Invalid run results provided.")
        return ""
    
    summary = get_dbt_summary(run_results)

    lines = []
    for result in run_results.get("results", []):
        if result.get("status") in ["error", "fail"]:
            unique_id = result.get("unique_id", "unknown")
            message = extract_error_message(result.get("message", ""))
            lines.append(f"{unique_id} -> {message}")

    return f"{summary}\n\n" + "\n\n".join(lines)
    
def task_fail_alert_dbt(context):
    """
    Sends an alert to Google Chat in case of task failure specifically for dbt tasks.
    Args:
        context (dict): Context object containing information about the task instance.
    """

    # forming the run_id, we will use it later as a unique thread_id so that we can push exception details as thread
    # while exception message will be posted as a card in Space.
    run_id = str(context.get("task_instance").dag_id)+"-"+str(context.get("task_instance").run_id).replace(
        "+", "-").replace(":", "-")
    dag_id = context.get("task_instance").dag_id
    
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket='evm-etl', Key=f'dbt/{dag_id}/run_results.json')
    content = response['Body'].read().decode('utf-8')
    run_results = json.loads(content)

    print("task_fail_alert()")
    exception = extract_run_results(run_results)
    formatted_exception = str(exception)

    base_log_url = context.get("task_instance").log_url
    print(base_log_url)
    base_mark_success_url = context.get("task_instance").mark_success_url
    
    if(Variable.get("env")=='dev'):
            log_url=base_log_url.replace("localhost:8080", "airflow.dev.internal")
            mark_success_url=base_mark_success_url.replace("localhost:8080", "airflow.dev.internal")
    elif(Variable.get("env")=='prod'):
            log_url=base_log_url.replace("localhost:8080", "airflow.prod.internal") 
            mark_success_url=base_mark_success_url.replace("localhost:8080", "airflow.prod.internal")
    try:
        tb = None if type(exception) == str else exception.__traceback__
        formatted_exception = "".join(
            traceback.format_exception(etype=type(
                exception), value=exception, tb=tb)
        )
    except:
        pass
    # form a card to represent alert in a better way.
    
    body = {
        'cardsV2': [{
            'cardId': 'createCardMessage',
            'card': {
                'header': {
                    'title': "{task} is failed after {tries} tries".format(task=context.get("task_instance").task_id, tries=context.get("task_instance").prev_attempted_tries),
                    'subtitle': context.get("task_instance").dag_id,
                    'imageUrl': "https://img.icons8.com/fluency/48/delete-sign.png"
                },
                'sections': [
                    {
                        'widgets': [
                            {
                                "textParagraph": {
                                    "text": f"<b>Execution Time:</b> <time>{context.get('logical_date')}</time>",
                                }
                            },
                            {
                                "textParagraph": {
                                    "text": f"<b>Task Duration: </b> {context.get('task_instance').duration}s",
                                }
                            },
                            {
                                "textParagraph": {
                                    "text": f"<b>Exception:</b> <i>{str(exception)}</i>",
                                }
                            },
                            {
                                'buttonList': {
                                    'buttons': [
                                        {
                                            'text': 'View Logs',
                                            'onClick': {
                                                'openLink': {
                                                    'url': log_url
                                                }
                                            }
                                        },
                                        {
                                            'text': 'Mark Success',
                                            'onClick': {
                                                'openLink': {
                                                    'url': mark_success_url
                                                }
                                            }
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                ]
            }
        }]
    }

    thread_ref = f"&threadKey={run_id}&messageReplyOption=REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD"
    full_url = _get_webhook_url(GCHAT_CONNECTION, thread_ref)
    print("sending alert card")
    _make_http_request(body, full_url)

    print("sending exception as a thread")
    body = {
        "text": f"""<users/108448109570314801636>
                    ```{formatted_exception}```"""
    }
    # _make_http_request(body, full_url)
    try:
        s3.delete_object(Bucket='evm-etl', Key=f'dbt/{dag_id}/run_results.json')
        print("run_results.json deleted from S3")
    except Exception as e:
        print(f"Failed to delete run_results.json: {e}")

def _make_http_request(body, full_url):
    """
    Sends an HTTP POST request with the provided body to the given URL.
    Args:
        body (dict): The request body.
        full_url (str): The URL to send the request to.
    """
    r = requests.post(
        url=full_url,
        json=body,
        headers={"Content-type": "application/json"},
    )
    print(r.status_code, r.ok)


def _get_webhook_url(connection_id: str, thread_ref: str = ""):
    """
    Retrieves the webhook URL for the specified connection ID.
    Args:
        connection_id (str): The connection ID.
        thread_ref (str): The optional thread reference.
    Returns:
        str: The constructed URL.
    """
    gchat_connection = BaseHook.get_connection(connection_id)
    full_url = f"{gchat_connection.host}{gchat_connection.password}{thread_ref}"
    return full_url