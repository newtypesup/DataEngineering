import json
import urllib3

http = urllib3.PoolManager()

SLACK_WEBHOOK_URL = "SLACK_WEBHOOK"
AWS_REGION = "ap-northeast-2"

def lambda_handler(event, context):
    # Glue Job 정보 추출
    glue_job_name = event['detail'].get('jobName', 'Unknown')
    state = event['detail'].get('state', 'Unknown')
    job_run_id = event['detail'].get('jobRunId', 'Unknown')

    # CloudWatch 로그 링크 생성
    log_link = (
        f"https://console.aws.amazon.com/cloudwatch/home?region={AWS_REGION}"
        f"#logEventViewer:group=/aws-glue/jobs/output;stream={job_run_id}"
    )

    # Slack 메시지 구성
    message = (
        f":x: *Glue Job Failed!*\n"
        f"• *Job Name:* `{glue_job_name}`\n"
        f"• *Status:* `{state}`\n"
        f"• *Job Run ID:* `{job_run_id}`\n"
        f"🔗[View Logs]({log_link})"
    )

    # Slack으로 POST 요청 전송
    slack_data = {
        "text": message
    }

    resp = http.request(
        "POST",
        SLACK_WEBHOOK_URL,
        body=json.dumps(slack_data).encode('utf-8'),
        headers={'Content-Type': 'application/json'}
    )

    return {
        'statusCode': resp.status,
        'body': resp.data.decode('utf-8')
    }
