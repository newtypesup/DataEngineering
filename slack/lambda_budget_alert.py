import json
import urllib3

def lambda_handler(event, context):
    print("🚀 Lambda 함수가 실행되었습니다.")
    print("📥 받은 이벤트:")
    print(json.dumps(event))

    webhook_url = "SLACK_WEBHOOK"
    http = urllib3.PoolManager()

    try:
        sns_message_str = event['Records'][0]['Sns']['Message']
        print("📩 SNS Message (string):", sns_message_str)

        sns_message = json.loads(sns_message_str)
        print("📦 Parsed JSON:", sns_message)

        budget_name = sns_message.get('budgetName', 'Unknown')
        budgeted = float(sns_message.get('budgetedAmount', 1))
        actual = float(sns_message.get('costAmount', 0))
        threshold = sns_message.get('threshold', 'N/A')
        percent_used = round((actual / budgeted) * 100, 2)
        percent_over = round(percent_used - 100, 2)

        slack_msg = {
            "text": f"💸 *AWS 예산 경고*\n\n"
                    f"*예산 이름:* `{budget_name}`\n"
                    f"*예산 한도:* ${budgeted}\n"
                    f"*사용 금액:* ${actual}\n"
                    f"*사용 비율:* {percent_used}%\n"
                    f"*초과 비율:* {percent_over}%\n\n"
                    f"⚠️ 예산을 초과했습니다!"
        }

        print("📤 Slack으로 전송할 메시지:", slack_msg)

        response = http.request(
            "POST",
            webhook_url,
            body=json.dumps(slack_msg),
            headers={'Content-Type': 'application/json'}
        )

        print(f"✅ Slack 응답 코드: {response.status}")
        print(f"✅ Slack 응답 내용: {response.data.decode('utf-8')}")

    except Exception as e:
        print("❌ 예외 발생:", str(e))
