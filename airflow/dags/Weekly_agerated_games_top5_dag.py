import time
import boto3 #type: ignore
import pendulum
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator #type: ignore
from airflow.models import DagRun #type: ignore
from airflow.utils.session import create_session #type: ignore
from utils.Slack_alert import slack_fail_alert

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'on_failure_callback': slack_fail_alert,
}

def weekly_agerated_games_top5_job(job_name, **context):
    dag_id = context['dag'].dag_id
    run_id = context['run_id']

    # 실행 중인 동일 DAG 확인
    with create_session() as session:
        running_dagruns = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.state == 'running',
            DagRun.run_id != run_id
        ).count()

    if running_dagruns > 0:
        print(f"[{dag_id}] 실행 중인 다른 DAG {running_dagruns}개 감지됨 → 120초 대기")
        time.sleep(120)
        # 딜레이를 걸어둔 이유 :
        # 만약 단번에 많은 DAG를 실행할 경우, 현재 DAG가 Glue job을 호출하는 시간은 5초도 안된다. 
        # Glue job의 최대 실행 갯수를 넘으면 대기가 되는게 아닌, job 실패로 돌아가기 때문에
        # 해당 job이 완료되는 시간보다 충분한 대기 시간을 부여해서 과거 긴 기간에 대한 배치가
        # 필요할 때, 날짜를 수정할 필요 없이 배치가 진행되기 때문에 용이하다.
        # 로컬의 Airflow가 아닌 AWS의 Airflow 환경에서는 Glue script에서의 딜레이를 부여하려면 
        # 비용을 비교해보고 결정해야 한다.
    else:
        print(f"[{dag_id}] 실행 중인 다른 DAG 없음 → 바로 실행")

    # Glue Job 실행
    execution_date = context['data_interval_start'].strftime("%Y-%m-%d")
    glue = boto3.client('glue', region_name='ap-northeast-2')
    response = glue.start_job_run(JobName=job_name, Arguments={'--batch_date': execution_date})
    job_run_id = response['JobRunId']
    print(f"Start Glue JobName: {job_name}, JobRunId: {job_run_id}")
    print(f"Start Glue JobRunId: {job_run_id} for batch_date={execution_date}")

with DAG(
    dag_id='weekly_agerated_games_top5',
    default_args=default_args,
    start_date=datetime(2025, 5, 5, 9, 10, tzinfo=KST),
    end_date=datetime(2025, 8, 1, 0, 0, tzinfo=KST),
    schedule='10 9 * * 1',  # 매주 월요일 오전 9시 10분 KST
    catchup=True,
    max_active_runs=5,
    tags=['weekly', 'agerated', 'games', 'top5'],
) as dag:

    weekly_agerated_games_top5 = PythonOperator(
        task_id='weekly_agerated_games_top5',
        python_callable=weekly_agerated_games_top5_job,
        op_args=['weekly_agerated_games_top5'],
        provide_context=True,
    )

    weekly_agerated_games_top5