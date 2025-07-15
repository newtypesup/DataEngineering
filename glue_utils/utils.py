### util작성 요령 알아보는 중
import boto3 #type: ignore
import time

# DynamoDB에서 쿼리 읽기
def get_query_from_dynamodb(metric_name, table_name='MetricQueryTable', region='ap-northeast-2'):
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)
    
    response = table.get_item(Key={'metric_name': metric_name})
    
    if 'Item' not in response:
        raise ValueError(f"No query found for metric: {metric_name}")
    
    return response['Item']['query']

# Athena에서 쿼리 실행
def execute_athena_query(query, database, output_location, region='ap-northeast-2'):
    athena_client = boto3.client('athena', region_name=region)
    
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output_location}  # S3 버킷 경로
    )
    
    query_execution_id = response['QueryExecutionId']
    
    while True:
        result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = result['QueryExecution']['Status']['State']
        
        if state in ['SUCCEEDED', 'FAILED']:
            break
        
        time.sleep(5)

    if state == 'FAILED':
        raise Exception(f"Athena query failed: {result['QueryExecution']['Status']['StateChangeReason']}")
    
    results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    return results

# DynamoDB에서 쿼리문을 읽고 Athena에서 실행하는 함수
def run_athena_query_from_dynamodb(metric_name, database, output_location, table_name='MetricQueryTable'):
    # DynamoDB에서 쿼리문 가져오기
    query = get_query_from_dynamodb(metric_name, table_name)
    
    # Athena에서 해당 쿼리 실행
    results = execute_athena_query(query, database, output_location)
    
    return results



# # 실행 예시
# metric_name = 'daily_cate_top5'  # DynamoDB에서 가져올 쿼리의 metric_name
# database = 'athena_database'  # Athena에서 사용할 데이터베이스
# output_location = 's3://newtypesup/athena-results/'  # Athena 쿼리 결과를 저장할 S3 경로

# # DynamoDB에서 쿼리문을 가져와 Athena에서 실행
# results = run_athena_query_from_dynamodb(metric_name, database, output_location)

# # 쿼리 결과 출력
# for row in results['ResultSet']['Rows']:
#     print(row['Data'])


# # daily_cate_top5
# # top5_weekly_agerated_games
