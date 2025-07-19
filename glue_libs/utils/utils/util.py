import boto3 #type: ignore
import time
import pandas as pd
import io

def get_query(metric_name, table_name='etl_sql', region='ap-northeast-2'):
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)
    response = table.get_item(Key={'query_id': metric_name})
    if 'Item' not in response:
        raise ValueError(f"No query found for metric: {metric_name}")
    return response['Item']['query']

def execute_athena_query(query, database, region='ap-northeast-2',
                         output_location='s3://newtypesup/etl/athena-results/temp/'):
    athena_client = boto3.client('athena', region_name=region)
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output_location}
    )

    query_execution_id = response['QueryExecutionId']
    
    while True:
        result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = result['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(3)

    if state != 'SUCCEEDED':
        raise Exception(f"Athena query failed: {result['QueryExecution']['Status']['StateChangeReason']}")

    result_response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    columns = [col['Label'] for col in result_response['ResultSet']['ResultSetMetadata']['ColumnInfo']]

    rows = result_response['ResultSet']['Rows'][1:]
    data = [[field.get('VarCharValue', '') for field in row['Data']] for row in rows]

    return pd.DataFrame(data, columns=columns)
