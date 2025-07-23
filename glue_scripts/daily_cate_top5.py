import sys
import pytz
import time
import boto3  # type: ignore
import warnings
from datetime import datetime, timedelta, timezone

from awsglue.utils import getResolvedOptions  # type: ignore
from pyspark.context import SparkContext  # type: ignore
from awsglue.context import GlueContext  # type: ignore
from awsglue.job import Job  # type: ignore

from utils.util import execute_athena_query, get_query  # type: ignore

warnings.filterwarnings("ignore")

DATE_FORMAT = "%Y-%m-%d"
S3_SAVE_PATH = "s3://newtypesup/etl/results/daily_cate_top5/"
YESTERDAY_KST = datetime.now(timezone.utc).astimezone(pytz.timezone('Asia/Seoul')) - timedelta(days=1)

def _parameter_to_dict():
    print('_parameter_to_dict()')
    argv_list = sys.argv
    parameter_info = dict()

    if ('--batch_date' in argv_list) and (getResolvedOptions(sys.argv, ['batch_date'])['batch_date'] != '$today'):
        parameter_info['batch_date'] = getResolvedOptions(sys.argv, ['batch_date'])['batch_date']  # e.g., 2025-05-01
    else:
        parameter_info['batch_date'] = YESTERDAY_KST.strftime(DATE_FORMAT)

    for key, value in parameter_info.items():
        print('> {} : {}'.format(key, value))

    date_info = parameter_info['batch_date']
    print('> batch_date : ', date_info)
    return date_info

def _extract_daily_cate_top5(date_info):
    print('_extract_daily_cate_top5()')
    metric_name = 'daily_cate_top5'
    query_text = get_query(metric_name).replace('$today', date_info)

    data_frame = execute_athena_query(query_text, database='newtypesup_db')
    df_result = data_frame.fillna('NA')
    df_result['app_rank'] = df_result['app_rank'].astype(int)
    df_result['cate_name'] = df_result['cate_name'].astype(str)
    df_result['ctnt_name'] = df_result['ctnt_name'].astype(str)
    df_result['app_count'] = df_result['app_count'].astype(int)

    print('> df_result : ', df_result.head())
    return df_result

def _save_to_s3(df_result, date_info):
    print('_save_to_s3()')

    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    gluecontext = GlueContext(sc)
    spark = gluecontext.spark_session
    job = Job(gluecontext)
    job.init(args['JOB_NAME'], args)

    df_single = spark.createDataFrame(df_result).coalesce(1)
    print('> Ready to save to S3')
    df_single.show()

    year, month, day = date_info.split('-')
    output_path = f"{S3_SAVE_PATH}/year={year}/month={month}/day={day}/"
    print('> Writing to ', output_path)

    df_single.write.mode('overwrite').option('header', 'true').csv(output_path)
    job.commit()

    s3 = boto3.client('s3')
    bucket_name = 'newtypesup'
    prefix = f"etl/results/daily_cate_top5/year={year}/month={month}/day={day}/"
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    print('> S3 objects:', response)

    part_file_key = None
    for obj in response.get('Contents', []):
        if obj['Key'].endswith('.csv'):
            part_file_key = obj['Key']
            break

    if part_file_key:
        new_file_key = f"{prefix}{date_info.replace('-', '')}.csv"

        original_obj = s3.get_object(Bucket=bucket_name, Key=part_file_key)
        raw_data = original_obj['Body'].read()
        data_with_bom = b'\xef\xbb\xbf' + raw_data

        s3.put_object(Bucket=bucket_name, Key=new_file_key, Body=data_with_bom)
        s3.delete_object(Bucket=bucket_name, Key=part_file_key)

        print(f"> BOM added and file renamed: {part_file_key} → {new_file_key}")
    else:
        print('No CSV file found in S3')

if __name__ == "__main__":
    start_time = time.time()
    step_string = ''

    try:
        step_string = 'step1. _parameter_to_dict()'
        date_info = _parameter_to_dict()

        step_string = 'step2. _extract_daily_cate_top5()'
        df_result = _extract_daily_cate_top5(date_info)

        step_string = 'step3. _save_to_s3()'
        _save_to_s3(df_result, date_info)

        print('<< total time : ', round(time.time() - start_time, 2), 'sec >>')

    except Exception as ex:
        print('<< error >>')
        print('> step_string : ', step_string)
        print('> error : ', ex)
        sys.exit(-1)