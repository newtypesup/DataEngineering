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

DATE_FORMAT = "%Y%m%d%H"
S3_SAVE_PATH = "s3://newtypesup/etl/results/realtime_applist/"
KST = datetime.now(timezone.utc).astimezone(pytz.timezone('Asia/Seoul'))

def _parameter_to_dict():
    print('_parameter_to_dict()')
    argv_list = sys.argv
    parameter_info = dict()

    if ('--batch_time' in argv_list) and (getResolvedOptions(sys.argv, ['batch_time'])['batch_time'] != '$realtime'):
        parameter_info['batch_time'] = getResolvedOptions(sys.argv, ['batch_time'])['batch_time']  # 2025050102
    else:
        parameter_info['batch_time'] = KST.strftime(DATE_FORMAT)

    for key, value in parameter_info.items():
        print('> {} : {}'.format(key, value))

    date_info = parameter_info['batch_time']
    print('> batch_time : ', date_info)
    return date_info

def _extract_realtime_applist(date_info):
    print('_extract_realtime_applist()')
    metric_name = 'realtime_applist'
    prev_date_info = (datetime.strptime(date_info, DATE_FORMAT)-timedelta(hours=2)).strftime(DATE_FORMAT)
    query_text = get_query(metric_name).replace('$prev_time', prev_date_info)
    query_text = get_query(metric_name).replace('$current_time', date_info)

    data_frame = execute_athena_query(query_text, database='newtypesup_db')
    df_result = data_frame.fillna('NA')
    df_result['ctnt_id'] = df_result['ctnt_id'].astype(int)
    df_result['ctnt_name'] = df_result['ctnt_name'].astype(str)
    df_result['cate_name'] = df_result['cate_name'].astype(str)
    df_result['reg_date'] = df_result['reg_date'].astype(str)
    df_result['inc_index'] = df_result['inc_index'].astype(float)
    df_result['diff_index'] = df_result['diff_index'].astype(float)
    df_result['index_result'] = df_result['index_result'].astype(float)

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

    date = datetime.strptime(date_info, DATE_FORMAT)
    output_path = f"{S3_SAVE_PATH}/year={date.year}/month={date.month}/day={date.day}/hour={date.hour}/"
    print('> Writing to ', output_path)

    df_single.write.mode('overwrite').option('header', 'true').csv(output_path)
    job.commit()

    s3 = boto3.client('s3')
    bucket_name = 'newtypesup'
    prefix = f"etl/results/realtime_applist/year={date.year}/month={date.month}/day={date.day}/hour={date.hour}/"
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    print('> S3 objects:', response)

    part_file_key = None
    for obj in response.get('Contents', []):
        if obj['Key'].endswith('.parquet'):
            part_file_key = obj['Key']
            break

    if part_file_key:
        new_file_key = f"{prefix}{date}.parquet"

        s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': part_file_key}, Key=new_file_key)
        s3.delete_object(Bucket=bucket_name, Key=part_file_key)

        print(f"> Parquet file renamed: {part_file_key} → {new_file_key}")
    else:
        print('No Parquet file found in S3')

if __name__ == "__main__":
    start_time = time.time()
    step_string = ''

    try:
        step_string = 'step1. _parameter_to_dict()'
        date_info = _parameter_to_dict()

        step_string = 'step2. _extract_realtime_applist()'
        df_result = _extract_realtime_applist(date_info)

        step_string = 'step3. _save_to_s3()'
        _save_to_s3(df_result, date_info)

        print('<< total time : ', round(time.time() - start_time, 2), 'sec >>')

    except Exception as ex:
        print('<< error >>')
        print('> step_string : ', step_string)
        print('> error : ', ex)
        sys.exit(-1)