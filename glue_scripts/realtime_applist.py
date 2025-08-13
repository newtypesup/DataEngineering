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
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import sqrt, col, when, row_number

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
        parameter_info['batch_time'] = getResolvedOptions(sys.argv, ['batch_time'])['batch_time'] # 2025050102
    else:
        parameter_info['batch_time'] = KST.strftime(DATE_FORMAT)

    for key, value in parameter_info.items():
        print('> {} : {}'.format(key, value))

    time_info = parameter_info['batch_time']
    print('> batch_time : ', time_info)
    return time_info

def _extract_prev_data(time_info: str):
    print('_extract_prev_data()')
    prev_time = (datetime.strptime(time_info, DATE_FORMAT) - timedelta(hours=2)).strftime(DATE_FORMAT)
    print(f"> prev_time : {prev_time}")
    query_text = get_query('prev_data').replace('$prev_time', prev_time)
    print("==== prev_data 쿼리 ====")
    print(query_text)
    df_prev = execute_athena_query(query_text, database='newtypesup_db')
    return df_prev

def _extract_current_data(time_info: str):
    print('_extract_current_data()')
    print(f"> current_time : {time_info}")
    query_text = get_query('current_data').replace('$current_time', time_info)
    print("==== current_data 쿼리 ====")
    print(query_text)
    df_current = execute_athena_query(query_text, database='newtypesup_db')
    return df_current

def _init_spark_context():
    print('_init_spark_context()')
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    return sc, glue_context, spark

def _pandas_to_spark(spark, df_prev, df_current):
    print('_pandas_to_spark()')
    spark_prev = spark.createDataFrame(df_prev)
    spark_current = spark.createDataFrame(df_current)
    return spark_prev, spark_current

def _process_data(spark_prev, spark_current, time_info):
    print('_process_data()')
    current_date_str = datetime.strptime(time_info, DATE_FORMAT).strftime("%Y-%m-%d")
    print(f"> current_date_str: {current_date_str}")

    # reg_date 컬럼을 날짜 타입으로 변환
    spark_prev = spark_prev.withColumn("reg_date", F.to_date("reg_date"))
    spark_current = spark_current.withColumn("reg_date", F.to_date("reg_date"))

    # current_date 값을 spark_current에만 추가 (cross join)
    current_date_df = spark_current.select(F.lit(current_date_str).alias("crt_date")).limit(1)
    spark_current = spark_current.crossJoin(current_date_df)

    # spark_prev 컬럼 이름 변경 (충돌 방지)
    spark_prev_renamed = spark_prev.select(
        "ctnt_id",
        "ctnt_name",
        F.col("cate_name").alias("prev_cate_name"),
        F.col("reg_date").alias("prev_reg_date"),
        "prev_count"
    )

    # left join on ctnt_id, ctnt_name
    joined_df = spark_current.alias("cd").join(
        spark_prev_renamed.alias("pd"),
        on=["ctnt_id", "ctnt_name"],
        how="left"
    )

    # 날짜 차이 계산 (일 단위)
    joined_df = joined_df.withColumn(
        "prev_dates",
        F.datediff(col("crt_date"), col("pd.prev_reg_date"))
    )

    joined_df = joined_df.withColumn(
        "current_dates",
        F.datediff(col("crt_date"), col("cd.reg_date"))
    )

    # sqrt 계산, 음수는 0으로 처리 (sqrt는 음수 불가)
    joined_df = joined_df.withColumn(
        "sqrt_prev_date",
        sqrt(when(col("prev_dates") < 0, 0).otherwise(col("prev_dates")))
    )

    joined_df = joined_df.withColumn(
        "sqrt_current_date",
        sqrt(when(col("current_dates") < 0, 0).otherwise(col("current_dates")))
    )

    # prev_days = prev_count / sqrt_prev_date (0일때 NULL, 반올림 2자리)
    joined_df = joined_df.withColumn(
        "prev_days",
        when(col("sqrt_prev_date") == 0, None).otherwise(F.round(col("prev_count") / col("sqrt_prev_date"), 2))
    )

    # current_days = current_count / sqrt_current_date (0일때 NULL, 반올림 2자리)
    joined_df = joined_df.withColumn(
        "current_days",
        when(col("sqrt_current_date") == 0, None).otherwise(F.round(col("current_count") / col("sqrt_current_date"), 2))
    )

    # inc_index = current_days - 1 (반올림 2자리)
    joined_df = joined_df.withColumn(
        "inc_index",
        when(col("current_days").isNull(), None).otherwise(F.round(col("current_days") - 1, 2))
    )

    # diff_index = current_days - prev_days (반올림 2자리)
    joined_df = joined_df.withColumn(
        "diff_index",
        when(col("current_days").isNull() | col("prev_days").isNull(), None).otherwise(F.round(col("current_days") - col("prev_days"), 2))
    )

    # result_index = 최대값 (inc_index, diff_index)
    joined_df = joined_df.withColumn(
        "result_index",
        F.greatest(col("inc_index"), col("diff_index"))
    )

    window_spec = Window.orderBy(col("result_index").desc())

    result_df = (
                joined_df
                .withColumn("app_rank", row_number().over(window_spec))
                .filter(col("app_rank") <= 10)                                            
                .select(
                        "app_rank",
                        "ctnt_id",
                        "ctnt_name",
                        "cate_name",
                        "reg_date",
                        "crt_date",
                        "prev_count",
                        "current_count",
                        "prev_dates",
                        "current_dates",
                        "sqrt_prev_date",
                        "sqrt_current_date",
                        "prev_days",
                        "current_days",
                        "inc_index",
                        "diff_index",
                        "result_index"
                )
    )

    return result_df

def _save_to_s3(glue_context, joined_df, time_info):
    print('_save_to_s3()')

    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)

    df_single = joined_df.coalesce(1)
    print('> Ready to save to S3')
    df_single.show()

    date = datetime.strptime(time_info, DATE_FORMAT)
    output_path = f"{S3_SAVE_PATH}/year={date.year}/month={date.month}/day={date.day}/hour={date.hour}/"
    print('> Writing to ', output_path)

    df_single.write.mode('append').parquet(output_path) # parquet 타입으로 저장.
    # df_single.write.mode('append').option('header', 'true').csv(output_path) # csv 타입으로 저장.
    job.commit()

    s3 = boto3.client('s3')
    bucket_name = 'newtypesup'
    prefix = f"etl/results/realtime_applist/year={date.year}/month={date.month}/day={date.day}/hour={date.hour}/"
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    print('> S3 objects:', response)

    if 'Contents' not in response:
        print('No file found in S3')
        return

    # parquet 타입으로 저장.
    parquet_files = [
        obj for obj in response['Contents']
        if obj['Key'].endswith('.parquet') and obj['Key'].startswith(prefix + "part-")
    ]
    if not parquet_files:
        print('No part-*.parquet files found in S3')
        return

    latest_file = max(parquet_files, key=lambda x: x['LastModified'])
    latest_key = latest_file['Key']

    new_file_key = f"{prefix}{time_info}.parquet"

    original_obj = s3.get_object(Bucket=bucket_name, Key=latest_key)
    raw_data = original_obj['Body'].read()

    s3.put_object(Bucket=bucket_name, Key=new_file_key, Body=raw_data)
    s3.delete_object(Bucket=bucket_name, Key=latest_key)

    print(f"> parquet file renamed: {latest_key} → {new_file_key}")

    ## csv 타입으로 저장.
    # csv_files = [
    #     obj for obj in response['Contents']
    #     if obj['Key'].endswith('.csv') and obj['Key'].startswith(prefix + "part-")
    # ]
    # if not csv_files:
    #     print('No part-*.csv files found in S3')
    #     return

    # latest_file = max(csv_files, key=lambda x: x['LastModified'])
    # latest_key = latest_file['Key']

    # new_file_key = f"{prefix}{time_info}.csv"

    # original_obj = s3.get_object(Bucket=bucket_name, Key=latest_key)
    # raw_data = original_obj['Body'].read()
    # data_with_bom = b'\xef\xbb\xbf' + raw_data

    # s3.put_object(Bucket=bucket_name, Key=new_file_key, Body=data_with_bom)
    # s3.delete_object(Bucket=bucket_name, Key=latest_key)

    # print(f"> BOM added and file renamed: {latest_key} → {new_file_key}")

if __name__ == "__main__":
    start_time = time.time()
    step_string = ''

    try:
        step_string = 'step1. _parameter_to_dict()'
        time_info = _parameter_to_dict()

        step_string = 'step2. _extract_prev_data()'
        df_prev = _extract_prev_data(time_info)

        step_string = 'step3. _extract_current_data()'
        df_current = _extract_current_data(time_info)

        step_string = 'step4. _init_spark_context()'
        sc, glue_context, spark = _init_spark_context()

        step_string = 'step5. _pandas_to_spark()'
        spark_prev, spark_current = _pandas_to_spark(spark, df_prev, df_current)

        step_string = 'step6. _process_data()'
        joined_df = _process_data(spark_prev, spark_current, time_info)

        step_string = 'step7. _save_to_s3()'
        _save_to_s3(glue_context, joined_df, time_info)

        print(f"<< total time : {round(time.time() - start_time, 2)} sec >>")

    except Exception as e:
        print("<< error >>")
        print(f"> step_string : {step_string}")
        print(f"> error : {e}")
        sys.exit(-1)
