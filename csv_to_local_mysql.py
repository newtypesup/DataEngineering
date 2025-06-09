import pandas as pd
import pymysql.cursors

def mysql_conn():
    conn = pymysql.connect(host='localhost', user='root', password='12345', db='test_db', charset='utf8')
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    df = pd.read_csv('C:\\Py\\ETL\\sample_data\\category.csv')
    # df = pd.read_csv('C:\\Py\\ETL\\sample_data\\content.csv')
    # df = pd.read_csv('C:\\Py\\ETL\\sample_data\\download.csv')

    pd.set_option('display.max_rows',None)
    pd.set_option('display.max_columns',None)
    return conn, cursor, df

def insert_data_tuples(conn, cursor, df):
    batch = 10000
    data_tuples = list(df.itertuples(index=None, name=None))

    for i in range(0, len(data_tuples), batch):
        batches = data_tuples[i:i + batch]
        print(f"{i}:{len(data_tuples)}")
        cursor.executemany(
            '''INSERT IGNORE INTO tb_category(cate_id, parent_id, cate_name, age_rating)
                VALUES (%s, %s, %s, %s)''', batches
        )
        # cursor.executemany(
        #     '''INSERT IGNORE INTO tb_content(ctnt_id, cate_id, ctnt_name, reg_date)
        #         VALUES (%s, %s, %s, %s)''', batches
        # )
        # cursor.executemany(
        #     '''INSERT IGNORE INTO tb_download(ctnt_id, cnty_cd, status, date)
        #         VALUES (%s, %s, %s, %s)''', batches
        # )

    conn.commit()
    conn.close()

if __name__=="__main__":
    try:
        conn, cursor, df = mysql_conn()
        insert_data_tuples(conn, cursor, df)
    except Exception as ex:
        print(ex)

#----------------
#데이터 배치