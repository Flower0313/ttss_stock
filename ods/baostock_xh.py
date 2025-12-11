import random
import time
from datetime import datetime

import duckdb
import pandas as pd
import baostock as bs
#CREATE TABLE df_a_bao_stock_daily_df(date VARCHAR, code VARCHAR, open DOUBLE, high DOUBLE, low DOUBLE, "close" DOUBLE, preclose DOUBLE, volume DOUBLE, amount DOUBLE, turn DOUBLE);

def baostock_info_all(con,stock_code,start_date,end_date):
    print(f"正在获取 {stock_code} 的数据...")
    rs = bs.query_history_k_data_plus(
        stock_code,
        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn",
        start_date=start_date, end_date=end_date,
        frequency="d", adjustflag="2"
    )

    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())

    if not data_list:
        print(f"{stock_code} 没有获取到数据，跳过")
        return

    df = pd.DataFrame(data_list, columns=rs.fields)

    #对齐字段名，保证顺序正确插入
    # 只选择接口返回的字段
    insert_cols = [
        'date', 'code', 'open', 'high', 'low', 'close', 'preclose','volume', 'amount', 'turn'
    ]

    for col in insert_cols:
        if col not in df.columns:
            df[col] = None  # 没有的数据填 None

    # 字段顺序重排
    df = df[insert_cols]
    # 类型转换
    numeric_cols = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 插入 DuckDB
    con.from_df(df).insert_into('df_a_bao_stock_daily_df')
    con.commit()


# 获取历史数据
def get_all_stock(con,switch):
    sql='''
    SELECT distinct lower(split_part(ts_code, '.', 2)) || '.' || split_part(ts_code, '.', 1) as code 
    FROM dim_a_tu_stock_info_df
    where market='主板';
    '''

    codes_df = con.execute(sql).fetchdf()
    codes_list = codes_df['code'].tolist()  # 转为列表
    for stock_code in codes_list:
        time.sleep(random.uniform(0.5, 2.5))
        if switch:
            baostock_info_all(con,stock_code,'2000-01-01','2025-12-31')
        else:
            baostock_info_all(con, stock_code,datetime.today().strftime('%Y-%m-%d'),datetime.today().strftime('%Y-%m-%d')) #


if __name__ == '__main__':
    # 登录 baostock
    con = duckdb.connect("../stocks.duckdb")
    lg = bs.login()
    print('login respond error_code:' + lg.error_code)
    print('login respond error_msg:' + lg.error_msg)

    # 获取历史数据 每周执行一次
    #get_all_stock(con,True)

    # 获取当日数据 (当日交易数据17:30才有，所以18:00抓比较靠谱)
    get_all_stock(con,False)

    # 登出 baostock
    bs.logout()
    print("全部完成！")