import random
import time
import akshare as ak
import pandas as pd
import tushare as ts
import duckdb

pd.set_option('display.max_columns', None)  # 显示所有列
pd.set_option('display.max_rows', None)  # 显示所有行（注意太大可能卡）
pd.set_option('display.width', 1000)  # 设置显示宽度，防止自动换行
from datetime import datetime


# ak 新浪
def inlang():
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot()
    return stock_zh_a_spot_em_df


# ak 东财
def dongcai():
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    return stock_zh_a_spot_em_df


# ak 雪球
def snow():
    stock_individual_spot_xq_df = ak.stock_individual_spot_xq(symbol="SH600000")
    return stock_individual_spot_xq_df


# tushare
def tushare_today_all():
    # ts_code   name    pre_close   high    open    low close   vol amount  num
    ts.set_token('0d46c0471fac9d57ace59a6c4b2b7498305a0eb52c6cd382c4fdbf7e')
    pro = ts.pro_api()
    df = pro.rt_k(ts_code='3*.SZ,6*.SH,0*.SZ,9*.BJ')
    return df


# tushare 历史日线
def tushare_history_all(ds):
    ts.set_token('0d46c0471fac9d57ace59a6c4b2b7498305a0eb52c6cd382c4fdbf7e')
    pro = ts.pro_api()
    # 获取每日行情
    df = pro.daily(trade_date=ds)
    if df.empty:
        return df
    # 字段映射 → 对应到 DuckDB 表字段
    # DuckDB 表: code,date,open,high,low,close,preclose,change,pctchg,volume,amount
    df = df.rename(columns={
        'ts_code': 'code',
        'trade_date': 'date',
        'pct_chg': 'pctchg',
        'pre_close': 'preclose',
        'vol': 'volume'
    })
    # 转换日期格式 YYYYMMDD → YYYY-MM-DD
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

    # 保留需要的列，顺序严格对应 DuckDB 表
    insert_cols = ['code', 'date', 'open', 'high', 'low', 'close', 'preclose','change', 'pctchg', 'volume', 'amount']

    # 如果某列不存在，补空值
    for col in insert_cols:
        if col not in df.columns:
            df[col] = None

    df = df[insert_cols]

    # 类型转换
    numeric_cols = ['open', 'high', 'low', 'close', 'preclose', 'change', 'pctchg', 'volume', 'amount']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


# tushare 最新股票基本信息
def tushare_info_all():
    # CREATE TABLE IF NOT EXISTS dim_a_tu_stock_info_df (
    #   ts_code VARCHAR,        -- TS代码
    #   symbol VARCHAR,         -- 股票代码
    #   name VARCHAR,           -- 股票名称
    #   area VARCHAR,           -- 地域
    #   industry VARCHAR,       -- 所属行业
    #   fullname VARCHAR,                -- 股票全称
    #   enname VARCHAR,                  -- 英文全称
    #   cnspell VARCHAR,        -- 拼音缩写
    #   market VARCHAR,         -- 市场类型（主板/创业板/科创板/CDR）
    #   exchange VARCHAR,                -- 交易所代码
    #   list_date VARCHAR,         -- 上市日期
    #   is_hs VARCHAR,                   -- 是否沪深港通标的，N否 H沪股通 S深股通
    #   act_name VARCHAR,       -- 实控人名称
    #   act_ent_type VARCHAR    -- 实控人企业性质
    # );
    ts.set_token(
        '89a1bf5713d021ea5c443643eade1fe340a529ce36c01a27b2f2aa78')  # 0d46c0471fac9d57ace59a6c4b2b7498305a0eb52c6cd382c4fdbf7e
    pro = ts.pro_api()
    df = pro.stock_basic(exchange='', list_status='L',
                         fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,list_date,is_hs,act_name,act_ent_type')
    if df.empty:
        return df

    df = df.rename(columns={
        'ts_code': 'ts_code',
        'symbol': 'symbol',
        'name': 'name',
        'area': 'area',
        'industry': 'industry',
        'fullname': 'fullname',
        'enname': 'enname',
        'cnspell': 'cnspell',
        'market': 'market',
        'exchange': 'exchange',
        'list_date': 'list_date',
        'is_hs': 'is_hs',
        'act_name': 'act_name',
        'act_ent_type': 'act_ent_type'
    })

    insert_to_duckdb(df, "dim_a_tu_stock_info_df", 1)
    print("执行完毕")


def tushare_financial_all():
    # CREATE TABLE df_a_tu_stock_financial_df (
    # ts_code VARCHAR,                    -- TS股票代码
    # trade_date VARCHAR,                 -- 交易日期
    # close DOUBLE,                       -- 当日收盘价
    # turnover_rate DOUBLE,               -- 换手率（%）
    # turnover_rate_f DOUBLE,             -- 换手率（自由流通股）
    # volume_ratio DOUBLE,                -- 量比
    # pe DOUBLE,                          -- 市盈率（总市值/净利润，亏损的PE为空）
    # pe_ttm DOUBLE,                      -- 市盈率（TTM，亏损的PE为空）
    # pb DOUBLE,                          -- 市净率（总市值/净资产）
    # ps DOUBLE,                          -- 市销率
    # ps_ttm DOUBLE,                      -- 市销率（TTM）
    # dv_ratio DOUBLE,                    -- 股息率（%）
    # dv_ttm DOUBLE,                      -- 股息率（TTM）（%）
    # total_share DOUBLE,                 -- 总股本（万股）
    # float_share DOUBLE,                 -- 流通股本（万股）
    # free_share DOUBLE,                  -- 自由流通股本（万股）
    # total_mv DOUBLE,                    -- 总市值（万元）
    # circ_mv DOUBLE                      -- 流通市值（万元）
    # );

    ts.set_token('0d46c0471fac9d57ace59a6c4b2b7498305a0eb52c6cd382c4fdbf7e')
    pro = ts.pro_api()
    df = pro.query('daily_basic', ts_code='', trade_date=datetime.today().strftime('%Y%m%d'),
                   fields='ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv')
    if df.empty:
        return df

    df = df.rename(columns={
        'ts_code': 'ts_code',
        'trade_date': 'trade_date',
        'close': 'close',
        'turnover_rate': 'turnover_rate',
        'turnover_rate_f': 'turnover_rate_f',
        'volume_ratio': 'volume_ratio',
        'pe': 'pe',
        'pe_ttm': 'pe_ttm',
        'pb': 'pb',
        'ps': 'ps',
        'ps_ttm': 'ps_ttm',
        'dv_ratio': 'dv_ratio',
        'dv_ttm': 'dv_ttm',
        'total_share': 'total_share',
        'float_share': 'float_share',
        'free_share': 'free_share',
        'total_mv': 'total_mv',
        'circ_mv': 'circ_mv'
    })

    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
    insert_to_duckdb(df, "df_a_tu_stock_financial_df")
    print("df_a_tu_stock_financial_df 执行完毕")


def trade_day():
    con = duckdb.connect("../stocks.duckdb")
    query = """
    SELECT STRFTIME(ds, '%Y%m%d') ds
    FROM dim_a_stock_calendar_all
    WHERE is_trade_day = true order by ds
    """
    years = con.execute(query).fetchall()
    return years

def sh_index(ds):
    pro = ts.pro_api()
    ts.set_token('0d46c0471fac9d57ace59a6c4b2b7498305a0eb52c6cd382c4fdbf7e')
    df = pro.index_daily(ts_code='000001.SH', start_date=ds, end_date=ds)
    if df.empty:
        return df
    df = df.rename(columns={
        'ts_code': 'ts_code',
        'trade_date': 'trade_date',
        'open': 'open',
        'high': 'high',
        'low': 'low',
        'close': 'close',
        'pre_close': 'pre_close',
        'change': 'change',
        'pct_chg': 'pct_chg',
        'vol': 'vol',
        'amount': 'amount'
    })
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
    df2 = df[
        ['ts_code', 'trade_date', 'open', 'high', 'low', 'close',
         'pre_close', 'change', 'pct_chg', 'vol', 'amount']
    ]

    return df2

# ========== 插入到 DuckDB 表 ==========
def insert_to_duckdb(df: pd.DataFrame, table_name, is_del: int = 0):
    if df.empty:
        return
    con = duckdb.connect("../stocks.duckdb")

    if is_del == 1:
        print(f"清空表 {table_name} ...")
        con.execute(f"DELETE FROM {table_name}")

    con.register("tmp_df", df)  # 把 DataFrame 注册为临时表
    con.execute(f"INSERT INTO {table_name} SELECT * FROM tmp_df")
    con.unregister("tmp_df")  # 可选：注销临时表


if __name__ == '__main__':
    # 补历史数据
    # for (ds,) in trade_day():
    #     try:
    #         print(f"抓取并插入交易日 {ds}")
    #         df = tushare_history_all(ds)
    #         insert_to_duckdb(df,"df_a_tu_stock_daily_df")
    #         time.sleep(random.uniform(0.8, 3.5))
    #     except Exception as e:
    #         print(f"[ERROR] 处理交易日 {ds} 出错: {e}")
    #         continue

    # 抓最新股票的基本数据
    tushare_info_all()

    # 抓取股票当天的财务数据
    tushare_financial_all()

    # 抓当天行情数据
    df = tushare_history_all(datetime.today().strftime('%Y%m%d'))
    insert_to_duckdb(df, "df_a_tu_qfq_stock_daily_df")
    print("当天行情插入成功")

    # 上证指数
    #df = sh_index(datetime.today().strftime('%Y%m%d'))
    #insert_to_duckdb(df, "df_a_tu_stock_daily_df")
    #print("当天行情插入成功")

