import duckdb
import pandas as pd
import numpy as np

# ========== 连接 DuckDB 并取数据 ==========
con = duckdb.connect("stocks.duckdb")
con.execute("PRAGMA max_temp_directory_size='100GB';")

query = """
select code, ds, name, opening_price, highest, lowest, closing_price, vol as deal_vol, industry, up_down_rate, amplitude
from (
    select a.code,
           a.date ds,
           b.name,
           a.open opening_price,
           a.high highest,
           a.low lowest, 
           a.close closing_price,
           a.volume as vol,
           a.amount as amount,
           a.pctchg up_down_rate,
           0 as amplitude,
           b.industry,
           b.act_ent_type
    from df_a_tu_qfq_stock_daily_df a
    join dim_a_tu_stock_info_df b on a.code = b.ts_code
    join (select ts_code from df_a_tu_stock_financial_df where trade_date='2025-12-11' and circ_mv>=600000) c
         on a.code = c.ts_code
    where a.date >= '2024-04-01' and b.name not like '%ST%'
) a;
"""

df = con.execute(query).fetchdf()

# ========== 预处理：类型、排序、去重 ==========
df['ds'] = pd.to_datetime(df['ds'], errors='coerce')
df = df[~df['ds'].isna()].copy()
df = df.sort_values(['code', 'ds']).drop_duplicates(subset=['code', 'ds'], keep='last')
df = df.sort_values(['code', 'ds']).reset_index(drop=True)

# ========== 定义指标计算函数 ==========
def compute_indicators(stock_df):

    stock_df = stock_df.sort_values('ds').reset_index(drop=True).copy()

    # 转数值
    for col in ['closing_price', 'highest', 'lowest', 'deal_vol', 'opening_price']:
        stock_df[col] = pd.to_numeric(stock_df[col], errors='coerce')

    stock_df['up_down_rate'] = pd.to_numeric(stock_df.get('up_down_rate', pd.Series(dtype=float)))
    stock_df['amplitude'] = pd.to_numeric(stock_df.get('amplitude', pd.Series(dtype=float)))

    close = stock_df['closing_price'].ffill().fillna(0)

    # ---------- MA 均线 ----------
    stock_df['ma5'] = close.rolling(5, min_periods=1).mean()
    stock_df['ma10'] = close.rolling(10, min_periods=1).mean()
    stock_df['ma60'] = close.rolling(60, min_periods=1).mean()

    # ---------- MACD ----------
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    stock_df['dif'] = ema12 - ema26
    stock_df['dea'] = stock_df['dif'].ewm(span=9, adjust=False).mean()
    stock_df['macd'] = 2 * (stock_df['dif'] - stock_df['dea'])

    # ---------- KDJ ----------
    n = 9
    stock_df['low_n'] = stock_df['lowest'].rolling(n, min_periods=1).min()
    stock_df['high_n'] = stock_df['highest'].rolling(n, min_periods=1).max()
    denom = stock_df['high_n'] - stock_df['low_n']

    stock_df['RSV'] = np.where(denom == 0, 50.0,
                               (close - stock_df['low_n']) / denom * 100)

    stock_df['K'] = 50.0
    stock_df['D'] = 50.0
    for i in range(1, len(stock_df)):
        prev_k = stock_df.at[i-1, 'K']
        prev_d = stock_df.at[i-1, 'D']
        rsv_i = stock_df.at[i, 'RSV']
        k_i = prev_k * 2/3 + rsv_i / 3
        d_i = prev_d * 2/3 + k_i / 3
        stock_df.at[i, 'K'] = k_i
        stock_df.at[i, 'D'] = d_i
    stock_df['J'] = 3 * stock_df['K'] - 2 * stock_df['D']

    # ---------- 白线 ----------
    ema10 = close.ewm(span=10, adjust=False).mean()
    stock_df['white'] = ema10.ewm(span=10, adjust=False).mean()

    # ---------- 黄线 ----------
    ma1 = close.rolling(14, min_periods=1).mean()
    ma2 = close.rolling(28, min_periods=1).mean()
    ma3 = close.rolling(57, min_periods=1).mean()
    ma4 = close.rolling(114, min_periods=1).mean()
    stock_df['yellow'] = (ma1 + ma2 + ma3 + ma4) / 4

    # ---------- BBI ----------
    ma_bbi1 = close.rolling(3, min_periods=1).mean()
    ma_bbi2 = close.rolling(6, min_periods=1).mean()
    ma_bbi3 = close.rolling(12, min_periods=1).mean()
    ma_bbi4 = close.rolling(24, min_periods=1).mean()
    stock_df['bbi'] = (ma_bbi1 + ma_bbi2 + ma_bbi3 + ma_bbi4) / 4

    # ---------- 成交量 RSV ----------
    vol = stock_df['deal_vol'].fillna(0).astype(float)
    stock_df['vol_min'] = vol.rolling(n, min_periods=1).min()
    stock_df['vol_max'] = vol.rolling(n, min_periods=1).max()
    denom_v = stock_df['vol_max'] - stock_df['vol_min']

    stock_df['vol_RSV'] = np.where(denom_v == 0, 50.0,
                                   (vol - stock_df['vol_min']) / denom_v * 100)

    # ——★ 将 open / high / low 返回 ——★
    return stock_df[['code', 'name',
                     'opening_price', 'highest', 'lowest',
                     'closing_price', 'up_down_rate', 'amplitude', 'ds',
                     'deal_vol',
                     'ma5', 'ma10', 'ma60', 'K', 'D', 'J',
                     'bbi', 'white', 'yellow',
                     'industry', 'dif', 'dea', 'macd', 'vol_RSV'
                    ]]

# ========== 计算全量指标 ==========
df_all = df.groupby(['code', 'name'], group_keys=False).apply(compute_indicators).reset_index(drop=True)

# ========== 创建结果表 ==========
create_sql = """
CREATE TABLE IF NOT EXISTS df_a_stock_technical_index_df (
    code VARCHAR,
    name VARCHAR,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    vol DOUBLE,
    up_down_rate DOUBLE,
    amplitude DOUBLE,
    ma5 DOUBLE,
    ma10 DOUBLE,
    ma60 DOUBLE,
    k DOUBLE,
    d DOUBLE,
    j DOUBLE,
    bbi DOUBLE,
    white DOUBLE,
    yellow DOUBLE,
    dif DOUBLE,
    dea DOUBLE,
    macd DOUBLE,
    vol_rsv DOUBLE,
    industry VARCHAR,
    ds DATE
);
"""
con.execute(create_sql)

con.register("temp_df", df_all)

con.execute("DELETE FROM df_a_stock_technical_index_df")

# ========== 插入 ==========
con.execute("""
INSERT INTO df_a_stock_technical_index_df
SELECT 
    code,
    name,
    opening_price AS open,
    highest AS high,
    lowest AS low,
    closing_price AS close,
    deal_vol AS vol,
    up_down_rate,
    amplitude,
    ma5, ma10, ma60,
    K AS k, D AS d, J AS j,
    bbi, white, yellow,
    dif, dea, macd, vol_RSV AS vol_rsv,
    industry,
    ds
FROM temp_df;
""")

con.commit()
print("技术指标已成功写入 df_a_stock_technical_index_df 表！")
