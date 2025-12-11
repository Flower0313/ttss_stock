import duckdb
import pandas as pd
import numpy as np

# ========== 参数 ==========
N1 = 3    # 你给的短期参数
N2 = 21   # 你给的长期参数

# ========== 连接 DuckDB 并取数据 ==========
con = duckdb.connect("stocks.duckdb")

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
    join (select ts_code from df_a_tu_stock_financial_df where trade_date='2025-12-05' and circ_mv>=600000) c
         on a.code = c.ts_code
    where a.date >= '2024-04-01' and b.name not like '%ST%'
) a;
"""
df = con.execute(query).fetchdf()

# ========== 统一清洗：日期、类型、排序 ==========
if 'ds' in df.columns:
    df['ds'] = pd.to_datetime(df['ds'])
df = df.sort_values(['code', 'ds']).reset_index(drop=True)

num_cols = ['opening_price', 'closing_price', 'highest', 'lowest', 'up_down_rate', 'deal_vol', 'amplitude']
for c in num_cols:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors='coerce')


# ========== 辅助：寻找前高 ==========
def find_local_highs(stock_df):
    stock_df = stock_df.copy().reset_index(drop=True)
    close = stock_df['closing_price']
    openp = stock_df['opening_price']
    n = len(stock_df)
    stock_df['local_high'] = False

    for i in range(3, n):
        left_condition = (close.iloc[i] > close.iloc[i - 3:i]).all()
        right_condition = (close.iloc[i] > close.iloc[i + 1:]).all() if i < n - 1 else True
        bullish_condition = (close.iloc[i] > close.iloc[i - 1]) & (close.iloc[i] > openp.iloc[i])
        if left_condition and right_condition and bullish_condition:
            stock_df.loc[i, 'local_high'] = True

    stock_df['prev_high_price'] = np.nan
    stock_df['prev_high_vol'] = np.nan
    stock_df['prev_high_date'] = pd.NaT
    last_high_price = np.nan
    last_high_vol = np.nan
    last_high_date = pd.NaT

    for i in range(n):
        if stock_df.loc[i, 'local_high']:
            last_high_price = stock_df.loc[i, 'closing_price']
            last_high_vol = stock_df.loc[i, 'deal_vol']
            last_high_date = stock_df.loc[i, 'ds']
        stock_df.loc[i, 'prev_high_price'] = last_high_price
        stock_df.loc[i, 'prev_high_vol'] = last_high_vol
        stock_df.loc[i, 'prev_high_date'] = last_high_date

    return stock_df

def calc_tdx_rsv(stock_df, n):
    stock_df = stock_df.copy().sort_values('ds').reset_index(drop=True)
    lows = stock_df['lowest']
    closes = stock_df['closing_price']

    llv = lows.rolling(window=n, min_periods=1).min()    # 包含当前K线 ✅
    hhv = closes.rolling(window=n, min_periods=1).max()  # 包含当前K线 ✅

    rsv = 100 * (closes - llv) / (hhv - llv)
    rsv = rsv.replace([np.inf, -np.inf], np.nan).fillna(50).clip(0, 100)
    return rsv

# ========== 指标计算 ==========
def compute_indicators(stock_df, kdj_n=9, init_kd=50.0, n1=N1, n2=N2, debug=False):
    stock_df = stock_df.copy().reset_index(drop=True)
    if 'ds' in stock_df.columns:
        stock_df['ds'] = pd.to_datetime(stock_df['ds'])
    stock_df = stock_df.sort_values('ds').reset_index(drop=True)

    # 强制数值列
    for c in ['closing_price', 'highest', 'lowest', 'opening_price', 'deal_vol', 'up_down_rate']:
        if c in stock_df.columns:
            stock_df[c] = pd.to_numeric(stock_df[c], errors='coerce')

    close = stock_df['closing_price'].astype(float)
    low = stock_df['lowest'].astype(float)
    high = stock_df['highest'].astype(float)

    # ---------- KDJ（稳健递推） ----------
    n = int(kdj_n)
    stock_df['low_n'] = low.rolling(window=n, min_periods=1).min()
    stock_df['high_n'] = high.rolling(window=n, min_periods=1).max()

    denom = stock_df['high_n'] - stock_df['low_n']
    eps = 1e-9
    stock_df['RSV'] = np.where(denom.abs() <= eps, float(init_kd),
                               (close - stock_df['low_n']) / denom * 100.0)
    stock_df['RSV'] = pd.Series(stock_df['RSV']).replace([np.inf, -np.inf], np.nan).fillna(float(init_kd)).values

    alpha = 1.0 / 3.0
    m = len(stock_df)
    K = np.full(m, float(init_kd))
    D = np.full(m, float(init_kd))
    for i in range(1, m):
        rsv_i = float(stock_df['RSV'].iat[i])
        K[i] = (1 - alpha) * K[i - 1] + alpha * rsv_i
        D[i] = (1 - alpha) * D[i - 1] + alpha * K[i]
    stock_df['K'], stock_df['D'], stock_df['J'] = K, D, 3 * K - 2 * D

    # ---------- 前高 ----------
    stock_df = find_local_highs(stock_df)

    # ---------- 白线（双 EMA(10)） ----------
    stock_df['white_line'] = close.ewm(span=10, adjust=False).mean().ewm(span=10, adjust=False).mean()

    # ---------- 黄线（你原先定义的均线组平均） ----------
    ma1 = close.rolling(14, min_periods=1).mean()
    ma2 = close.rolling(28, min_periods=1).mean()
    ma3 = close.rolling(57, min_periods=1).mean()
    ma4 = close.rolling(114, min_periods=1).mean()
    stock_df['yellow_line'] = (ma1 + ma2 + ma3 + ma4) / 4.0

    # ---------- BBI ----------
    ma_bbi1 = close.rolling(3, min_periods=1).mean()
    ma_bbi2 = close.rolling(6, min_periods=1).mean()
    ma_bbi3 = close.rolling(12, min_periods=1).mean()
    ma_bbi4 = close.rolling(24, min_periods=1).mean()
    stock_df['BBI'] = (ma_bbi1 + ma_bbi2 + ma_bbi3 + ma_bbi4) / 4.0

    # ---------- 通达信多周期 RSV 指标（按照你给的公式） ----------

    stock_df['rsv_short'] = calc_tdx_rsv(stock_df, 3)
    stock_df['rsv_long'] = calc_tdx_rsv(stock_df, 21)

    return stock_df


# ========== 对每个股票分组计算 ==========
df_all = df.groupby('code', group_keys=False).apply(lambda d: compute_indicators(d, debug=True))

# ========== 最终筛选（含你要求的新条件：短线 <20，黄线(中期 RSV=10) >80） ==========
# 注意：这里把“黄线在80以上”理解为“中期 RSV(10) > 80”
target_date = pd.to_datetime('2025-12-04')

result = df_all[
    (df_all['rsv_short'] <= 20) &          # 短期 RSV(N1=3) < 20
    (df_all['rsv_long'] >= 80) &            # 中期 RSV(10) > 80（即你口中的“黄线>80”）
    (df_all['ds'] == target_date)
][[
    'ds', 'code', 'opening_price', 'closing_price', 'up_down_rate',
    'amplitude', 'deal_vol', 'prev_high_price', 'prev_high_vol', 'prev_high_date',
    'J', 'white_line', 'yellow_line', 'rsv_short', 'rsv_long','industry'
]]

# ========== 输出和导出 ==========
print(result)
output_file = "stock_signal_b1_with_rsv.xlsx"
result.to_excel(output_file, index=False)
print(f"✅ 筛选结果已导出到 {output_file}，共 {len(result)} 条记录。")
