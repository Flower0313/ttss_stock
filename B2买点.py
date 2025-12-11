import duckdb
import pandas as pd
import numpy as np

# ========== 连接 DuckDB 并取数据 ==========
con = duckdb.connect("stocks.duckdb")

query = """
select ts_code as code,trade_date as ds,name,open as opening_price,high as highest
,low as lowest,close as closing_price,vol as deal_vol,pre_close,industry
,round((close-pre_close)*100/pre_close,2) as up_down_rate
,round((high - low)* 100 / pre_close,2) as amplitude
from (
	select a.ts_code,a.trade_date,b.name,a.open,a.high,a.low,a.close,a.vol,a.amount
	,lag(a.close,1,a.close) over(partition by a.ts_code order by a.trade_date) as pre_close
	,b.industry,b.act_ent_type
	from df_a_tu_stock_daily_df a
	join dim_a_tu_stock_info_df b on a.ts_code=b.ts_code
	join (select ts_code from df_a_tu_stock_financial_df where trade_date='2025-11-06' and circ_mv>=1000000) c on a.ts_code=c.ts_code
	where a.trade_date>='2025-05-01' and industry<>'房产服务' -- and c.circ_mv>=1000000 
) a
;
"""
df = con.execute(query).fetchdf()

# ========== 统一清洗：日期、类型、排序 ==========
# ds -> datetime & ensure sorted
if 'ds' in df.columns:
    df['ds'] = pd.to_datetime(df['ds'])
df = df.sort_values(['code', 'ds']).reset_index(drop=True)

# 强制数值列为 float
num_cols = ['opening_price', 'closing_price', 'highest', 'lowest', 'up_down_rate', 'deal_vol', 'amplitude']
for c in num_cols:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors='coerce')


# ========== 定义函数：寻找前高（修正） ==========
def find_local_highs(stock_df):
    stock_df = stock_df.copy().reset_index(drop=True)
    close = stock_df['closing_price']
    open = stock_df['opening_price']
    n = len(stock_df)
    stock_df['prev_high_date'] = pd.NaT

    stock_df['local_high'] = False
    for i in range(3, n):
        left_condition = (close.iloc[i] > close.iloc[i - 3:i]).all()
        right_condition = (close.iloc[i] > close.iloc[i + 1:]).all() if i < n - 1 else True
        bullish_condition = (close.iloc[i] > close.iloc[i - 1]) & (close.iloc[i] > open.iloc[i])

        if left_condition and right_condition and bullish_condition:
            stock_df.loc[i, 'local_high'] = True

    stock_df['prev_high_price'] = np.nan
    stock_df['prev_high_vol'] = np.nan
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


# ========== 定义函数：计算技术指标（包含稳健 KDJ） ==========
def compute_indicators(stock_df, kdj_n=9, init_kd=50.0, debug=False):
    stock_df = stock_df.copy().reset_index(drop=True)

    # ensure sorted by date
    if 'ds' in stock_df.columns:
        stock_df['ds'] = pd.to_datetime(stock_df['ds'])
        stock_df = stock_df.sort_values('ds').reset_index(drop=True)

    # 强制数值列为 float
    for c in ['closing_price', 'highest', 'lowest', 'opening_price', 'deal_vol', 'up_down_rate']:
        if c in stock_df.columns:
            stock_df[c] = pd.to_numeric(stock_df[c], errors='coerce')

    close = stock_df['closing_price'].astype(float)

    # ---------- KDJ（稳健实现） ----------
    n = int(kdj_n)
    stock_df['low_n'] = stock_df['lowest'].rolling(window=n, min_periods=1).min()
    stock_df['high_n'] = stock_df['highest'].rolling(window=n, min_periods=1).max()

    denom = stock_df['high_n'] - stock_df['low_n']
    eps = 1e-9
    # 当 denom 接近0 或为NaN 用中性值 init_kd（50）来填充 RSV，避免 NaN/inf 传播
    stock_df['RSV'] = np.where(denom.abs() <= eps,
                               float(init_kd),
                               (stock_df['closing_price'] - stock_df['low_n']) / denom * 100.0)
    # 把可能的 inf/NaN 再填充为 init_kd
    stock_df['RSV'] = stock_df['RSV'].replace([np.inf, -np.inf], np.nan).fillna(float(init_kd))

    # 递推计算 K 和 D（alpha = 1/3）
    alpha = 1.0 / 3.0
    m = len(stock_df)
    K = np.empty(m, dtype=float)
    D = np.empty(m, dtype=float)
    if m > 0:
        K[0] = float(init_kd)
        D[0] = float(init_kd)
    for i in range(1, m):
        rsv_i = float(stock_df['RSV'].iat[i])
        K[i] = (1 - alpha) * K[i - 1] + alpha * rsv_i
        # D 使用当前 K[i]
        D[i] = (1 - alpha) * D[i - 1] + alpha * K[i]

    stock_df['K'] = K
    stock_df['D'] = D
    stock_df['J'] = 3 * stock_df['K'] - 2 * stock_df['D']

    # ---------- 前高 ----------
    stock_df = find_local_highs(stock_df)

    # ---------- 白线（双 EMA(10)） ----------
    stock_df['white_line'] = close.ewm(span=10, adjust=False).mean().ewm(span=10, adjust=False).mean()

    # ---------- 黄线（四条均线的平均） ----------
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
    return stock_df


# ========== 对每个股票分组计算 ==========
df_all = df.groupby('code', group_keys=False).apply(lambda d: compute_indicators(d, debug=True))



# ========== 原筛选条件（针对9月29号） ==========
base_condition = (
    (df_all['amplitude'] <= 7) &
    #(df_all['up_down_rate'].between(-2, 1.79)) &
    (df_all['prev_high_vol'].notna()) &
    (df_all['deal_vol'] < df_all['prev_high_vol']) &
    (df_all['J'] < 13) &
    (df_all['white_line'] >= df_all['yellow_line']) &
    (df_all['closing_price'] >= df_all['yellow_line'])
)

cond_929 = base_condition & (df_all['ds'] == '2025-11-05')
codes_929 = set(df_all.loc[cond_929, 'code'])

# ========== 取9月30号的数据 ==========
df_930 = df_all[df_all['ds'] == '2025-11-06'].copy()

# 取每个股票9月29号的成交量，方便比较
prev_vol = (
    df_all[df_all['ds'] == '2025-11-05'][['code', 'deal_vol']]
    .rename(columns={'deal_vol': 'deal_vol_prev'})
)

df_930 = df_930.merge(prev_vol, on='code', how='left')

# ========== 9月30号筛选条件 ==========
cond_930 = (
    (df_930['code'].isin(codes_929)) &             # 9月29号满足原条件
    (df_930['J'] <= 57) &                           # J < 55
    (df_930['up_down_rate'] >= 3.9) &                 # 涨幅 > 4%
    (df_930['deal_vol'] >= df_930['deal_vol_prev']) & # 成交量增加
    (df_930['closing_price'] <= df_930['white_line'])
)

result = df_930.loc[cond_930, [
    'ds', 'code', 'opening_price', 'closing_price', 'up_down_rate',
    'amplitude', 'deal_vol', 'deal_vol_prev', 'prev_high_price',
    'prev_high_vol', 'prev_high_date', 'J', 'white_line', 'yellow_line','industry'
]]

# ========== 输出结果 ==========
print(result)
print(f"满足条件的股票数量：{len(result)}")

# ========== 导出 Excel ==========
output_file = "stock_signal_b2.xlsx"
result.to_excel(output_file, index=False)
print(f"筛选结果已导出到 {output_file}，共 {len(result)} 条记录。")
