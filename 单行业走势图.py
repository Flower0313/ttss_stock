import duckdb
import pandas as pd
import plotly.express as px

# 1️⃣ 读取行业历史行情数据
con = duckdb.connect("stocks.duckdb")

query = """
SELECT
    code,              -- 行业代码
    name,              -- 行业名称
    closing_price,     -- 收盘价
    ds                 -- 交易日期
FROM df_a_industry_sector_history_df
WHERE ds BETWEEN '2025-07-01' AND '2025-11-07'
ORDER BY ds
"""

df = con.execute(query).fetchdf()

# 2️⃣ 按行业归一化（以首日收盘价为基准）
df['close_norm'] = df.groupby('code')['closing_price'].transform(
    lambda x: x / x.iloc[0]
)

# 3️⃣ 绘制行业对比曲线
fig = px.line(
    df,
    x='ds',
    y='close_norm',
    color='name',   # 每个行业一条线
    hover_data={'code': True, 'closing_price': True},
    title='2025年行业归一化收盘价走势（行业对比）'
)

fig.update_layout(
    xaxis_title='日期',
    yaxis_title='归一化收盘价（相对行业首日）',
    legend_title_text='行业',
    hovermode='x unified'
)

fig.show()
