import duckdb
import pandas as pd
import plotly.express as px

# 1️⃣ 连接 DuckDB
con = duckdb.connect("stocks.duckdb")

# 2️⃣ 执行 SQL 获取每周最后收盘价
query = """
WITH weekly_last AS (
    SELECT DISTINCT name, year, week_of_year,
           FIRST_VALUE(closing_price) OVER(PARTITION BY name, year, week_of_year ORDER BY ds DESC) AS last
    FROM (
        SELECT a.name, a.closing_price, a.deal_amount, a.ds, b.year, b.week_of_year
        FROM df_a_industry_sector_history_df a
        JOIN dim_a_stock_calendar_all b 
            ON a.ds = b.ds AND b.is_trade_day = TRUE
        WHERE b.year >= 2025
    ) a
)

, first_week AS (
    -- 取每个行业第一周的收盘价
    SELECT name,
           FIRST_VALUE(last) OVER(PARTITION BY name ORDER BY year, week_of_year) AS first_last,
           year,
           week_of_year,
           last
    FROM weekly_last
)

SELECT name, year, week_of_year, last,
       last / first_last AS last_norm  -- 归一化
FROM first_week
ORDER BY name, year, week_of_year;
"""
df = con.execute(query).df()

# 3️⃣ 将 year + week_of_year 转为日期（周一）
df['yw_date'] = pd.to_datetime(df['year'].astype(str) + df['week_of_year'].astype(str) + '1', format='%Y%W%w')

# 4️⃣ 绘图
fig = px.line(
    df,
    x='yw_date',
    y='last_norm',
    color='name',
    markers=True,
    title='各行业每周收盘价走势（2024年起）',
    labels={'last_norm': '收盘价', 'yw_date': '周'}
)
fig.update_layout(xaxis_title='周', yaxis_title='收盘价')
fig.show()
