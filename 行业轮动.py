import duckdb
import pandas as pd
import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = ['SimHei']    # 黑体，最常用
plt.rcParams['axes.unicode_minus'] = False      # 解决负号显示问题

# 1. 读取数据
con = duckdb.connect("stocks.duckdb")
df = con.execute("""
    select distinct name, ds, closing_price
    from df_a_industry_sector_history_df
    where name in ('软件开发','通信服务') 
      and ds >= '2025-01-01' and ds<='2025-12-30'
    order by ds
""").df()

# 2. 日期处理
df['ds'] = pd.to_datetime(df['ds'])

# 3. 使用 pivot_table 处理重复数据
pivot_df = df.pivot_table(
    index='ds',
    columns='name',
    values='closing_price',
    aggfunc='mean'
)

# 4. ====== 归一化处理 ======
# 让每个行业都从1开始
norm_df = pivot_df / pivot_df.iloc[0]

# 5. 画图
plt.figure(figsize=(10,5))

plt.plot(norm_df.index, norm_df['软件开发'], label='软件开发（归一化）')
plt.plot(norm_df.index, norm_df['通信服务'], label='通信服务（归一化）')

plt.title("行业走势对比（归一化）")
plt.xlabel("日期")
plt.ylabel("指数化价格（起点=1）")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()
