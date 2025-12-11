import duckdb
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from datetime import datetime

# ===== 连接 DuckDB =====
con = duckdb.connect("../stocks.duckdb")

# ===== 目标日期（自动取今天）=====
today = datetime.now().strftime("%Y-%m-%d")
print("当前日期:", today)

# ===== 保存目录 =====
save_dir = f"D:/{today}/B2"
if not os.path.exists(save_dir):
    os.makedirs(save_dir)
    print("创建目录:", save_dir)
else:
    print("目录已存在:", save_dir)

# ===== 查询符合条件的股票代码 =====
sql_codes = """
select distinct code
from df_a_stock_technical_index_df 
-- B1
where (ds='2025-12-10' and j<=13 and white>=yellow and close>=yellow)
-- B2
or (ds='2025-12-11' and j<=57 and white>=yellow and close>=yellow and up_down_rate>=3.9)
group by code,name,industry having count(*)>1
;
"""
codes = con.execute(sql_codes).fetchdf()

if codes.empty:
    print("没有符合条件的股票")
    raise SystemExit()

print("共找到", len(codes), "只股票")


# ===========================
#     批量生成图表部分
# ===========================
for code in codes['code']:

    print("生成图表:", code)

    sql_data = f"""
        SELECT
            ds AS date,
            open, high, low, close,
            white, yellow, bbi,
            vol AS volume,
            name, industry
        FROM df_a_stock_technical_index_df
        WHERE code = '{code}'
        ORDER BY ds DESC
        LIMIT 45
    """
    df = con.execute(sql_data).fetchdf()

    if df.empty:
        print(code, "没有数据，跳过")
        continue

    # ===== 预处理 =====
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    df['date_str'] = df['date'].dt.strftime("%Y-%m-%d")

    df['up'] = df['close'] > df['open']
    df['down'] = df['close'] <= df['open']

    volume_colors = df.apply(
        lambda row: 'red' if row['close'] > row['open'] else 'green',
        axis=1
    )

    # ===== 创建子图结构 =====
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.7, 0.3]
    )

    # ===== K线：上涨（红色空心）=====
    fig.add_trace(go.Candlestick(
        x=df['date_str'],
        open=df['open'].where(df['up']),
        high=df['high'].where(df['up']),
        low=df['low'].where(df['up']),
        close=df['close'].where(df['up']),
        increasing_line_color='red',
        increasing_fillcolor='rgba(0,0,0,0)',
        showlegend=False
    ), row=1, col=1)

    # ===== K线：下跌（绿色实心）=====
    fig.add_trace(go.Candlestick(
        x=df['date_str'],
        open=df['open'].where(df['down']),
        high=df['high'].where(df['down']),
        low=df['low'].where(df['down']),
        close=df['close'].where(df['down']),
        decreasing_line_color='green',
        decreasing_fillcolor='green',  # 实心
        showlegend=False
    ), row=1, col=1)

    # ===== 白线、黄线、BBI =====
    fig.add_trace(go.Scatter(
        x=df['date_str'], y=df['white'],
        line=dict(width=1, color='white'),
        name="短期线"
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=df['date_str'], y=df['yellow'],
        line=dict(width=1, color='gold'),
        name="大哥线"
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=df['date_str'], y=df['bbi'],
        line=dict(width=1, color='blue'),
        name="BBI"
    ), row=1, col=1)

    # ===== 成交量：颜色跟随K线涨跌 =====
    fig.add_trace(go.Bar(
        x=df['date_str'],
        y=df['volume'],
        marker_color=volume_colors,
        showlegend=False
    ), row=2, col=1)

    # ===== 标题信息 =====
    stock_name = df.iloc[-1]['name']
    industry = df.iloc[-1]['industry']

    fig.update_layout(
        title=f"{stock_name}（{code}）<br>{industry}",
        title_x=0.1,
        title_font=dict(size=18),
        template="plotly_dark",
        xaxis_rangeslider_visible=False,
        width=900,
        height=500
    )

    fig.update_xaxes(
        type='category',
        showticklabels=False  # 隐藏横坐标日期
    )

    # ===== 保存为 PNG =====
    save_path = f"{save_dir}/{code}.png"
    fig.write_image(save_path, scale=2)
    print("已保存:", save_path)

print("全部完成！")
