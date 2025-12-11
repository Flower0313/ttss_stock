# _*_ coding: utf-8 _*_
# @Author : Holden
# @File   : get_a_stock_kline.py
# @Desc   : 使用 AkShare 并发获取 A 股日K行情数据并写入 DuckDB

import akshare as ak
import pandas as pd
from datetime import datetime
import duckdb
from concurrent.futures import ThreadPoolExecutor, as_completed

# DuckDB 连接（写入阶段只使用主线程）
con = duckdb.connect("stocks.duckdb")


def get_a_stock_kline(stock_code: str,
                      start_date: str = None,
                      end_date: str = None,
                      adjust: str = "qfq") -> pd.DataFrame:
    """获取单只股票的日K线"""
    if stock_code.startswith(("6", "9")):
        market = "sh"
    elif stock_code.startswith(("0", "3")):
        market = "sz"
    else:
        raise ValueError(f"无法识别市场类型: {stock_code}")

    symbol = f"{market}{stock_code}"
    df = ak.stock_zh_a_daily(symbol=symbol, adjust=adjust)

    df["date"] = pd.to_datetime(df["date"])
    df = df.rename(columns={
        "date": "trade_date",
        "open": "opening_price",
        "close": "closing_price",
        "high": "highest",
        "low": "lowest",
        "volume": "deal_vol",
        "amount": "deal_amount"
    })
    df["up_down_rate"] = df["closing_price"].pct_change() * 100
    df["up_down_amount"] = df["closing_price"] - df["closing_price"].shift(1)
    df["amplitude"] = (df["highest"] - df["lowest"]) / df["closing_price"].shift(1) * 100

    # 日期过滤
    if start_date:
        df = df[df["trade_date"] >= pd.Timestamp(start_date)]
    if end_date:
        df = df[df["trade_date"] <= pd.Timestamp(end_date)]

    df = df.reset_index(drop=True)
    return df


def fetch_and_build_record(row, target_ds):
    """获取单支股票数据并转换为插表记录"""
    market, code, name = row["market"], row["code"], row["name"]
    try:
        df_history = get_a_stock_kline(code, start_date=target_ds.replace("-", ""), end_date=target_ds.replace("-", ""))
        if df_history.empty:
            return None

        df_row = df_history.iloc[-1]  # 最新一行
        record = (
            market,
            code,
            name,
            float(df_row.get("up_down_rate", 0) or 0),
            float(df_row.get("up_down_amount", 0) or 0),
            float(df_row.get("turnover", 0) * 100 or 0),
            float(df_row.get("amplitude", 0) or 0),
            float(df_row.get("highest", 0) or 0),
            float(df_row.get("lowest", 0) or 0),
            float(df_row.get("opening_price", 0) or 0),
            float(df_row.get("closing_price", 0) or 0),
            float(df_row.get("deal_amount", 0) or 0),
            float(df_row.get("deal_vol", 0) or 0),
            target_ds
        )
        print(f"✅ 获取成功: {name}({code})")
        return record
    except Exception as e:
        print(f"❌ 获取 {name}({code}) 失败: {e}")
        return None


if __name__ == "__main__":
    query = "SELECT market, code, name FROM df_a_stock_detail_df WHERE ds='2025-10-29' and board in (2,6);"
    df_stock_list = con.execute(query).fetchdf()

    target_ds = "2025-10-30"
    records = []

    # ===== 使用线程池并发获取 =====
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_and_build_record, row, target_ds): row for _, row in df_stock_list.iterrows()}
        for future in as_completed(futures):
            record = future.result()
            if record:
                records.append(record)

    print(f"\n📊 共获取到 {len(records)} 条有效数据")

    # ===== 批量插入 DuckDB =====
    if records:
        df = pd.DataFrame(records, columns=[
            'market', 'code', 'name', 'up_down_rate', 'up_down_amount', 'turnover_rate', 'amplitude',
            'highest', 'lowest', 'opening_price', 'closing_price', 'deal_amount', 'deal_vol', 'ds'
        ])

        con.register('tmp_kline_df', df)
        con.execute("""
            INSERT INTO df_a_stock_history_detail_df
            SELECT * FROM tmp_kline_df
        """)
        con.commit()
        print(f"✅ 成功写入 {len(df)} 条记录至 df_a_stock_history_detail_df")
