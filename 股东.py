import duckdb
import requests
import time
import pandas as pd


def get_free_holders(stock_code: str, end_date: str, page: int = 1, page_size: int = 50):
    """
    获取东方财富 F10 前十大流通股东数据
    """
    url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"

    headers = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6",
        "Connection": "keep-alive",
        "Origin": "https://emweb.securities.eastmoney.com",
        "Referer": "https://emweb.securities.eastmoney.com/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/141.0.0.0 Safari/537.36"
        ),
    }

    params = {
        "reportName": "RPT_F10_EH_FREEHOLDERS",
        "columns": (
            "SECUCODE,SECURITY_CODE,END_DATE,HOLDER_RANK,HOLDER_NEW,"
            "HOLDER_NAME,HOLDER_TYPE,SHARES_TYPE,HOLD_NUM,FREE_HOLDNUM_RATIO,"
            "HOLD_NUM_CHANGE,CHANGE_RATIO"
        ),
        "quoteColumns": "",
        "filter": f'(SECUCODE="{stock_code}")(END_DATE=\'{end_date}\')',
        "pageNumber": page,
        "pageSize": page_size,
        "sortTypes": 1,
        "sortColumns": "HOLDER_RANK",
        "source": "HSF10",
        "client": "PC",
        "v": str(int(time.time() * 1000)),  # 动态时间戳
    }

    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()

    data = resp.json()
    if "result" not in data or not data["result"]:
        return pd.DataFrame()  # 空返回

    df = pd.DataFrame(data["result"]["data"])
    return df


def main():
    # 1️⃣ 连接 DuckDB
    con = duckdb.connect("stocks.duckdb")

    # 2️⃣ 查询符合条件的股票列表
    stock_sql = """
        SELECT 
            CONCAT(code,CASE WHEN market = 1 THEN '.SH' ELSE '.SZ' END) AS code,
            name
        FROM df_a_stock_detail_df
        WHERE ds = '2025-10-23'
          AND board IN (2, 6) and name not like '%ST%' and name not like '%ST%' and current_price<=5 
    """
    stock_list = con.execute(stock_sql).fetchdf()

    print(f"共查询到 {len(stock_list)} 只股票")

    # 3️⃣ 创建目标表（如果不存在）
    con.execute("""
        CREATE TABLE IF NOT EXISTS df_a_stock_shareholders_df (
            code VARCHAR,
            name VARCHAR,
            ds varchar,
            HOLDER_NAME VARCHAR,
            HOLD_NUM DOUBLE,
            FREE_HOLDNUM_RATIO DOUBLE
            
        )
    """)

    # 4️⃣ 遍历每只股票，获取股东数据并写入
    all_data = []
    for _, row in stock_list.iterrows():
        code = row["code"]
        name = row["name"]

        try:
            df = get_free_holders(code, "2025-09-30")
            if not df.empty:
                df["code"] = code
                df["name"] = name
                df['ds'] = "2025-09-30"
                all_data.append(df[["code", "name","ds", "HOLDER_NAME", "HOLD_NUM", "FREE_HOLDNUM_RATIO"]])
                print(f"✅ 已获取 {name} ({code}) 股东数据")
                #time.sleep(1.5)
            else:
                print(f"⚠️ {name} ({code}) 暂无数据")
        except Exception as e:
            print(f"❌ {name} ({code}) 获取失败: {e}")

        time.sleep(0.8)  # 加点延时防止触发风控

    # 5️⃣ 批量写入 DuckDB
    if all_data:
        result_df = pd.concat(all_data, ignore_index=True)
        con.register("tmp_df", result_df)
        con.execute("INSERT INTO df_a_stock_shareholders_df SELECT * FROM tmp_df")
        print(f"🎯 已插入 {len(result_df)} 条记录到 df_a_stock_shareholders_df")

    con.close()


if __name__ == "__main__":
    main()