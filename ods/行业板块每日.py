# _*_ coding : utf-8 _*_
import requests
import duckdb
import pandas as pd
import time
from datetime import datetime
# =====================
# 基础配置
# =====================
db_path = "../stocks.duckdb"
table_name = "df_a_industry_sector_detail_df"

# 接口基础参数
url = "https://push2.eastmoney.com/api/qt/clist/get"

cookies = {
    'qgqp_b_id': 'cbaf91ab2a32ba754f9ff83b95ca890b',
    'st_nvi': 'egZQSMgtTClr1cqkCuwnXe6c6',
    'mtp': '1',
    'ct': 'FT3ki4qamnZ3ZBIYTkMjBtfGHflrYMBSxOzaJuay1ZmCuv0COdNwch5ksCpoYJYbdeEDuBLLJm-O_fn0jqgwh7W2U1WZZJ81TAFGixXeQT7mIWBrUs5NzFaOTMwfLHtVG6hBF1j2RDyTrDKXAjobDfZo0OajUQMz3NxN9XGUiLE',
    'ut': 'FobyicMgeV4n4cCT8MJKZOhRZpPnvGrbz_B_nVAvVyBqu1HKh8C_X-DZIbMu0e-69SQKPieUWG7iAtaVxvX2gHxvUsl0Ij-Dx93k5eQgcELPnYGC38fl2w37MaBNh7wkezhFuExwif1d0VqnJCI_ppvxQqVdpMja08rqvec05cwi8d9ITNGrl74x3SiqaZ-shoxBo22_C0p-qJqrNwPXofKOtdA6FsCIvMZ0euqvJBoAMdSX2-_YZvdXb1yjNE8AhdQ77Pabq_y9OrC-1CeWFe87Zts2Gt1f',
    'nid18': '0c5414be2064203f83bda4c87fdd9ffc',
    'nid18_create_time': '1764328948834',
    'gviem': 'As6kolzy8l2IdkY1U_lG25aa8',
    'gviem_create_time': '1764328948834',
    'emshistory': '%5B%22%E8%B5%9B%E5%8A%9B%E6%96%AF%22%2C%22xd%E8%B5%9B%E5%8A%9B%E6%96%AF%22%2C%22%E5%A4%A9%E5%92%8C%E7%A3%81%E6%9D%90%22%5D',
    'sensorsdata2015jssdkcross': '%7B%22distinct_id%22%3A%2219af23754ed623-0e0705f8463bb28-26061b51-3686400-19af23754ee11e2%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTlhZjIzNzU0ZWQ2MjMtMGUwNzA1Zjg0NjNiYjI4LTI2MDYxYjUxLTM2ODY0MDAtMTlhZjIzNzU0ZWUxMWUyIn0%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%2C%22%24device_id%22%3A%2219af23754ed623-0e0705f8463bb28-26061b51-3686400-19af23754ee11e2%22%7D',
    'st_si': '29474095788729',
    'st_asi': 'delete',
    'websitepoptg_api_time': '1765279855767',
    'fullscreengg': '1',
    'fullscreengg2': '1',
    'st_pvi': '80850450855664',
    'st_sp': '2025-08-25%2022%3A01%3A51',
    'st_inirUrl': 'https%3A%2F%2Fwww.baidu.com%2Flink',
    'st_sn': '5',
    'st_psi': '20251209193105525-113200313002-4492090209',
}

headers = {
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6',
    'Connection': 'keep-alive',
    'Referer': 'https://quote.eastmoney.com/center/gridlist.html',
    'Sec-Fetch-Dest': 'script',
    'Sec-Fetch-Mode': 'no-cors',
    'Sec-Fetch-Site': 'same-site',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    # 'Cookie': 'qgqp_b_id=cbaf91ab2a32ba754f9ff83b95ca890b; st_nvi=egZQSMgtTClr1cqkCuwnXe6c6; mtp=1; ct=FT3ki4qamnZ3ZBIYTkMjBtfGHflrYMBSxOzaJuay1ZmCuv0COdNwch5ksCpoYJYbdeEDuBLLJm-O_fn0jqgwh7W2U1WZZJ81TAFGixXeQT7mIWBrUs5NzFaOTMwfLHtVG6hBF1j2RDyTrDKXAjobDfZo0OajUQMz3NxN9XGUiLE; ut=FobyicMgeV4n4cCT8MJKZOhRZpPnvGrbz_B_nVAvVyBqu1HKh8C_X-DZIbMu0e-69SQKPieUWG7iAtaVxvX2gHxvUsl0Ij-Dx93k5eQgcELPnYGC38fl2w37MaBNh7wkezhFuExwif1d0VqnJCI_ppvxQqVdpMja08rqvec05cwi8d9ITNGrl74x3SiqaZ-shoxBo22_C0p-qJqrNwPXofKOtdA6FsCIvMZ0euqvJBoAMdSX2-_YZvdXb1yjNE8AhdQ77Pabq_y9OrC-1CeWFe87Zts2Gt1f; nid18=0c5414be2064203f83bda4c87fdd9ffc; nid18_create_time=1764328948834; gviem=As6kolzy8l2IdkY1U_lG25aa8; gviem_create_time=1764328948834; emshistory=%5B%22%E8%B5%9B%E5%8A%9B%E6%96%AF%22%2C%22xd%E8%B5%9B%E5%8A%9B%E6%96%AF%22%2C%22%E5%A4%A9%E5%92%8C%E7%A3%81%E6%9D%90%22%5D; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2219af23754ed623-0e0705f8463bb28-26061b51-3686400-19af23754ee11e2%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTlhZjIzNzU0ZWQ2MjMtMGUwNzA1Zjg0NjNiYjI4LTI2MDYxYjUxLTM2ODY0MDAtMTlhZjIzNzU0ZWUxMWUyIn0%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%2C%22%24device_id%22%3A%2219af23754ed623-0e0705f8463bb28-26061b51-3686400-19af23754ee11e2%22%7D; st_si=29474095788729; st_asi=delete; websitepoptg_api_time=1765279855767; fullscreengg=1; fullscreengg2=1; st_pvi=80850450855664; st_sp=2025-08-25%2022%3A01%3A51; st_inirUrl=https%3A%2F%2Fwww.baidu.com%2Flink; st_sn=5; st_psi=20251209193105525-113200313002-4492090209',
}

base_params = {
    "np": 1,
    "fltt": 1,
    "invt": 2,
    "fs": "m:90+t:2+f:!50",  # 市场过滤条件
    "fid": "f3",
    "pz": 100,               # 每页100条
    "po": 1,
    "dect": 1,
    "ut": "fa5fd1943c7b386f172d6893dbfba10b",
    "wbp2u": "4819115464066356|0|1|0|web",
    "fields": "f13,f12,f14,f3,f4,f8,f7,f15,f16,f17,f2,f5,f6,f34,f35,f21,f39,f64,f65,f70,f71,f76,f77,f82,f83",
}


# 字段映射
mapping = {
    "f13": "market",
    "f12": "code",
    "f14": "name",
    "f3": "up_down_rate",
    "f4": "up_down_amount",
    "f8": "turnover_rate",
    "f7": "amplitude",
    "f15": "highest",
    "f16": "lowest",
    "f17": "opening_price",
    "f2": "closing_price",
    "f5": "deal_amount",
    "f6": "deal_vol",
    "f34": "outer_disk",
    "f35": "inner_disk",
    "f21": "free_float_market_cap",
    "f39": "free_float_shares",
    "f64": "extra_large_inflow",
    "f65": "extra_large_outflow",
    "f70": "large_inflow",
    "f71": "large_outflow",
    "f76": "medium_inflow",
    "f77": "medium_outflow",
    "f82": "small_inflow",
    "f83": "small_outflow",
}

# =====================
# 初始化 DuckDB 连接
# =====================
con = duckdb.connect(db_path)

# 若表不存在则创建
con.execute(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    market STRING,
    code STRING,
    name STRING,
    up_down_rate DECIMAL(10,4),
    up_down_amount DECIMAL(10,4),
    turnover_rate DECIMAL(10,4),
    amplitude DECIMAL(10,4),
    highest DECIMAL(10,2),
    lowest DECIMAL(10,2),
    opening_price DECIMAL(10,2),
    closing_price DECIMAL(10,2),
    deal_amount DECIMAL(20,2),
    deal_vol BIGINT,
    outer_disk DECIMAL(20,2),
    inner_disk DECIMAL(20,2),
    free_float_market_cap DECIMAL(20,2),
    free_float_shares BIGINT,
    extra_large_inflow DECIMAL(20,2),
    extra_large_outflow DECIMAL(20,2),
    large_inflow DECIMAL(20,2),
    large_outflow DECIMAL(20,2),
    medium_inflow DECIMAL(20,2),
    medium_outflow DECIMAL(20,2),
    small_inflow DECIMAL(20,2),
    small_outflow DECIMAL(20,2),
    ds STRING
);
""")

# =====================
# 分页抓取逻辑
# =====================
all_rows = []
page = 1
page_size = base_params["pz"]

while True:
    print(f"📥 抓取第 {page} 页 ...")
    base_params["pn"] = page
    resp = requests.get(url, params=base_params,cookies=cookies,headers=headers, timeout=10)
    data = resp.json()

    if not data or "data" not in data or "diff" not in data["data"]:
        print("⚠️ 接口返回为空或结构异常，终止。")
        break

    rows = data["data"]["diff"]
    if not rows:
        print("✅ 已到最后一页。")
        break

    # 转换映射字段
    df = pd.DataFrame([{v: row.get(k) for k, v in mapping.items()} for row in rows])

    # 比例换算
    for col in ["up_down_rate", "up_down_amount", "turnover_rate", "amplitude",
                "highest", "lowest", "opening_price", "closing_price"]:
        if col in df.columns:
            df[col] = df[col] / 100
    df["ds"] = datetime.today().strftime("%Y-%m-%d")
    # 注册并插入 DuckDB
    con.register("df_page", df)
    con.execute(f"INSERT INTO {table_name} SELECT * FROM df_page")
    con.unregister("df_page")

    print(f"✅ 第 {page} 页插入 {len(df)} 条记录")
    all_rows.append(len(df))

    # 如果不足一页说明已结束
    if len(df) < page_size:
        print("✅ 数据量不足一页，已到最后一页。")
        break

    page += 1
    time.sleep(2)  # 防止请求过快被限流

# =====================
# 检查结果
# =====================
total = sum(all_rows)
print(f"\n🎉 抓取完成，共插入 {total} 条记录到表 `{table_name}` 中。")

sample = con.execute(f"SELECT * FROM {table_name} LIMIT 5").df()
print(sample)
