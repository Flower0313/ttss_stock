import json
import requests
import duckdb
import pandas as pd
# CREATE TABLE dim_cics_industry_stock_info (
#     code     VARCHAR,   -- 证券代码
#     name     VARCHAR,   -- 证券名称
#     industry_1_code      VARCHAR,   -- CICS 一级行业代码
#     industry_1_name      VARCHAR,   -- CICS 一级行业名称
#     industry_2_code      VARCHAR,   -- CICS 二级行业代码
#     industry_2_name      VARCHAR,   -- CICS 二级行业名称
#     industry_3_code      VARCHAR,   -- CICS 三级行业代码
#     industry_3_name      VARCHAR,   -- CICS 三级行业名称
#     industry_4_code      VARCHAR,   -- CICS 四级行业代码
#     industry_4_name      VARCHAR,   -- CICS 四级行业名称
#     csrc1stCode      VARCHAR,   -- 证监会一级行业代码
#     csrc1stName      VARCHAR,   -- 证监会一级行业名称
#     csrc2ndCode      VARCHAR,   -- 证监会二级行业代码
#     csrc2ndName      VARCHAR    -- 证监会二级行业名称
# );

cookies = {
    'Hm_lvt_f0fbc9a7e7e7f29a55a0c13718e9e542': '1765336902',
    'HMACCOUNT': '194D2CC4E4F6A7C6',
    'Hm_lpvt_f0fbc9a7e7e7f29a55a0c13718e9e542': '1765350186',
}

headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6',
    'Connection': 'keep-alive',
    'Content-Type': 'application/json;charset=UTF-8',
    'Origin': 'https://www.csindex.com.cn',
    'Referer': 'https://www.csindex.com.cn/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

json_data = {
    'pageNum': 1,
    'pageSize': 10000,
    'sortField': None,
    'sortOrder': None,
}

response = requests.post(
    'https://www.csindex.com.cn/csindex-home/indexInfo/security-industry-search',
    cookies=cookies,
    headers=headers,
    json=json_data,
)
data = response.json().get("data", [])

if not data:
    print("接口未返回任何数据，程序退出")
    exit()

mapping = {
    "securityCode": "code",
    "securityName": "name",
    "cics1stCode": "industry_1_code",
    "cics1stName": "industry_1_name",
    "cics2ndCode": "industry_2_code",
    "cics2ndName": "industry_2_name",
    "cics3rdCode": "industry_3_code",
    "cics3rdName": "industry_3_name",
    "cics4thCode": "industry_4_code",
    "cics4thName": "industry_4_name",
    "csrc1stCode": "csrc1stCode",
    "csrc1stName": "csrc1stName",
    "csrc2ndCode": "csrc2ndCode",
    "csrc2ndName": "csrc2ndName"
}

# 生成映射后的数据
mapped_data = []
for item in data:
    row = {}
    for api_field, my_field in mapping.items():
        row[my_field] = item.get(api_field)
    mapped_data.append(row)

df = pd.DataFrame(mapped_data)

# ---------------------------------------
# 3. 写入 DuckDB：清空 + 插入
# ---------------------------------------
con = duckdb.connect("../stocks.duckdb")


# 若表不存在，自动建表（仅第一次）
con.execute("""
CREATE TABLE IF NOT EXISTS dim_cics_industry_stock_info AS
SELECT * FROM df
""")

# 清空表
con.execute("DELETE FROM dim_cics_industry_stock_info")

# 插入新数据
con.execute("""
INSERT INTO dim_cics_industry_stock_info
SELECT * FROM df
""")

con.close()

print("成功写入 DuckDB！")