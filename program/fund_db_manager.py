import requests
import json
import re
import sqlite3
import os
import math
import time

# --- [1. 路径与配置] ---
# 优先使用本地 D 盘，否则使用脚本同级目录
local_path = r"D:\quant\webCrawlerProjectPractice\output"
output_dir = local_path if os.path.exists(local_path) else os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(output_dir, "funds_manager.db")

target_fund = "023350"


# --- [2. 数据库逻辑] ---
def init_db():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fund_history (
            fund_code TEXT, date TEXT, unit_value REAL, 
            total_value REAL, growth_rate TEXT,
            PRIMARY KEY(fund_code, date)
        )
    ''')
    conn.commit()
    return conn


def fetch_data(code, page):
    """通用的抓取函数"""
    url = f"https://api.fund.eastmoney.com/f10/lsjz?callback=jQuery&fundCode={code}&pageIndex={page}&pageSize=20"
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://fundf10.eastmoney.com/"}
    res = requests.get(url, headers=headers)
    json_str = re.search(r'\((.*)\)', res.text).group(1)
    return json.loads(json_str)


def sync_fund(conn, code):
    # 1. 检查本地数据库里这只基金有多少条记录
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM fund_history WHERE fund_code=?", (code,))
    local_count = cursor.fetchone()[0]

    # 2. 获取服务器上的总条数
    first_page = fetch_data(code, 1)
    server_count = int(first_page['TotalCount'])

    # 3. 决定抓取范围
    if local_count < server_count:
        # 如果本地数据少于服务器，说明需要补历史数据，计算总页数进行全量同步
        pages_to_fetch = math.ceil(server_count / 20)
        print(f"检测到数据缺失（本地:{local_count} < 远程:{server_count}），开始同步共 {pages_to_fetch} 页...")
    else:
        # 否则只抓第一页（增量更新）
        pages_to_fetch = 1
        print(f"数据已是最新或只需增量检查，仅抓取第1页...")

    for page in range(1, pages_to_fetch + 1):
        print(f"正在同步第 {page}/{pages_to_fetch} 页...")
        data = fetch_data(code, page)
        for item in data['Data']['LSJZList']:
            sql = "INSERT OR IGNORE INTO fund_history VALUES (?, ?, ?, ?, ?)"
            cursor.execute(sql, (code, item['FSRQ'], item['DWJZ'], item['LJJZ'], item['JZZZL']))
        conn.commit()
        time.sleep(0.5)


if __name__ == "__main__":
    db_conn = init_db()
    sync_fund(db_conn, target_fund)
    db_conn.close()
    print("同步任务结束。")