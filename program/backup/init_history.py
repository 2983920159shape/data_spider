import requests
import json
import re
import sqlite3
import os
import math

# 路径指向你 output 文件夹下的数据库
db_path = r"D:\quant\webCrawlerProjectPractice\output\funds_manager.db"


def init_all_history(fund_code):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 1. 先抓第一页，确定总共有多少页
    base_url = f"https://api.fund.eastmoney.com/f10/lsjz?callback=jQuery&fundCode={fund_code}&pageIndex={{}}&pageSize=20"
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://fundf10.eastmoney.com/"}

    res = requests.get(base_url.format(1), headers=headers)
    json_str = re.search(r'\((.*)\)', res.text).group(1)
    total_count = int(json.loads(json_str)['TotalCount'])
    total_pages = math.ceil(total_count / 20)

    print(f"检测到共有 {total_count} 条数据，共 {total_pages} 页。开始全量初始化...")

    for page in range(1, total_pages + 1):
        print(f"正在抓取第 {page} 页...")
        res = requests.get(base_url.format(page), headers=headers)
        data_list = json.loads(re.search(r'\((.*)\)', res.text).group(1))['Data']['LSJZList']

        for item in data_list:
            sql = "INSERT OR IGNORE INTO fund_history VALUES (?, ?, ?, ?, ?)"
            cursor.execute(sql, (fund_code, item['FSRQ'], item['DWJZ'], item['LJJZ'], item['JZZZL']))

        conn.commit()

    conn.close()
    print("全量数据补齐完成！现在你的数据库里有所有历史数据了。")


if __name__ == "__main__":
    init_all_history("023350")