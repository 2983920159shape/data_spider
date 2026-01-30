import requests
import sqlite3
import pandas as pd
from datetime import datetime
import os
import re
import json


def fetch_fund_data(fund_code):
    """抓取基金净值数据"""
    url = f"https://fundgz.1234567.com.cn/js/{fund_code}.js"
    try:
        response = requests.get(url, timeout=15)
        match = re.search(r'jsonpgz\((.*)\);', response.text)
        if match:
            data = json.loads(match.group(1))
            return {
                'fund_code': data['fundcode'],
                'date': data['gztime'].split(' ')[0],
                'unit_value': float(data['gsz']),
                'total_value': float(data['gsz']),
                'growth_rate': float(data['gszzl'])
            }
    except Exception as e:
        print(f"抓取基金 {fund_code} 失败: {e}")
    return None


def main():
    # 路径处理：兼容本地 Windows 和云端 Linux
    db_path = os.path.join('output', 'funds_manager.db')
    if not os.path.exists('output'):
        os.makedirs('output')

    conn = sqlite3.connect(db_path)

    # 1. 检查运行前状态
    try:
        before_df = pd.read_sql("SELECT * FROM fund_history", conn)
        before_count = len(before_df)
    except:
        before_count = 0
        # 如果表不存在则创建
        conn.execute('''CREATE TABLE IF NOT EXISTS fund_history 
                        (fund_code TEXT, date TEXT, unit_value REAL, 
                         total_value REAL, growth_rate REAL, 
                         PRIMARY KEY (fund_code, date))''')

    # 2. 执行抓取任务
    fund_list = ['023350']
    print(f"[{datetime.now()}] 启动同步程序 (Pandas 驱动)...")

    results = []
    for code in fund_list:
        res = fetch_fund_data(code)
        if res:
            results.append(res)

    # 3. 增量写入数据库
    if results:
        df_new = pd.DataFrame(results)
        cursor = conn.cursor()
        for _, row in df_new.iterrows():
            cursor.execute('''
                INSERT OR IGNORE INTO fund_history (fund_code, date, unit_value, total_value, growth_rate)
                VALUES (?, ?, ?, ?, ?)
            ''', (row['fund_code'], row['date'], row['unit_value'], row['total_value'], row['growth_rate']))
        conn.commit()

    # 4. 检查运行后状态
    after_df = pd.read_sql("SELECT * FROM fund_history", conn)
    after_count = len(after_df)
    new_records = after_count - before_count
    conn.close()

    # --- 自动化战果汇报 ---
    print("\n" + "=" * 40)
    print(f"📊 数据同步报告 | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"✅ 本次新增记录: {new_records} 条")
    print(f"📈 数据库总条数: {after_count} 条")
    print(f"📅 状态反馈: {'数据更新成功' if new_records > 0 else '今日暂无新数据或非交易日'}")
    print("=" * 40 + "\n")


if __name__ == "__main__":
    main()