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
        response = requests.get(url, timeout=10)
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
    # 确保路径通用
    db_path = os.path.join('output', 'funds_manager.db')
    if not os.path.exists('output'):
        os.makedirs('output')

    conn = sqlite3.connect(db_path)

    # 运行前检查数据条数 (使用 pandas 读取)
    try:
        df_before = pd.read_sql("SELECT * FROM fund_history", conn)
        before_count = len(df_before)
    except:
        before_count = 0

    # 执行抓取
    fund_list = ['023350']
    print(f"[{datetime.now()}] 启动云端同步程序 (Pandas版)...")

    results = []
    for code in fund_list:
        data = fetch_fund_data(code)
        if data:
            results.append(data)

    if results:
        df_new = pd.DataFrame(results)
        # 写入数据库，重复的 (code, date) 会因为 PRIMARY KEY 冲突而忽略
        # 我们手动处理或使用 SQL 语句
        cursor = conn.cursor()
        for _, row in df_new.iterrows():
            cursor.execute('''
                INSERT OR IGNORE INTO fund_history (fund_code, date, unit_value, total_value, growth_rate)
                VALUES (?, ?, ?, ?, ?)
            ''', (row['fund_code'], row['date'], row['unit_value'], row['total_value'], row['growth_rate']))
        conn.commit()

    # 运行后检查
    df_after = pd.read_sql("SELECT * FROM fund_history", conn)
    after_count = len(df_after)
    new_records = after_count - before_count
    conn.close()

    # --- 战果汇报 ---
    print("\n" + "=" * 35)
    print(f"📊 Pandas 运行报告 | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"✅ 今日成功更新: {new_records} 条数据")
    print(f"📈 数据库总条数: {after_count} 条")
    print(f"📅 状态: {'数据已同步' if new_records > 0 else '非交易日或已存在'}")
    print("=" * 35 + "\n")


if __name__ == "__main__":
    main()