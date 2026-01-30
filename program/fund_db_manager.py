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
        # 使用正则提取 JSON 内容
        match = re.search(r'jsonpgz\((.*)\);', response.text)
        if match:
            data = json.loads(match.group(1))
            return {
                'fund_code': data['fundcode'],
                'date': data['gztime'].split(' ')[0],  # 只取日期部分
                'unit_value': float(data['gsz']),
                'total_value': float(data['gsz']),  # 简易处理，通常总净值需另抓
                'growth_rate': float(data['gszzl'])
            }
    except Exception as e:
        print(f"抓取基金 {fund_code} 失败: {e}")
    return None


def main():
    # 确保路径在云端也能被找到
    db_path = os.path.join('output', 'funds_manager.db')

    # 如果 output 文件夹不存在则创建（防止云端环境初始报错）
    if not os.path.exists('output'):
        os.makedirs('output')

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 确保表结构存在
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fund_history (
            fund_code TEXT,
            date TEXT,
            unit_value REAL,
            total_value REAL,
            growth_rate REAL,
            PRIMARY KEY (fund_code, date)
        )
    ''')

    # 记录运行前的行数
    cursor.execute("SELECT COUNT(*) FROM fund_history")
    before_count = cursor.fetchone()[0]

    # 需要抓取的基金列表
    fund_list = ['023350']

    print(f"[{datetime.now()}] 启动云端同步程序...")

    for code in fund_list:
        data = fetch_fund_data(code)
        if data:
            # 使用 INSERT OR IGNORE 防止重复插入同一天数据导致报错
            cursor.execute('''
                INSERT OR IGNORE INTO fund_history (fund_code, date, unit_value, total_value, growth_rate)
                VALUES (?, ?, ?, ?, ?)
            ''', (data['fund_code'], data['date'], data['unit_value'], data['total_value'], data['growth_rate']))

    conn.commit()

    # 记录运行后的行数
    cursor.execute("SELECT COUNT(*) FROM fund_history")
    after_count = cursor.fetchone()[0]
    new_records = after_count - before_count

    conn.close()

    # --- 战果汇报模块 ---
    print("\n" + "=" * 35)
    print(f"📊 运行报告 | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"✅ 今日成功更新: {new_records} 条数据")
    print(f"📈 数据库总条数: {after_count} 条")
    print(f"📅 状态: {'数据已更新' if new_records > 0 else '今日非交易日或数据已存在'}")
    print("=" * 35 + "\n")


if __name__ == "__main__":
    main()