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
    # 路径处理
    db_path = os.path.join('output', 'funds_manager.db')
    log_path = os.path.join('output', 'daily_sync.log')
    csv_path = os.path.join('output', 'funds_history_export.csv')

    if not os.path.exists('output'):
        os.makedirs('output')

    conn = sqlite3.connect(db_path)

    # 1. 检查运行前状态
    try:
        before_df = pd.read_sql("SELECT * FROM fund_history", conn)
        before_count = len(before_df)
    except:
        before_count = 0
        conn.execute('''CREATE TABLE IF NOT EXISTS fund_history 
                        (fund_code TEXT, date TEXT, unit_value REAL, 
                         total_value REAL, growth_rate REAL, 
                         PRIMARY KEY (fund_code, date))''')

    # 2. 执行抓取任务
    fund_list = ['023350']
    print(f"[{datetime.now()}] 启动同步程序...")

    results = []
    for code in fund_list:
        res = fetch_fund_data(code)
        if res:
            results.append(res)

    # 3. 写入数据库
    if results:
        df_new = pd.DataFrame(results)
        cursor = conn.cursor()
        for _, row in df_new.iterrows():
            cursor.execute('''
                INSERT OR IGNORE INTO fund_history (fund_code, date, unit_value, total_value, growth_rate)
                VALUES (?, ?, ?, ?, ?)
            ''', (row['fund_code'], row['date'], row['unit_value'], row['total_value'], row['growth_rate']))
        conn.commit()

    # 4. 获取最新全量数据并导出 CSV
    full_df = pd.read_sql("SELECT * FROM fund_history ORDER BY date DESC", conn)
    after_count = len(full_df)
    new_records = after_count - before_count

    # 导出 CSV 方便网页查看
    full_df.to_csv(csv_path, index=False, encoding='utf-8-sig')

    conn.close()

    # 5. 生成报告内容
    report = (
            f"\n" + "=" * 40 + "\n"
                               f"📊 数据同步报告 | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                               f"✅ 本次新增记录: {new_records} 条\n"
                               f"📈 数据库总条数: {after_count} 条\n"
                               f"📅 状态反馈: {'数据更新成功' if new_records > 0 else '今日暂无新数据或非交易日'}\n"
                               f"========================================\n"
    )

    # 6. 打印到控制台 (Actions 日志可见)
    print(report)

    # 7. 追加到本地日志文件 (GitHub 仓库可见)
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(report)


if __name__ == "__main__":
    main()