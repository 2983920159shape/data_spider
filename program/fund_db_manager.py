import requests
import sqlite3
import pandas as pd
import os
import re
import json
from datetime import datetime, timedelta, timezone


def fetch_fund_data(fund_code):
    """抓取基金净值数据"""
    url = f"https://fundgz.1234567.com.cn/js/{fund_code}.js"
    try:
        # 增加 User-Agent 伪装，提高云端访问成功率
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(url, timeout=25, headers=headers)
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


def get_beijing_time():
    """获取精准的北京时间"""
    # 强制偏移 UTC+8
    tz_beijing = timezone(timedelta(hours=8))
    return datetime.now(tz_beijing)


def main():
    db_path = os.path.join('output', 'funds_manager.db')
    log_path = os.path.join('output', 'daily_sync.log')
    csv_path = os.path.join('output', 'funds_history_export.csv')

    if not os.path.exists('output'):
        os.makedirs('output')

    conn = sqlite3.connect(db_path)

    # 保持旧的英文列名结构
    conn.execute('''CREATE TABLE IF NOT EXISTS fund_history 
                    (fund_code TEXT, date TEXT, unit_value REAL, 
                     total_value REAL, growth_rate REAL, 
                     PRIMARY KEY (fund_code, date))''')

    # 检查运行前数据量
    before_df = pd.read_sql("SELECT * FROM fund_history", conn)
    before_count = len(before_df)

    # 获取北京时间并打印报告头部
    bj_now = get_beijing_time()
    print(f"[{bj_now.strftime('%Y-%m-%d %H:%M:%S')}] 启动云端同步程序...")

    # 执行抓取
    fund_list = ['023350']
    results = []
    for code in fund_list:
        res = fetch_fund_data(code)
        if res:
            results.append(res)

    # 写入增量数据
    if results:
        df_new = pd.DataFrame(results)
        cursor = conn.cursor()
        for _, row in df_new.iterrows():
            cursor.execute('''
                INSERT OR IGNORE INTO fund_history (fund_code, date, unit_value, total_value, growth_rate)
                VALUES (?, ?, ?, ?, ?)
            ''', (row['fund_code'], row['date'], row['unit_value'], row['total_value'], row['growth_rate']))
        conn.commit()

    # 刷新状态并导出 CSV
    full_df = pd.read_sql("SELECT * FROM fund_history ORDER BY date DESC", conn)
    after_count = len(full_df)
    new_records = after_count - before_count
    full_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    conn.close()

    # 生成报告内容
    report = (
            f"\n" + "=" * 40 + "\n"
                               f"📊 数据同步报告 | {bj_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
                               f"✅ 本次新增记录: {new_records} 条\n"
                               f"📈 数据库总条数: {after_count} 条\n"
                               f"📅 状态反馈: {'数据更新成功' if new_records > 0 else '今日暂无新数据或抓取超时'}\n"
                               f"========================================\n"
    )
    print(report)
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(report)


if __name__ == "__main__":
    main()