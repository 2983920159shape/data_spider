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
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(url, timeout=20, headers=headers)
        match = re.search(r'jsonpgz\((.*)\);', response.text)
        if match:
            data = json.loads(match.group(1))
            return {
                '基金代码': data['fundcode'],
                '日期': data['gztime'].split(' ')[0],
                '单位净值': float(data['gsz']),
                '累计净值': float(data['gsz']),
                '日涨跌幅': float(data['gszzl'])
            }
    except Exception as e:
        print(f"抓取基金 {fund_code} 失败: {e}")
    return None


def get_beijing_time():
    """获取精准的北京时间"""
    tz_beijing = timezone(timedelta(hours=8))
    return datetime.now(tz_beijing)


def main():
    db_path = os.path.join('output', 'funds_manager.db')
    log_path = os.path.join('output', 'daily_sync.log')
    csv_path = os.path.join('output', 'funds_history_export.csv')

    if not os.path.exists('output'):
        os.makedirs('output')

    conn = sqlite3.connect(db_path)

    # 1. 确保表结构（使用中文列名）
    conn.execute('''CREATE TABLE IF NOT EXISTS fund_history 
                    (基金代码 TEXT, 日期 TEXT, 单位净值 REAL, 
                     累计净值 REAL, 日涨跌幅 REAL, 
                     PRIMARY KEY (基金代码, 日期))''')

    # 检查运行前状态
    before_df = pd.read_sql("SELECT * FROM fund_history", conn)
    before_count = len(before_df)

    # 2. 执行抓取任务
    bj_now = get_beijing_time()
    fund_list = ['023350']
    print(f"[{bj_now.strftime('%Y-%m-%d %H:%M:%S')}] 启动云端同步程序...")

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
                INSERT OR IGNORE INTO fund_history (基金代码, 日期, 单位净值, 累计净值, 日涨跌幅)
                VALUES (?, ?, ?, ?, ?)
            ''', (row['基金代码'], row['日期'], row['单位净值'], row['累计净值'], row['日涨跌幅']))
        conn.commit()

    # 4. 获取最新数据并导出 CSV
    full_df = pd.read_sql("SELECT * FROM fund_history ORDER BY 日期 DESC", conn)
    after_count = len(full_df)
    new_records = after_count - before_count

    # 导出中文列名的 CSV
    full_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    conn.close()

    # 5. 生成报告
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