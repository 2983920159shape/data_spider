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
        response = requests.get(url, timeout=25, headers=headers)
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
    cursor = conn.cursor()

    # --- 核心优化：智能检查并升级表结构 ---
    try:
        cursor.execute("SELECT fund_code FROM fund_history LIMIT 1")
        # 如果执行成功，说明还是旧的英文列名，我们直接删表重建（因为数据量小，重新抓取很快）
        print("检测到旧版英文表，正在自动升级为中文结构...")
        cursor.execute("DROP TABLE fund_history")
    except sqlite3.OperationalError:
        # 如果报错，说明已经是中文表或者表不存在，这是正常的
        pass

    # 创建中文列名的表
    cursor.execute('''CREATE TABLE IF NOT EXISTS fund_history 
                    (基金代码 TEXT, 日期 TEXT, 单位净值 REAL, 
                     累计净值 REAL, 日涨跌幅 REAL, 
                     PRIMARY KEY (基金代码, 日期))''')
    conn.commit()

    # 执行抓取
    bj_now = get_beijing_time()
    fund_list = ['023350']
    print(f"[{bj_now.strftime('%Y-%m-%d %H:%M:%S')}] 启动同步程序...")

    results = []
    for code in fund_list:
        res = fetch_fund_data(code)
        if res:
            results.append(res)

    if results:
        df_new = pd.DataFrame(results)
        for _, row in df_new.iterrows():
            cursor.execute('''
                INSERT OR IGNORE INTO fund_history (基金代码, 日期, 单位净值, 累计净值, 日涨跌幅)
                VALUES (?, ?, ?, ?, ?)
            ''', (row['基金代码'], row['日期'], row['单位净值'], row['累计净值'], row['日涨跌幅']))
        conn.commit()

    # 导出 CSV
    full_df = pd.read_sql("SELECT * FROM fund_history ORDER BY 日期 DESC", conn)
    full_df.to_csv(csv_path, index=False, encoding='utf-8-sig')

    after_count = len(full_df)
    conn.close()

    # 战果汇报
    report = (
            f"\n" + "=" * 40 + "\n"
                               f"📊 数据同步报告 | {bj_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
                               f"✅ 状态反馈: 数据汉化同步成功\n"
                               f"📈 数据库总条数: {after_count} 条\n"
                               f"========================================\n"
    )
    print(report)
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(report)


if __name__ == "__main__":
    main()