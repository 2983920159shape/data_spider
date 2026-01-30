import requests
import sqlite3
import pandas as pd
import os
import json
import time
import math
from datetime import datetime, timedelta, timezone


def get_beijing_time():
    """【功能】获取当前的北京时间，用于日志记录和时间戳"""
    # 设置东八区时区
    tz_beijing = timezone(timedelta(hours=8))
    return datetime.now(tz_beijing)


def main():
    # --- 1. 路径自动对齐模块 ---
    # 获取当前执行代码文件的绝对路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 获取根目录（即 program 的上一级），确保 output 文件夹与 program 同级
    root_dir = os.path.dirname(current_dir)
    # 定义输出文件夹路径
    output_dir = os.path.join(root_dir, 'output')

    # 如果文件夹不存在（比如你刚删了），程序会自动创建它
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 定义三个核心文件的存放路径
    db_path = os.path.join(output_dir, 'funds_manager.db')
    csv_path = os.path.join(output_dir, 'funds_history_export.csv')
    log_path = os.path.join(output_dir, 'daily_sync.log')

    # --- 2. 数据库准备模块 ---
    # 连接数据库（如果文件不存在会自动创建）
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # 创建表：使用中文列名，并将 (基金代码, 日期) 设为主键（PRIMARY KEY）
    # 这是实现“增量更新”的核心：主键保证了同一个基金在同一个日期只能有一条记录
    cursor.execute('''CREATE TABLE IF NOT EXISTS fund_history 
                    (基金代码 TEXT, 日期 TEXT, 单位净值 REAL, 累计净值 REAL, 日涨跌幅 REAL, 
                     PRIMARY KEY (基金代码, 日期))''')

    # --- 3. 投石问路：初始化获取总数 ---
    fund_code = '023350'
    page_size = 20  # 采用网页默认的每页20条，最稳健，不容易被封IP
    headers = {
        'Referer': f'https://fundf10.eastmoney.com/jjjz_{fund_code}.html',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

    # 先请求一次第一页，目的是为了看服务器上一共有多少条数据（TotalCount）
    first_url = f"https://api.fund.eastmoney.com/f10/lsjz?fundCode={fund_code}&pageIndex=1&pageSize={page_size}"
    try:
        res = requests.get(first_url, headers=headers, timeout=20)
        first_data = res.json()
        # 提取总记录数
        total_records = int(first_data['TotalCount'])
        # 核心算法：总页数 = 总记录数 / 每页条数 (向上取整)
        total_pages = math.ceil(total_records / page_size)
        print(f"检测到基金 {fund_code} 共有 {total_records} 条数据，程序将分 {total_pages} 页进行全量扫描。")
    except Exception as e:
        print(f"初始化失败，请检查网络或接口链接: {e}")
        return

    # --- 4. 自动化分页抓取模块 ---
    new_count = 0  # 计数器：记录本次运行真正往数据库里新塞了多少条
    for page in range(1, total_pages + 1):
        print(f"[{get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}] 正在同步第 {page}/{total_pages} 页...")
        url = f"https://api.fund.eastmoney.com/f10/lsjz?fundCode={fund_code}&pageIndex={page}&pageSize={page_size}"

        try:
            response = requests.get(url, headers=headers, timeout=20)
            data = response.json()
            # 获取当前页的明细列表
            records = data['Data']['LSJZList']

            for item in records:
                # INSERT OR IGNORE：这是实现“全量+增量”的关键逻辑
                # 如果数据库里已经有了该日期，就忽略（IGNORE）；没有就插入
                cursor.execute('''
                    INSERT OR IGNORE INTO fund_history VALUES (?, ?, ?, ?, ?)
                ''', (fund_code, item['FSRQ'], item['DWJZ'], item['LJJZ'], item['JZZZL']))

                # cursor.rowcount > 0 表示这一行是真正新插入成功的
                if cursor.rowcount > 0:
                    new_count += 1

            # 每一页处理完提交一次事务，保存数据
            conn.commit()

            # 高效采集：听你的，只等 1 秒，既尊重服务器又不拖泥带水
            time.sleep(1)

        except Exception as e:
            print(f"第 {page} 页同步时发生突发错误: {e}")
            break

    # --- 5. 数据导出模块 ---
    # 从数据库读取所有记录，并按日期倒序排列（最新的在上面）
    full_df = pd.read_sql("SELECT * FROM fund_history ORDER BY 日期 DESC", conn)
    # 导出 CSV：使用 utf-8-sig 编码，确保用 Excel 打开中文不乱码
    full_df.to_csv(csv_path, index=False, encoding='utf-8-sig')

    total_count = len(full_df)
    conn.close()

    # --- 6. 日志报告模块 ---
    bj_now = get_beijing_time()
    report = (
            f"\n" + "=" * 40 + "\n"
                               f"📊 数据同步报告 | {bj_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
                               f"✅ 状态反馈: 全自动分页同步完成\n"
                               f"🆕 本次新增记录: {new_count} 条\n"
                               f"📈 数据库总条数: {total_count} 条\n"
                               f"========================================\n"
    )
    # 打印到屏幕，同时追加写入到 daily_sync.log 文件中
    print(report)
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(report)


if __name__ == "__main__":
    main()