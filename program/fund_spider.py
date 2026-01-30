import requests  # 导入网络请求库，负责从服务器下载数据
import json  # 导入 JSON 解析库，负责把乱糟糟的文本变成字典
import re  # 导入正则表达式库，负责精准切割字符串
import csv  # 导入 CSV 库，负责把数据存入表格
import os  # 导入系统操作库，负责处理文件夹和路径
import time  # 导入时间库，负责让程序“休息”，防止被封
import math  # 导入数学库，负责用 ceil 函数进行“向上取整”

# --- 第一阶段：配置环境 ---
# --- 核心逻辑：输入你想爬的基金代码 ---
fund_code = input("请输入您想抓取的基金代码 (如 000001): ").strip()
# 定义保存文件的文件夹路径
output_dir = r"D:\quant\webCrawlerProjectPractice\output"
# 如果电脑里没有这个文件夹，就自动创建一个，防止程序报错
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# 定义最终生成文件的完整路径（文件名：基金代码_全量数据.csv）
file_path = os.path.join(output_dir, "fund_023350_full.csv")

# --- 第二阶段：定义“取餐窗口”参数 ---
# 这是我们要访问的基础地址，{} 是占位符，等会儿翻页时会填入数字
base_url = "https://api.fund.eastmoney.com/f10/lsjz?callback=jQuery183&fundCode=023350&pageIndex={}&pageSize=20"

# 伪装信息：让服务器认为我们是一个正常的 Chrome 浏览器在访问
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://fundf10.eastmoney.com/"  # 来源页，证明我们是从官网看数据的
}


def get_data(page):
    """
    定义一个功能函数：专门负责抓取指定页码的数据
    """
    url = base_url.format(page)  # 将页码填入 URL 的占位符中
    response = requests.get(url, headers=headers)  # 发起请求
    # 使用正则表达式：去掉 jQuery183(...) 外壳，只保留括号里的 JSON 字符串
    json_str = re.search(r'\((.*)\)', response.text).group(1)
    return json.loads(json_str)  # 将字符串转为 Python 的字典格式返回


# --- 第三阶段：主程序运行 ---
try:
    print("正在初始化，获取总数据量...")
    # 先抓取第 1 页，目的是为了拿到“总条数”
    first_page_data = get_data(1)
    # 从返回的字典里提取总记录数（比如 240 条）
    total_count = int(first_page_data['TotalCount'])
    # 计算总页数：总数 / 每页20条。math.ceil 是向上取整（比如 12.1 页会变成 13 页）
    total_pages = math.ceil(total_count / 20)

    print(f"检测到共有 {total_count} 条记录，分为 {total_pages} 页。开始全量抓取...")

    # 使用 'w' 模式打开文件（新建或覆盖），设置 utf-8-sig 防止 Excel 中文乱码
    with open(file_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)  # 创建一个 CSV 写入器
        writer.writerow(['日期', '单位净值', '累计净值', '日增长率'])  # 写入第一行表头

        # 开始循环：从第 1 页遍历到最后一页
        for page in range(1, total_pages + 1):
            print(f"--- 正在处理第 {page}/{total_pages} 页 ---")

            # 如果不是第一页（第一页刚才已经抓过了，直接拿来用），则发起新的请求
            if page == 1:
                page_data = first_page_data
            else:
                page_data = get_data(page)

            # 提取这一页里的“历史净值列表”
            data_list = page_data['Data']['LSJZList']

            # 遍历这一页的每一行，写入 CSV
            for row in data_list:
                writer.writerow([
                    row['FSRQ'],  # 日期
                    row['DWJZ'],  # 单位净值
                    row['LJJZ'],  # 累计净值
                    row['JZZZL']  # 日增长率
                ])

            # 关键一步：每抓完一页休息 1.2 秒，做个文明的爬虫，避免被服务器拉黑
            time.sleep(1.2)

    print(f"\n任务圆满完成！数据已安全存入：{file_path}")

except Exception as error:
    # 如果中间任何一步出错了（比如网络断了），会打印出错误原因
    print(f"哎呀，程序出错了: {error}")