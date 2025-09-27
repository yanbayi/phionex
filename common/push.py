import io
import time
from PIL import Image
import requests
import json

# 配置参数
CONFIG = {
    "dingtalk_webhook": "https://oapi.dingtalk.com/robot/send?access_token=8d4e9cbf96282b04e05a9710cbaddc8d55b1df257e868635987799e5459f008e",
    "table_title": "股票推荐,策略:",
}


def send_dingtalk_stock_message(data_list):
    if not data_list:
        print("没有数据可推送")
        return False

    # 构建Markdown内容
    markdown_text = f"### {CONFIG['table_title']}\n\n"
    markdown_text += f"\n概念板块:日线挑选3-5天涨幅最高，至少有3天在涨，并且成交额大的20个概念板块"
    markdown_text += f"\n股票:筛选5天内有过bbi上穿，15天内有过1天超过8%涨幅，成交量在5天内放大1.2倍以上，10天涨幅超过10%"
    markdown_text += f"\n"
    count = 0
    for row in data_list:
        count += 1
        # 提取数据并处理空值
        code, name, date, price, pct_change, concepts = row
        code = code or "未知"
        name = name or "未知"
        date = date or "未知"
        price = f"{price:.2f}" if isinstance(price, (int, float)) else str(price)
        pct_change = f"{pct_change:.2f}%" if isinstance(pct_change, (int, float)) else str(pct_change)
        # 概念板块处理：长文本换行，关键概念加粗
        # 关键信息加粗
        code = f"**{code}**"
        name = f"**{name}**"
        # 添加行数据
        markdown_text += f"{count}-代码:{code},名称:{name}\n日期:{date},价格:{price},涨跌幅:{pct_change}\n概念板块{concepts}\n\n"

    # 构建请求参数
    params = {"access_token": CONFIG["dingtalk_webhook"].split("=")[-1]}

    headers = {"Content-Type": "application/json;charset=utf-8"}
    payload = {
        "msgtype": "markdown",
        "markdown": {
            "title": CONFIG["table_title"],
            "text": markdown_text
        }
    }

    # 发送请求
    try:
        response = requests.post(
            url="https://oapi.dingtalk.com/robot/send",
            params=params,
            headers=headers,
            data=json.dumps(payload)
        )

        result = response.json()
        if result.get("errcode") == 0:
            print("钉钉消息推送成功")
            return True
        else:
            print(f"推送失败: {result.get('errmsg')}")
            return False
    except Exception as e:
        print(f"推送发生错误: {str(e)}")
        return False


def send_image_to_dingtalk(image_url, title):
    try:
        # 构建钉钉消息
        headers = {'Content-Type': 'application/json;charset=utf-8'}
        data = {
            "msgtype": "image",
            "image": {
                "picURL": image_url,
                "title": title
            }
        }
        msg_json_str = json.dumps(data, ensure_ascii=False)  # 序列化，保留中文
        msg_total_size = len(msg_json_str.encode('utf-8'))  # 计算字节数（UTF-8编码）

        print(f"3. 钉钉消息体（body）总大小：{msg_total_size} bytes")

        # 发送请求
        response = requests.post(CONFIG["dingtalk_webhook"], headers=headers, json=data)
        response.raise_for_status()  # 抛出HTTP错误
        print(f"钉钉消息发送成功，响应: {response.text}")

    except Exception as e:
        print(f"发送钉钉消息失败: {str(e)}")


# 示例用法
def common_push(data):
    # 推送示例数据
    send_dingtalk_stock_message(data)
