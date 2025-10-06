import base64
import io
import os
import re

import requests
from PIL import Image  # 用于图片压缩
import chinadata.ca_data as ts
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime
from util import mongoDb_ctl
from common import const, push, utils


pro = ts.pro_api('j80872d274089319b71f9e5ca3966abf009')
# daily_coll = mongoDb_ctl.init_mongo_collection(const.MONGO_DAILY_COLL)
# basic_coll = mongoDb_ctl.init_mongo_collection(const.MONGO_BASIC_COLL)
# #
# cc = daily_coll.find({"ts_code":"000559.SZ"},{"trade_date":1,"close":1,"_id":0})
# count = 0
# ma3 = 0
# ma6 = 0
# ma12 = 0
# ma24 = 0
# ma20 = 0
# ma21 = 0
# ma22 = 0
# for i, c in enumerate(cc):
#     count += c["close"]
#     if i ==2:
#         ma3 = count/3
#     if i ==5:
#         ma6 = count/6
#     if i ==11:
#         ma12 = count/12
#     if i ==23:
#         ma24 = count/24
#     if i ==19:
#         ma20 = count/20
#     if i ==20:
#         ma21 = count/21
#     if i ==21:
#         ma22 = count/22
#
# print(ma3, ma6, ma12, ma24, ma20, ma21, ma22)
# print((ma3+ma6+ma12+ma24)/4, (ma3+ma6+ma12+ma20)/4, (ma3+ma6+ma12+ma21)/4, (ma3+ma6+ma12+ma22)/4)



# df = pro.daily(ts_code='000001.SZ', start_date='20250922', end_date='20250922')
# print(df)


#
# days = 30
# end_date = (datetime.now() - timedelta(days=1)).strftime(const.DATE_FORMAT)
# start_date = (datetime.now() - timedelta(days=days + const.BBI_PERIODS[3])).strftime(const.DATE_FORMAT)
# print(const.BBI_PERIODS[3])
# print(start_date)
# print(end_date)


# df = pro.tdx_daily(ts_code="880633.TDX", start_date="20250822", end_date="20250830")
# print(df.to_string())

# df = pro.tdx_index(trade_date="20250922",idx_type="概念板块")
# print(df.to_string())

# df = total_count = pro.stock_basic(
#     list_status='P',
#     exchange='',
#     fields='ts_code'  # 仅获取计数所需字段，减少数据传输   L上市 D退市 P暂停上市，默认是L
# )
# print(df.to_string())

# df = pro.stk_factor_pro(start_date="20250928",end_date="20250930", ts_code="000559.SZ",fields="""ts_code,trade_date,open_qfq,high_qfq,low_qfq,close_qfq,pre_close,change,pct_chg,vol,
#                         amount,turnover_rate,turnover_rate_f,volume_ratio,pe,pb,ps,total_share,float_share,free_share,
#                         total_mv,circ_mv,
#                         asi_qfq,asit_qfq,
#                         atr_qfq,
#                         bbi_qfq,
#                         bias1_qfq,bias2_qfq,bias3_qfq,
#                         boll_lower_qfq,boll_mid_qfq,boll_upper_qfq,
#                         brar_ar_qfq,brar_br_qfq,
#                         cr_qfq,
#                         kdj_qfq,kdj_d_qfq,kdj_k_qfq,
#                         ma_qfq_5,ma_qfq_10,ma_qfq_20,ma_qfq_60,ma_qfq_90,
#                         macd_qfq,macd_dea_qfq,macd_dif_qfq,
#                         obv_qfq,
#                         rsi_qfq_6,rsi_qfq_12,rsi_qfq_24
#                         """)
# # trade_date,open,high,low,close,pre_close,change,pct_change,vol,amount,vol_ratio,turn_over,swing,selling,buying,strength,activity,attack,avg_price
# print(df.to_string())

# df = pro.tdx_daily(ts_code="880558.TDX", start_date="20250924", end_date="20250924")


# start_time = datetime.datetime.strptime("2025-09-25 09:00:00", '%Y-%m-%d %H:%M:%S')
# end_time = datetime.datetime.strptime("2025-09-25 10:00:00", '%Y-%m-%d %H:%M:%S')
# print(type(start_time))
# print(type(end_time))
# df = pro.news(start_date=start_time,end_date=end_time, src="sina")
# print(df)

# df = pro.bak_daily(
#     ts_code="000001.SZ,000004.SZ",
#     start_date="20250925",
#     end_date="20250925",
#     fields="""ts_code,trade_date,open,high,low,close,pre_close,change,pct_change,vol,amount,vol_ratio,
#         turn_over,swing,selling,buying,strength,activity,attack,avg_price"""
# )
# print(df.to_string())

#
# df = pro.stk_factor_pro(start_date="20250924",end_date="20250925", ts_code="601218.SH")
# # # ,fields="""ts_code,trade_date,open,high,low,close,
# # # pre_close,change,pct_change,vol,amount,turnover_rate,turnover_rate_f,volume_ratio,bbi_bfq"""
# print(df.to_string())


# # ============== 画图
# # 设置中文显示
# plt.rcParams["font.family"] = ["SimHei"]
# plt.rcParams["axes.unicode_minus"] = False  # 正确显示负号
#
#
# def plot_stock_daily_chart(df):
#     # 确保日期格式正确
#     df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
#     df = df.sort_values(by='trade_date', ascending=True).reset_index(drop=True)
#     df['trade_index'] = range(len(df))
#
#
#     # 创建画布
#     fig, ax = plt.subplots(figsize=(12,10))
#
#     # 计算价格范围并设置固定间隔的Y轴刻度
#     prices = df[['open', 'close', 'high', 'low']]
#     min_price = prices.min().min()
#     max_price = prices.max().max()
#     price_range = max_price - min_price  # 价格波动范围
#     intervals = [
#         (0, 1), 0.05,  # 价格波动0-1，间隔0.05
#         (1, 5), 0.1,  # 价格波动1-5，间隔0.1
#         (5, 10), 0.5,  # 价格波动5-10，间隔0.5
#         (10, 50), 1,  # 价格波动10-50，间隔1
#         (50, 100), 5,  # 价格波动50-100，间隔5
#         (100, 500), 10,  # 价格波动100-500，间隔10
#         (500, 1000), 50,  # 价格波动500-1000，间隔50
#         (1000, float('inf')), 100  # 价格波动1000以上，间隔100
#     ]
#
#     # 找到适合当前价格范围的间隔
#     y_tick_interval=1
#
#     # for i in range(0, len(intervals), 2):
#     #     range_min, range_max = intervals[i]
#     #     if range_min <= price_range < range_max:
#     #         y_tick_interval = intervals[i + 1]
#
#     # 调整最小/最大价格到刻度间隔的整数倍，确保刻度对齐
#     adjusted_min = (min_price // y_tick_interval) * y_tick_interval
#     adjusted_max = ((max_price // y_tick_interval) + 1) * y_tick_interval
#
#     # 设置Y轴固定刻度
#     ax.set_ylim(adjusted_min - y_tick_interval, adjusted_max + y_tick_interval)
#     ax.yaxis.set_major_locator(plt.MultipleLocator(y_tick_interval))
#     ax.tick_params(axis='y', labelsize=18)  # 设置Y轴刻度字体大小
#
#     ax.plot(df['trade_index'], df['close'], label='收盘价', color='black', linewidth=1)
#     # ax.plot(df['trade_index'], df['bbi_bfq'], label='BBI线', color='purple', linewidth=1, alpha=0.8)
#     ax.plot(df['trade_index'], df['asi_bfq'], label='asi_bfq线', color='purple', linewidth=1, alpha=0.8)
#
#     # 绘制涨跌柱形（K线实体部分）
#     up = df[df['close'] >= df['open']]  # 上涨
#     down = df[df['close'] < df['open']]  # 下跌
#     flat = df[df['close'] == df['open']]  # 平盘（开盘价等于收盘价）
#
#     # 上涨柱体（红色）
#     ax.bar(up['trade_index'], up['close'] - up['open'], bottom=up['open'],
#            color='red', width=0.3, alpha=0.7, label='_nolegend_')
#     # 下跌柱体（绿色）
#     ax.bar(down['trade_index'], down['close'] - down['open'], bottom=down['open'],
#            color='green', width=0.3, alpha=0.7, label='_nolegend_')
#     if not flat.empty:
#         ax.hlines(flat['open'], flat['trade_index'] - 0.3, flat['trade_index'] + 0.3,
#                   color='gray', linewidth=3, label='_nolegend_')
#     # 绘制最高价和最低价（影线）
#     ax.vlines(df['trade_index'], df['low'], df['high'], color='gray', linewidth=1)
#
#     # 关键：从固定Y轴刻度引出水平虚线（贯穿整个图表）
#     x_min, x_max = ax.get_xlim()
#     for y_tick in ax.get_yticks():
#         # 只绘制在价格范围内的刻度线
#         if adjusted_min <= y_tick <= adjusted_max:
#             ax.axhline(y=y_tick, xmin=x_min, xmax=x_max, color='gray', linewidth=1, linestyle='--', alpha=1)
#
#
#     ax.set_xticks(df['trade_index'])  # 每5个交易日显示一个标签（避免拥挤）
#     ax.set_xticklabels(df['trade_date'].dt.strftime('%m-%d'), rotation=90, fontsize=15)
#     # 设置x轴日期格式
#     # ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
#     # ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=[0,1,2,3, 4]))
#     # plt.xticks(rotation=45)
#
#     # 设置标题和标签
#     ax.set_title(f'{df["ts_code"].iloc[0]} {df["name"].iloc[0]} 日线图', fontsize=20)
#     ax.set_xlabel('日期', fontsize=20)
#     ax.set_ylabel('价格', fontsize=20)
#
#     # 添加网格和图例
#     # ax.grid(True, linestyle='--', alpha=0.5)
#     ax.grid(False)
#     ax.legend(loc='upper left', fontsize=20)
#     # 调整布局
#     plt.tight_layout()
#     return fig
#
# # 上传图片到imgurl.org
# def upload_image_to_imgurl(image_path):
#     url = "https://www.imgurl.org/api/v2/upload"
#     files = {'file': open(image_path, 'rb')}
#     data = {
#         'uid': "eaa5590927cdb4d7e0412cbc9fd7e363",
#         'token': "96f820ca85ee4575fe8ee0ce22efd5d7"
#     }
#     headers = {
#         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
#         'Accept': 'application/json'
#     }
#     response = requests.post(url,
#             files=files,
#             data=data,
#             headers=headers,
#             timeout=10)
#     json_data = response.json()
#     print(json_data)
#     return json_data['data']['url']
#
# # 示例数据和使用
# if __name__ == "__main__":
#     # ts_code = "002471.SZ"
#     # names = basic_coll.find_one({'ts_code': ts_code},{"name":1})
#     df = pro.stk_factor_pro(start_date="20250701",end_date="20250929", ts_code="600103.SH",fields="""ts_code,trade_date,open,high,low,close,
# asi_bfq""")
#     df["name"] ="601218.SH"
#     print(df.to_string())
#     fig = plot_stock_daily_chart(df)
#     plt.show()

    # temp_image_path = "temp_image.png"
    # fig.savefig(temp_image_path)
    # 替换为你的钉钉机器人Webhook URL
    # uploaded_image_url = upload_image_to_imgurl(temp_image_path)
    # push.send_image_to_dingtalk(uploaded_image_url, names["name"])
    # 删除临时图片
    # if os.path.exists(temp_image_path):
    #     os.remove(temp_image_path)
    # print("正在压缩图片并展示结果...")
    # compressed_img = ultra_compress_image(fig, quality=100)
    # img_base64 = compress_image(fig, quality=60, img_format='JPEG')





# ===========================计算日期==========================
# date1 = datetime.datetime.strptime((datetime.datetime.now() - datetime.timedelta(days=5)).strftime(const.DATE_FORMAT),const.DATE_FORMAT)
# date2 = datetime.datetime.strptime("20251001", const.DATE_FORMAT)
#
# # 计算日期差（timedelta对象）
# delta = date1 - date2
# print(date2, date1)
# # 返回天数差的绝对值
# print(delta.days)

# today = datetime.datetime.now()
# if today.hour < 14:
#     today -= datetime.timedelta(days=1)
# print(today)

condition_results = {
        "1": ['881319.TDX'],
        "2": ['881319.TDX', '880904.TDX', '880912.TDX'],
        "3": ['880912.TDX', '880608.TDX']
    }
# "1 and 2",
# "1 or 2",
# "1 and (2 or 3)",
# "(1 or 2) and 3"
logic_expr = "2or(3or1)"
expr = logic_expr.replace('（', '(').replace('）', ')')
expr = expr.replace('and', '&').replace('or', '|')
# 3. 提取表达式中的所有数字键
keys = re.findall(r'\b\d+\b', expr)

# 4. 验证所有键都存在于condition_results中
for key in keys:
    if key not in condition_results:
        raise ValueError(f"表达式中的键 '{key}' 不存在于数据中")

# 5. 将数字键替换为对应的集合
for key in keys:
    # 使用正则确保只匹配独立的数字键，避免部分匹配
    expr = re.sub(rf'\b{key}\b', f"set(condition_results['{key}'])", expr)

try:
    # 6. 执行表达式计算
    result_set = eval(expr)

    # 7. 转换为排序后的列表并返回
    print(sorted(list(result_set)))
except Exception as e:
    raise ValueError(f"表达式执行错误: {str(e)}, 转换后的表达式: {expr}")