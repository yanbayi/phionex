import base64
import io
import os
import re
import sys
from dateutil.relativedelta import relativedelta
import requests
from PIL import Image  # 用于图片压缩
import chinadata.ca_data as ts
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime
from numba import typeof
from util import mongoDb_ctl
from common import const, push, utils


pro = ts.pro_api('j80872d274089319b71f9e5ca3966abf009')
# pro = ts.pro_api('j80872d2740ca3966abf009')
# df = pro.stock_basic(list_status='L', exchange='', fields='ts_code', limit=1)
# if type(df) != type(pd.DataFrame):
#     print(df)
#     sys.exit(1)
# else:
#     print(df.to_string())


# df = pro.daily(ts_code='000001.SZ', start_date='20250922', end_date='20250922')
# print(df)

# # df = pro.tdx_daily(ts_code="880633.TDX", start_date="20250822", end_date="20250905")
# # df = pro.stk_factor_pro(start_date="20080611", end_date="20080611", ts_code="000559.SZ")
# df = pro.adj_factor(ts_code='000001.SZ', start_date="20080801", end_date="20080805")
# print(df.to_string())
# df = pro.adj_factor(ts_code='000001.SZ', start_date="20251010")
# print(df.to_string())
# df = pro.daily(ts_code='000001.SZ', start_date="20080801", end_date="20080805")
# # try:
# #     df["date"] = pd.to_datetime(df["trade_date"], format=const.DATE_FORMAT)
# # except ValueError as e:
# #     raise ValueError(f"日期转换失败: {str(e)}")
# # df = df.sort_values(by=['ts_code', 'date']).reset_index(drop=False)
# print(df.to_string())
#
# # df = ts.pro_bar(ts_code='000001.SZ',asset = 'E', freq = 'D',adj='qfq', start_date='20080801', end_date='20080805')
# # print(df.to_string())
# df = pro.bak_daily(
#     ts_code="000001.SZ",
#     start_date="20080801",
#     end_date="20080805",
#     fields="""ts_code,trade_date,open,high,low,close"""
# )
# print(df.to_string())
# df = pro.stk_factor_pro(start_date="20080801",end_date="20080805", ts_code="000001.SZ",fields="""ts_code,trade_date,open,high,low,close,close_qfq""")
# # # ,fields="""ts_code,trade_date,open,high,low,close,
# # # pre_close,change,pct_change,vol,amount,turnover_rate,turnover_rate_f,volume_ratio,bbi_bfq"""
# print(df.to_string())

# df = pro.tdx_index(trade_date="20250922",idx_type="概念板块")
# print(df.to_string())

# df = total_count = pro.stock_basic(
#     list_status='L',
#     market='主板,创业板',
#     fields='ts_code'  # 仅获取计数所需字段，减少数据传输
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

