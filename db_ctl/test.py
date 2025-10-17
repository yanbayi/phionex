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
from db_ctl.util import mongoDb_ctl
from common import const, push, utils

pro = ts.pro_api('j80872d274089319b71f9e5ca3966abf009')
###################################  stock_basic ##########################################
# df = total_count = pro.stock_basic(
#     list_status='L',
#     market='主板,创业板',
#     fields='ts_code'  # 仅获取计数所需字段，减少数据传输
# )
# if type(df) != type(pd.DataFrame):
#     print(df)
#     sys.exit(1)
# else:
#     print(df.to_string())



###################################  daily ##########################################
# df = pro.daily(ts_code='000001.SZ', start_date='20250922', end_date='20250922')
# print(df)



###################################  tdx_daily ##########################################
# # df = pro.tdx_daily(ts_code="880633.TDX", start_date="20250822", end_date="20250905")



###################################  adj_factor ##########################################
# df = pro.adj_factor(ts_code='000001.SZ', start_date="20080801", end_date="20080805")
# print(df.to_string())



####################################  pro_bar ##########################################
# # df = ts.pro_bar(ts_code='000001.SZ',asset = 'E', freq = 'D',adj='qfq', start_date='20080801', end_date='20080805')
# # print(df.to_string())
# df = pro.bak_daily(
#     ts_code="000001.SZ",
#     start_date="20080801",
#     end_date="20080805",
#     fields="""ts_code,trade_date,open,high,low,close"""
# )
# print(df.to_string())

##################################  stk_factor_pro   ##################################
# ts_code,trade_date,open_qfq,high_qfq,low_qfq,close_qfq,pre_close,change,pct_chg,vol,
# amount,turnover_rate,turnover_rate_f,volume_ratio,pe,pb,ps,total_share,float_share,free_share,
# total_mv,circ_mv,
# asi_qfq,asit_qfq,
# atr_qfq,
# bbi_qfq,
# bias1_qfq,bias2_qfq,bias3_qfq,
# boll_lower_qfq,boll_mid_qfq,boll_upper_qfq,
# brar_ar_qfq,brar_br_qfq,
# cr_qfq,
# kdj_qfq,kdj_d_qfq,kdj_k_qfq,
# ma_qfq_5,ma_qfq_10,ma_qfq_20,ma_qfq_60,ma_qfq_90,
# macd_qfq,macd_dea_qfq,macd_dif_qfq,
# obv_qfq,
# rsi_qfq_6,rsi_qfq_12,rsi_qfq_24


# df = pro.stk_factor_pro(start_date="20250827",end_date="20250909", ts_code="605599.SH",fields="""ts_code,trade_date,
# ma_qfq_5,ma_qfq_10,ma_qfq_20,ma_qfq_60,
# bbi_qfq,
# macd_dif_qfq,macd_dea_qfq,macd_qfq,
# kdj_k_qfq,kdj_d_qfq,kdj_qfq,
# boll_mid_qfq,boll_upper_qfq,boll_lower_qfq
# """)
# df["date"] = pd.to_datetime(df["trade_date"], format=const.DATE_FORMAT)
# df_sorted = df.sort_values(by=['ts_code', 'date'])
# print(df_sorted.to_string())



##################################################  daily_basic     ######################################
df = pro.daily_basic(
    ts_code="000001.SZ,000589.SZ",
    start_date="20250910",
    end_date="20251016",
    fields="""ts_code,turnover_rate,turnover_rate_f,trade_date,total_share,float_share,free_share,total_mv,circ_mv"""
)
# df['huanshou'] = df[]
print(df.to_string())

##################################################  tdx_index     ######################################
# df = pro.tdx_index(trade_date="20250922",idx_type="概念板块")
# print(df.to_string())

# df = total_count = pro.stock_basic(
#     list_status='L',
#     market='主板,创业板',
#     fields='ts_code'  # 仅获取计数所需字段，减少数据传输
# )
# print(df.to_string())

# df = pro.tdx_daily(ts_code="880558.TDX", start_date="20250924", end_date="20250924")



##################################################  bak_daily     ######################################
# df = pro.bak_daily(
#     ts_code="000001.SZ,000004.SZ",
#     start_date="20250925",
#     end_date="20250925",
#     fields="""ts_code,trade_date,open,high,low,close,pre_close,change,pct_change,vol,amount,vol_ratio,
#         turn_over,swing,selling,buying,strength,activity,attack,avg_price"""
# )
# print(df.to_string())


