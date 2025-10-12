import sys
import chinadata.ca_data as ts
import pandas as pd
from common import const

def init_tushare_client():
    try:
        ts.set_token(const.TUSHARE_TOKEN)
        pro = ts.pro_api()
        df = pro.stock_basic(list_status='L', exchange='', fields='ts_code', limit=1)
        if type(df) != type(pd.DataFrame):
            print(df)
            sys.exit(1)
        return pro
    except ValueError as e:
        print(f"Tushare配置错误：{str(e)}")
        raise