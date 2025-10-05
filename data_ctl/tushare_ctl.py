import chinadata.ca_data as ts
from common import const

def init_tushare_client():
    try:
        ts.set_token(const.TUSHARE_TOKEN)
        pro = ts.pro_api()
        print("==============Tushare客户端初始化中")
        print("尝试获取一条数据:")
        df = pro.stock_basic(list_status='L', exchange='', fields='ts_code', limit=1)
        print(df)
        print("==============Tushare客户端初始化成功")
        return pro
    except ValueError as e:
        print(f"Tushare配置错误：{str(e)}")
        raise