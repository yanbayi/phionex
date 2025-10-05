# Tushare API密钥
TUSHARE_TOKEN = 'j80872d274089319b71f9e5ca3966abf009'

# mongoDB配置 数据库信息
MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
DB_NAME = 'stock'
# mongoDB配置 表信息
CONF_COLL = "conf_daily"  # 基础配置表
MONGO_DAILY_COLL = "a_share_daily"  # 日线集合名
MONGO_BASIC_COLL = "a_share_basic"  # 基础信息集合名（用于获取股票列表）
TDX_INDEX = "a_tdx_index"  # 通达信板块信息
TDX_MEMBER = "a_tdx_member"  # 通达信板块成分
TDX_DAILY = "a_tdx_daily"  # 通达信板块行情

# 所有日期格式化
DATE_FORMAT = "%Y%m%d"

# 指标公式
BBI_PERIODS = [3, 6, 12, 20, 24]  # BBI指数计算周期（3/6/12/20 24日均线）
MA_PERIODS = [5, 6, 10, 20, 60]
MACD_PERIODS = [12, 26, 9]
RSI_PERIODS = [6, 12, 24]
# 指标获取
VALID_IDX_TYPES = ["概念板块", "行业板块", "风格板块", "地区板块"]
