import chinadata.ca_data as ts
import logging
from common import const

# 配置日志 - 同时输出到控制台和文件
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("log.log"),  # 日志文件
        logging.StreamHandler()  # 控制台输出
    ]
)
logger = logging.getLogger(__name__)

def init_tushare_client():
    try:
        ts.set_token(const.TUSHARE_TOKEN)
        pro = ts.pro_api()
        logger.info("Tushare客户端初始化中")
        logger.info("尝试获取一条数据:")
        df = pro.stock_basic(list_status='L', exchange='', fields='ts_code', limit=1)
        print(df)
        logger.info("Tushare客户端初始化成功")
        return pro
    except ValueError as e:
        logger.error(f"Tushare配置错误：{str(e)}")
        raise
    except Exception as e:
        error_msg = str(e)
        if "invalid token" in error_msg.lower():
            logger.error("Tushare Token无效，请在网页端检查Token是否正确")
        elif "permission denied" in error_msg.lower():
            logger.error("Tushare接口权限不足，请在网页端完成实名认证并申请A股基础信息权限")
        else:
            logger.error(f"Tushare客户端初始化失败：{error_msg}")
        raise