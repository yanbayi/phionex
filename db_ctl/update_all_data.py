import get_tdx_basic
import get_tdx_daliy
import get_daily_share
import get_all_stock_basic
import logging

if __name__ == "__main__":
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

    try:
        # 获取全部A股信息
        get_all_stock_basic.main_get_all_stock_basic()
        # 获取日线 60天
        # get_daily_share.main_get_daily_share(60, False) # 全部删除，全部更新，适合第一次运行或者修改了获取天数
        get_daily_share.main_get_daily_share(60, True) # 只更新最新的
        # 获取通达信概念板块信息
        get_tdx_basic.main_get_tdx_basic()
        # 获取通达信概念板块日线 60天
        # get_tdx_daliy.main_get_tdx_daily(60, False)  # 全部删除，全部更新，适合第一次运行或者修改了获取天数
        get_tdx_daliy.main_get_tdx_daily(60, True) # 只更新最新的


    except Exception as e:
        logger.warning(f"脚本执行失败，）", e)


