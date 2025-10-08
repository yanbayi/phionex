import time
import get_tdx_basic
import get_tdx_daliy
import get_daily_share
import get_all_stock_basic

if __name__ == "__main__":

    try:
        # 获取全部A股信息
        time1 = time.time()
        get_all_stock_basic.main_get_all_stock_basic()
        time2 = time.time()
        # 获取日线 90天
        # get_daily_share.main_get_daily_share(180, False) # 全部删除，全部更新，适合第一次运行或者修改了获取天数
        get_daily_share.main_get_daily_share(180, True) # 只更新最新的
        time3 = time.time()
        # 获取通达信概念板块信息
        get_tdx_basic.main_get_tdx_basic()
        time4 = time.time()
        # 获取通达信概念板块日线 60天
        # get_tdx_daliy.main_get_tdx_daily(180, False)  # 全部删除，全部更新，适合第一次运行或者修改了获取天数
        get_tdx_daliy.main_get_tdx_daily(180, True) # 只更新最新的
        time5 = time.time()
        print(f"获取全部A股信息耗时: {time2 - time1:.6f} 秒")
        print(f"获取获取日线耗时: {time3 - time2:.6f} 秒")
        print(f"获取概念板块信息耗时: {time4 - time3:.6f} 秒")
        print(f"获取概念板块日线耗时: {time5 - time4:.6f} 秒")
    except Exception as e:
        print(f"脚本执行失败，）", e)


