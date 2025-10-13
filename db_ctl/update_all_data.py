import time
import get_tdx_basic
import get_tdx_daily
import get_daily_share
import get_all_stock_basic
import update_all_stock_formula
import get_tdx_daily_all
import get_daily_share_all

if __name__ == "__main__":

    try:
        time1 = time.time()
        # 获取全部A股信息
        # get_all_stock_basic.main_get_all_stock_basic()
        time2 = time.time()

        # 获取日线
        # get_daily_share_all.main_get_daily_share_all() # 第一次运行
        get_daily_share.main_get_daily_share()  # 只更新最新的
        time3 = time.time()

        # 获取通达信概念板块信息
        get_tdx_basic.main_get_tdx_basic()
        time4 = time.time()

        # 获取通达信概念板块日线
        # get_tdx_daily_all.main_get_tdx_daily_all() # 第一次运行
        get_tdx_daily.main_get_tdx_daily()  # 只更新最新的
        time5 = time.time()

        # 计算所有指标
        stockFormula = update_all_stock_formula.StockFormula()
        stockFormula.formula_main()
        time6 = time.time()

        print(f"获取全部A股信息耗时: {time2 - time1:.6f} 秒")
        print(f"获取获取日线耗时: {time3 - time2:.6f} 秒")
        print(f"获取概念板块信息耗时: {time4 - time3:.6f} 秒")
        print(f"获取概念板块日线耗时: {time5 - time4:.6f} 秒")
        print(f"计算日线指标耗时: {time6 - time5:.6f} 秒")
    except Exception as e:
        print(f"脚本执行失败，）", e)
