import time
from db_ctl import get_tdx_basic
from db_ctl import get_tdx_daily
from db_ctl import get_daily_share
from db_ctl import get_all_stock_basic
from db_ctl import update_all_stock_formula
from db_ctl import get_tdx_daily_all
from db_ctl import get_daily_share_all


def update_all_data_main() -> str | None:
    try:
        time1 = time.time()
        # 获取全部A股信息
        err = get_all_stock_basic.main_get_all_stock_basic()
        if err is not None:
            return err
        time2 = time.time()

        # 获取日线
        # get_daily_share_all.main_get_daily_share_all() # !!! 只能第一次使用时运行，大概执行3小时
        err = get_daily_share.main_get_daily_share()  # 只更新最新的
        if err is not None:
            return err
        time3 = time.time()

        # 获取通达信概念板块信息
        err = get_tdx_basic.main_get_tdx_basic()
        if err is not None:
            return err
        time4 = time.time()

        # 获取通达信概念板块日线
        # get_tdx_daily_all.main_get_tdx_daily_all() # !!! 只能第一次使用时运行，大概执行1小时
        err = get_tdx_daily.main_get_tdx_daily()  # 只更新最新的
        if err is not None:
            return err
        time5 = time.time()

        print(f"获取全部A股信息耗时: {time2 - time1:.6f} 秒")
        print(f"获取获取日线耗时: {time3 - time2:.6f} 秒")
        print(f"获取概念板块信息耗时: {time4 - time3:.6f} 秒")
        print(f"获取概念板块日线耗时: {time5 - time4:.6f} 秒")
        return None
    except Exception as e:
        err = "脚本执行失败:"+ str(e)
        print(err)
        return err


if __name__ == "__main__":
    update_all_data_main()
