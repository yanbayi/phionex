import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from common import const
import chinadata.ca_data as ts
from db_ctl.util import mongoDb_ctl
from data_ctl import tushare_ctl


def get_tushare_trading_cal(
        pro: ts.pro_api,
        start_date_str: str,
        end_date_str: str
) -> List[str]:
    # 从Tushare获取指定日期范围内的A股交易日列表（排除周末+节假日）
    try:
        # 调用Tushare交易日历接口（exchange=SSE：上交所，全市场交易日一致；is_open=1：开盘日=交易日）
        cal_df = pro.trade_cal(
            exchange="SSE",
            start_date=start_date_str,
            end_date=end_date_str,
            is_open=1
        )
        if cal_df.empty:
            # print(f"未获取到 {start_date_str}~{end_date_str} 的交易日历数据")
            return []

        # 转为字符串列表并按日期排序（确保时间顺序）
        trading_days = cal_df["cal_date"].astype(str).tolist()
        trading_days.sort()
        return trading_days
    except Exception as e:
        print(f"获取Tushare交易日历失败：{str(e)}")
        return []
#
#
# def calculate_date_range(pro: ts.pro_api,target_trading_days: int) -> Dict[str, str]:
#     # 计算包含足够交易日的时间范围（排除周末+节假日）
#     end_date = (datetime.now()).date()
#     end_date_str = end_date.strftime(const.DATE_FORMAT)
#     # 1.从Tushare确认结束日期是否为交易日，若不是则向前找最近的交易日
#     while True:
#         test_cal = get_tushare_trading_cal(pro, end_date_str, end_date_str)
#         if test_cal:  # 若返回非空列表，说明是交易日
#             break
#         # 不是交易日，向前调整1天
#         time.sleep(1)
#         end_date -= timedelta(days=1)
#         end_date_str = end_date.strftime(const.DATE_FORMAT)
#         # 安全阈值：最多向前调整10天（避免无限循环）
#         if (datetime.now().date() - end_date).days > 10:
#             print("未找到最近10天内的交易日，可能Tushare接口异常")
#             raise Exception("无法确定有效结束日期（交易日）")
#
#
#
#     # 2.估算起始日期（按1.8倍自然日估算，预留周末+节假日空间）
#     initial_estimate_days = int(target_trading_days * 1.8)
#     start_date = end_date - timedelta(days=initial_estimate_days)
#     start_date_str = start_date.strftime(const.DATE_FORMAT)
#
#     # 步骤4：循环调整起始日期，确保范围内交易日数 >= 所需总交易日数
#     while True:
#         # 获取当前[start_date_str, end_date_str]的交易日列表
#         current_trading_days = get_tushare_trading_cal(pro, start_date_str, end_date_str)
#         time.sleep(1)
#         if not current_trading_days:
#             start_date -= timedelta(days=10)  # 一次扩展10天，避免频繁调用接口
#             start_date_str = start_date.strftime(const.DATE_FORMAT)
#             continue
#
#         # 统计当前范围内的交易日数量
#         current_trading_count = len(current_trading_days)
#         if current_trading_count >= target_trading_days:
#             break
#         # 不满足需求，向前扩展起始日期（扩展天数=需求差的2倍，预留冗余）
#         need_more_days = target_trading_days - current_trading_count
#         start_date -= timedelta(days=need_more_days * 2)
#         start_date_str = start_date.strftime(const.DATE_FORMAT)
#         # 安全阈值：最多扩展至1年前（避免无限循环）
#         if (end_date - start_date).days > 365:
#             print(f"扩展至1年前仍未满足交易日需求（当前仅{current_trading_count}天），可能参数错误")
#             raise Exception("超出最大日期范围（1年），无法满足交易日需求")
#     return {
#         "start_date": start_date_str,
#         "end_date": end_date_str,
#         "trading_days": current_trading_days  # 额外返回范围内交易日列表
#     }

def get_end_date(pro: ts.pro_api, date: datetime) -> datetime: # 确定结束日期（前1个自然日，且为交易日）
    end_date_str = date.strftime(const.DATE_FORMAT)
    # 从Tushare确认结束日期是否为交易日，若不是则向前找最近的交易日
    while True:
        test_cal = get_tushare_trading_cal(pro, end_date_str, end_date_str)
        if test_cal:  # 若返回非空列表，说明是交易日
            break
        # 不是交易日，向前调整1天
        date -= timedelta(days=1)
        end_date_str = date.strftime(const.DATE_FORMAT)
        # 安全阈值：最多向前调整10天（避免无限循环）
        if (datetime.now() - date).days > 10:
            print("未找到最近10天内的交易日，可能Tushare接口异常")
            raise Exception("无法确定有效结束日期（交易日）")
    return datetime.strptime(end_date_str, const.DATE_FORMAT)


def get_start_end_date(pro: ts.pro_api, days: int, update: bool, is_tdx: bool) -> (bool, datetime, datetime, bool): # 是否要更新， 开始时间，结束时间， 是否是更新
    conf_coll = mongoDb_ctl.init_mongo_collection(const.CONF_COLL)
    # today = datetime.strptime("20250912 19:00:00", "%Y%m%d %H:%M:%S")
    today = datetime.now()
    start_date = datetime.now()
    end_date = datetime.now()
    if today.hour <= 18:
        today -= timedelta(days=1)
    today = get_end_date(pro, today)
    print("最新的交易日：", today)
    name = "daily_up_date"
    if is_tdx:
        name = "tdx_daily_up_date"
    if update:  # 如果是更新则先算要拉多少天的数据
        concept_info = conf_coll.find_one({"name": name})
        up_date = concept_info.get("value", "")
        print("当前交易日期：", up_date)
        if up_date == "":  # 没有最新日期的当作全量更新
            update = False
            # if not is_tdx:
            #     days += 50
            start_date = today - timedelta(days=days)
            end_date = today
        else:
            date1 = datetime.strptime(today.strftime(const.DATE_FORMAT), const.DATE_FORMAT)
            date2 = datetime.strptime(up_date, const.DATE_FORMAT)
            delta = date1 - date2
            if delta.days <= 0:
                print("当前最新日期：", date2, "不需要在更新数据, 当天股票信息19点后刷新")
                return False, start_date, end_date, update
            else:
                start_date = date2 + timedelta(days=1)
                end_date = date1
    else:
        # if not is_tdx:
        #     days += 50
        start_date = today - timedelta(days=days)
        end_date = today
    return True, start_date, end_date, update
