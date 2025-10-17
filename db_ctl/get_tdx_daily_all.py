# 通达信板块日线获取
import time
from datetime import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
from pymongo import errors
from tqdm import tqdm  # 进度条库（核心新增）
from common import const, utils
from data_ctl import tushare_ctl
from db_ctl.util import mongoDb_ctl


def main_get_tdx_daily_all():
    day = 360
    print("=" * 60)
    print(f"启动通达信板块{day}日日线数据增量拉取流程")
    print("=" * 60)
    pro = tushare_ctl.init_tushare_client()
    tdx_index_coll = mongoDb_ctl.init_mongo_collection(const.TDX_INDEX)
    tdx_daily_coll = mongoDb_ctl.init_mongo_collection(const.TDX_DAILY)
    conf_coll = mongoDb_ctl.init_mongo_collection(const.CONF_COLL)
    _, _, end_date = utils.get_start_end_date(pro, False)
    now = datetime.now()
    start_date_str = (now - relativedelta(years=1)).strftime(const.DATE_FORMAT)
    end_date_str = now.strftime(const.DATE_FORMAT)
    tdx_daily_coll.delete_many({})  # 全量更新先清空数据
    print(f"获取日线开始日期：{start_date_str}, 获取日线结束日期: {end_date_str}")
    try:
        stock_cursor = tdx_index_coll.find({}, projection={"ts_code": 1, "_id": 0})
        stock_list = list({doc["ts_code"] for doc in stock_cursor if "ts_code" in doc})
        if not stock_list:
            print("从tdx_index集合未读取到任何有效板块代码，终止流程")
            return
        print(f"成功读取{len(stock_list)}个有效板块代码（示例：{stock_list[:3]}）")
    except Exception as e:
        print(f"读取tdx_index集合失败：{str(e)}")
        return

    with tqdm(total=len(stock_list), desc="拉取日线数据进度", unit="个") as batch_pbar:
        for ts_code in stock_list:
            # 拉取数据
            try:
                df = pro.tdx_daily(ts_code=ts_code, start_date=start_date_str, end_date=end_date_str)
                if df.empty:
                    print(f"板块{ts_code}在{start_date_str}~{end_date_str}期间无数据")
                    continue
                df["up_time"] = datetime.now()
                mongo_fields = ["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change",
                                "pct_change", "vol", "amount", "rise", "vol_ratio", "turnover_rate", "swing", "up_num",
                                "limit_up_num", "lu_days", "3day", "5day", "10day", "20day", "60day", "up_time",
                                "bm_buy_net", "bm_buy_ratio", "bm_net", "bm_ratio"]
                df = df[mongo_fields].copy()
            except Exception as e:
                print(f" 拉取失败：{str(e)}")
            # 批量插入MongoDB
            data_list = df.to_dict("records")
            for doc in data_list:
                for key, value in doc.items():
                    if pd.isna(value) or value is None:
                        doc[key] = None
                        if key in ["turnover_rate", "up_num", "limit_up_num", "lu_days",
                                   "3day", "5day", "10day", "20day", "60day", "bm_buy_net", "bm_buy_ratio", "bm_net",
                                   "bm_ratio"]:
                            doc[key] = 0
                        else:
                            print("缺失字段: ", key)
            try:
                tdx_daily_coll.insert_many(data_list, ordered=False)
            except errors.BulkWriteError as e:
                details = e.details
                inserted_count = details.get("nInserted", 0)  # 成功插入数
                duplicate_count = details.get("nDuplicates", 0)  # 重复数据数
                write_errors = details.get("writeErrors", [])  # 具体错误列表（如字段类型错误）
                print(f"批量插入部分成功：成功插入{inserted_count}条，跳过重复{duplicate_count}条")
                if write_errors:
                    for i, err in enumerate(write_errors[:3]):  # 只打印前3个，避免日志过长
                        err_msg = err.get("errmsg", "未知错误")
                        err_doc = err.get("op", {})  # 导致错误的具体文档
                        print(
                            f"  错误{i + 1}：msg={err_msg}，涉及文档：ts_code={err_doc.get('ts_code')}, trade_date={err_doc.get('trade_date')}")
                return inserted_count
            except Exception as e:
                print(f"插入MongoDB失败（致命错误）：{str(e)}")
                return 0
            time.sleep(0.5)
            batch_pbar.update(1)

    # 输出最终结果
    conf_coll.update_one({"name": "tdx_daily_up_date"}, {"$set": {"value": end_date_str}})
    conf_coll.update_one({"name": "tdx_daily_start_date"}, {"$set": {"value": start_date_str}})
    print("=" * 60)
    print(f"通达信板块{day}日日线数据拉取流程完成")
    print(f"覆盖交易日：{start_date_str} ~ {end_date_str}")
    print("=" * 60)


if __name__ == "__main__":
    main_get_tdx_daily_all()
