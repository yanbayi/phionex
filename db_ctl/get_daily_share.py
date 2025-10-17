import time
from pymongo import errors
import pandas as pd
from datetime import timedelta
from tqdm import tqdm
from typing import List, Dict
from common import utils, const
from data_ctl import tushare_ctl
from db_ctl.util import mongoDb_ctl

err_tscode_list = {}
def insert_batch_data(daily_coll, df: pd.DataFrame) -> int:
    try:
        data_list: List[Dict] = df.to_dict("records")
        for idx, data in enumerate(data_list):
            for k, v in data.items():
                if pd.isna(v) or v is None:
                    err_tscode_list[data["ts_code"]] = 1
                    # print(f"数据出错：{data["ts_code"]}的 {k} 为空：{data[k]}")
                    data[k] = 0
    except Exception as e:
        print(f"数据预处理失败（无法转为MongoDB兼容格式）：{str(e)}")
        return 0
    try:
        inserted_count = 0
        for data in data_list:
            result = daily_coll.update_one({"ts_code": data["ts_code"], "trade_date": data["trade_date"]},
                                           {"$set": data}, upsert=True)
            inserted_count += 1

        return inserted_count
    except errors.BulkWriteError as e:
        details = e.details
        inserted_count = details.get("nInserted", 0)  # 成功插入数
        duplicate_count = details.get("nDuplicates", 0)  # 重复数据数
        write_errors = details.get("writeErrors", [])  # 具体错误列表（如字段类型错误）
        # 日志输出：先打印统计信息
        print(f"批量插入部分成功：成功插入{inserted_count}条，跳过重复{duplicate_count}条")
        # 关键：打印具体错误（若存在非重复错误，需重点关注）
        if write_errors:
            for i, err in enumerate(write_errors[:3]):  # 只打印前3个，避免日志过长
                err_msg = err.get("errmsg", "未知错误")
                err_doc = err.get("op", {})  # 导致错误的具体文档
                print(
                    f"  错误{i + 1}：msg={err_msg}，涉及文档：ts_code={err_doc.get('ts_code')}, trade_date={err_doc.get('trade_date')}")
        # 即使有重复，仍返回成功插入数
        return inserted_count
    except Exception as e:
        print(f"插入MongoDB失败（致命错误）：{str(e)}")
        return 0


def main_get_daily_share() -> str | None:
    print("=" * 60)
    print(f"启动A股日线数据更新")
    print("=" * 60)

    pro = tushare_ctl.init_tushare_client()
    basic_coll = mongoDb_ctl.init_mongo_collection(const.MONGO_BASIC_COLL)
    daily_coll = mongoDb_ctl.init_mongo_collection(const.MONGO_DAILY_COLL)
    conf_coll = mongoDb_ctl.init_mongo_collection(const.CONF_COLL)
    run, start_date, end_date = utils.get_start_end_date(pro, False)
    if end_date - start_date > timedelta(days=60):
        err = "数据更新超过100天，请运行全量更新"
        print(err)
        return err
    if not run:
        return None
    start_date_str = start_date.strftime(const.DATE_FORMAT)
    end_date_str = end_date.strftime(const.DATE_FORMAT)
    print(f"是否需要更新数据：{run}， 获取日线开始日期：{start_date_str}, 获取日线结束日期: {end_date_str}")

    # 获取上市股票列表
    try:
        stock_cursor = basic_coll.find(
            {"$and": [{"$or": [{"market": "主板"}, {"market": "创业板"}]}], "list_status": "L"},
            projection={"ts_code": 1, "_id": 0})
        # stock_cursor = basic_coll.find({"ts_code":"000001.SZ"},projection={"ts_code": 1, "_id": 0})
        stock_list = [doc["ts_code"] for doc in stock_cursor]
        if not stock_list:
            raise ValueError("未获取到上市股票列表（基础信息集合可能为空）")
        print(f"获取上市股票【主板、创业】排除st\科创的：{len(stock_list)}只")
    except Exception as e:
        err = "获取股票列表失败"+str(e)
        print(err)
        return err
    batch_size = 100
    all_stock_data: List[pd.DataFrame] = []
    stock_batches = [stock_list[i:i + batch_size] for i in range(0, len(stock_list), batch_size)]
    # 外层进度条：显示批次处理进度
    with tqdm(total=len(stock_batches), desc="拉取日线数据进度", unit="批") as batch_pbar:
        for batch in stock_batches:
            # 拉取数据
            try:
                ts_codes_new = ",".join(batch)
                df_stock = pro.daily(
                    ts_code=ts_codes_new,
                    start_date=start_date_str,
                    end_date=end_date_str,
                    fields="""ts_code,trade_date,open,high,low,close,change,pct_chg,vol,amount"""
                )
                if df_stock.empty:
                    print(f"股票无新数据:（{batch}）")
                    continue
                df_basic = pro.daily_basic(
                    ts_code=ts_codes_new,
                    start_date=start_date_str,
                    end_date=end_date_str,
                    fields="""ts_code,turnover_rate,turnover_rate_f,trade_date,total_share,float_share,free_share,total_mv,circ_mv"""
                )
                if df_basic.empty:
                    print(f"股票换手无新数据:（{batch}）")
                    continue
                merged_df1 = pd.merge(
                    df_stock,
                    df_basic,
                    on=['ts_code', 'trade_date'],  # 指定两个连接键
                    how='left'  # 内连接，只保留两个df中都存在的记录
                )
                all_stock_data.append(merged_df1)
            except Exception as e:
                err = "批量拉取失败" + str(e)
                print(err)
                return err
            batch_pbar.update(1)
            time.sleep(0.2)
    full_df = pd.concat(all_stock_data, ignore_index=True)
    # print(full_df.to_string())
    print(f"A股数据拉取完成，实际获取条数：{full_df.shape[0]} 条")
    count = 0
    all_adj_data: List[pd.DataFrame] = []
    while True:
        start_date_str_new = start_date.strftime(const.DATE_FORMAT)
        if start_date > end_date or count > 100:
            break
        df_adj = pro.adj_factor(start_date=start_date_str_new, end_date=start_date_str_new)
        all_adj_data.append(df_adj)
        start_date += timedelta(days=1)
        count += 1
    full_adj_df = pd.concat(all_adj_data, ignore_index=True)
    print(f"A股复权拉取完成，实际获取条数：{full_adj_df.shape[0]} 条")
    merged_df = pd.merge(
        full_df,
        full_adj_df,
        on=['ts_code', 'trade_date'],  # 指定两个连接键
        how='left'  # 内连接，只保留两个df中都存在的记录
    )
    # print(merged_df.to_string())

    # 批量插入MongoDB
    inserted = insert_batch_data(daily_coll, merged_df)
    conf_coll.update_one({"name": "daily_up_date"}, {"$set": {"value": end_date_str}})
    print("=" * 60)
    print(f"更新完成！总处理股票：{len(stock_list)}只，总插入数据：{inserted}条, 出问题数据：{len(err_tscode_list)}条")
    print("出问题数据：", err_tscode_list)
    print("=" * 60)
    return None


if __name__ == "__main__":
    main_get_daily_share()
