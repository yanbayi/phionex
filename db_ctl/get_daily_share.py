import time
from pymongo import MongoClient, errors
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
from typing import List, Dict, Optional
from common import utils, const, formula
from data_ctl import tushare_ctl
from util import mongoDb_ctl

err_tscode_list = {}
def insert_batch_data(daily_coll, df: pd.DataFrame, update: bool) -> int:
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
        if update:
            for data in data_list:
                result = daily_coll.update_one({"ts_code": data["ts_code"], "trade_date": data["trade_date"]},
                                               {"$set": data}, upsert=True)
                if result.upserted_id:
                    inserted_count += 1
        else:
            result = daily_coll.insert_many(data_list, ordered=False)
            inserted_count = len(result.inserted_ids)
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


def main_get_daily_share(days, update):
    print("=" * 60)
    print(f"启动A股日线数据更新（前{days}个交易日")
    print("=" * 60)

    pro = tushare_ctl.init_tushare_client()
    basic_coll = mongoDb_ctl.init_mongo_collection(const.MONGO_BASIC_COLL)
    daily_coll = mongoDb_ctl.init_mongo_collection(const.MONGO_DAILY_COLL)
    conf_coll = mongoDb_ctl.init_mongo_collection(const.CONF_COLL)
    run, start_date, end_date, update = utils.get_start_end_date(pro, days, update, False)
    if not run:
        return
    if not update:
        daily_coll.delete_many({})  # 全量更新先清空数据
    start_date_str = start_date.strftime(const.DATE_FORMAT)
    end_date_str = end_date.strftime(const.DATE_FORMAT)
    print(f"是否需要更新数据：{run}， 是否全量更新数据：{not update}， 获取日线开始日期：{start_date_str}, 获取日线结束日期: {end_date_str}")


    # 获取上市股票列表
    try:
        stock_cursor = basic_coll.find(
            {"$and": [{"$or": [{"market": "主板"}, {"market": "创业板"}]}, {"is_st": "N"}]},
            projection={"ts_code": 1, "_id": 0})      # {"market": "科创板"}
        stock_list = [doc["ts_code"] for doc in stock_cursor]
        if not stock_list:
            raise ValueError("未获取到上市股票列表（基础信息集合可能为空）")
        print(f"获取上市股票【主板、创业】排除st\科创的：{len(stock_list)}只")
    except Exception as e:
        print(f"获取股票列表失败：{str(e)}")
        return

    total_inserted = 0
    batch_size = 1
    stock_batches = [stock_list[i:i + batch_size] for i in range(0, len(stock_list), batch_size)]
    # 外层进度条：显示批次处理进度
    with tqdm(total=len(stock_batches), desc="拉取日线数据进度", unit="批") as batch_pbar:
        for batch in stock_batches:
            # 拉取数据
            try:
                # ts_codes_new = ",".join(batch)
                df = pro.stk_factor_pro(
                    ts_code=batch[0],
                    start_date=start_date_str,
                    end_date=end_date_str,
                    fields="""ts_code,trade_date,open_qfq,high_qfq,low_qfq,close_qfq,pre_close,change,pct_chg,vol,
                        amount,turnover_rate,turnover_rate_f,volume_ratio,total_share,float_share,free_share,
                        total_mv,circ_mv,
                        asi_qfq,asit_qfq,
                        atr_qfq,
                        bbi_qfq,
                        bias1_qfq,bias2_qfq,bias3_qfq,
                        boll_lower_qfq,boll_mid_qfq,boll_upper_qfq,
                        brar_ar_qfq,brar_br_qfq,
                        cr_qfq,
                        kdj_qfq,kdj_d_qfq,kdj_k_qfq,
                        ma_qfq_5,ma_qfq_10,ma_qfq_20,ma_qfq_60,ma_qfq_90,
                        macd_qfq,macd_dea_qfq,macd_dif_qfq,
                        obv_qfq,
                        rsi_qfq_6,rsi_qfq_12,rsi_qfq_24
                        """
                )
                # print(df.to_string())
                if df.empty:
                    print(f"股票无新数据:（{batch}）")
                    continue
                df["up_time"] = datetime.now()
                df = df.rename(columns={
                    'open_qfq': 'open',
                    'high_qfq': 'high',
                    'low_qfq': 'low',
                    'close_qfq': 'close',
                    'asi_qfq': 'asi',
                    'asit_qfq': 'asit',
                    'atr_qfq': 'atr',
                    'bbi_qfq': 'bbi',
                    'bias1_qfq': 'bias1',
                    'bias2_qfq': 'bias2',
                    'bias3_qfq': 'bias3',
                    'boll_lower_qfq': 'boll_lower',
                    'boll_mid_qfq': 'boll_mid',
                    'boll_upper_qfq': 'boll_upper',
                    'brar_ar_qfq': 'brar_ar',
                    'brar_br_qfq': 'brar_br',
                    'cr_qfq': 'cr',
                    'kdj_qfq': 'kdj_j',
                    'kdj_d_qfq': 'kdj_d',
                    'kdj_k_qfq': 'kdj_k',
                    'ma_qfq_5': 'ma5',
                    'ma_qfq_10': 'ma10',
                    'ma_qfq_20': 'ma20',
                    'ma_qfq_60': 'ma60',
                    'ma_qfq_90': 'ma90',
                    'macd_qfq': 'macd',
                    'macd_dea_qfq': 'macd_dea',
                    'macd_dif_qfq': 'macd_dif',
                    'obv_qfq': 'obv',
                    'rsi_qfq_6': 'rsi6',
                    'rsi_qfq_12': 'rsi12',
                    'rsi_qfq_24': 'rsi24'})
            except Exception as e:
                print(f" 批量拉取失败（{len(batch)}只股票）：{str(e)}")
            # 批量插入MongoDB
            inserted = insert_batch_data(daily_coll, df, update)
            # 5.累计插入条数，更新进度条
            total_inserted += inserted
            batch_pbar.update(1)
            time.sleep(0.2)

    conf_coll.update_one({"name": "daily_up_date"}, {"$set":{"value":end_date_str}})
    print("=" * 60)
    print(f"更新完成！总处理股票：{len(stock_list)}只，总插入数据：{total_inserted}条, 出问题数据：{len(err_tscode_list)}条")
    print("出问题数据：", err_tscode_list)
    print("=" * 60)


if __name__ == "__main__":
    main_get_daily_share(days=60, update=True)  # 追新or全量更新
