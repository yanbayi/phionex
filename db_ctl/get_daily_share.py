import time
from pymongo import MongoClient, errors
import logging
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
from typing import List, Dict, Optional
import chinadata.ca_data as ts
from common import utils, const, formula
from data_ctl import tushare_ctl
from util import mongoDb_ctl

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("log.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def fetch_batch_daily(pro: ts.pro_api, ts_codes: List[str], date_range: Dict[str, str]) -> pd.DataFrame:
    if not ts_codes:
        return pd.DataFrame()

    start_date = date_range["start_date"]
    end_date = date_range["end_date"]
    trading_days = date_range["trading_days"]
    # logger.info(f"拉取股票：{len(ts_codes)}只（示例：{ts_codes[:2]}...）")

    try:
        ts_codes_new = ",".join(ts_codes)
        df = pro.bak_daily(
            ts_code=ts_codes_new,
            start_date=start_date,
            end_date=end_date,
            fields="""ts_code,trade_date,open,high,low,close,pre_close,change,pct_change,vol,amount,vol_ratio,
                turn_over,swing,selling,buying,strength,activity,attack,avg_price"""
        )
        if df.empty:
            logger.error(f"股票无新数据:（{ts_codes}）")
            return pd.DataFrame()
        df["up_time"] = datetime.now()
        df_filtered = df.copy()
        if df_filtered.empty:
            # logger.info(f"无新增数据（{len(ts_codes)}只股票）")
            return pd.DataFrame()
        # logger.info(f"拉取完成：{len(df_result)}条数据")

        # 计算bbi
        # 步骤1：数据预处理（关键：按股票分组+交易日期升序排序，确保均线计算顺序正确）
        # 1.1 过滤非交易日数据（仅保留 trading_days 中的日期，避免节假日干扰）
        df_result = df_filtered[df_filtered["trade_date"].isin(trading_days)].copy()
        if df_result.empty:
            logger.warning(f"过滤非交易日后无数据（{len(ts_codes)}只股票）")
            return pd.DataFrame()

        # 1.2 按股票代码分组，每组内按交易日期升序排序（trade_date是YYYYMMDD字符串，直接排序=时间升序）
        df_result = df_result.sort_values(by=["ts_code", "trade_date"], ascending=[True, True])

        # 步骤2：定义BBI计算函数（按单只股票分组计算，避免跨股票干扰）
        def calculate_bbi_per_stock(group):
            group["MA0"] = group["close"].rolling(window=const.BBI_PERIODS[0], min_periods=const.BBI_PERIODS[0]).mean()
            group["MA1"] = group["close"].rolling(window=const.BBI_PERIODS[1], min_periods=const.BBI_PERIODS[1]).mean()
            group["MA2"] = group["close"].rolling(window=const.BBI_PERIODS[2], min_periods=const.BBI_PERIODS[2]).mean()
            group["MA3"] = group["close"].rolling(window=const.BBI_PERIODS[3], min_periods=const.BBI_PERIODS[3]).mean()
            valid_mask = ~group["MA0"].isna() & ~group["MA1"].isna() & ~group["MA2"].isna() & ~group["MA3"].isna()
            group["bbi"] = 0  # 先初始化
            group.loc[valid_mask, "bbi"] = (
                                                   group.loc[valid_mask, "MA0"] +
                                                   group.loc[valid_mask, "MA1"] +
                                                   group.loc[valid_mask, "MA2"] +
                                                   group.loc[valid_mask, "MA3"]
                                           ) / 4
            # 清理中间变量（仅保留BBI，删除MA5/MA10/MA20/MA60）
            group = group.drop(columns=["MA0", "MA1", "MA2", "MA3"])
            return group

        # 步骤3：按股票代码分组，应用BBI计算
        df_result = df_result.groupby("ts_code", group_keys=False).apply(calculate_bbi_per_stock)
        # 步骤4：处理可能的NaN值（若某行BBI计算失败，保留NaN，后续可按业务需求处理）
        df_result["bbi"] = df_result["bbi"].round(2)  # 可选：保留2位小数，便于阅读
        # logger.info(f"过滤非      交易日后无数据（{df_result.to_string()}只股票）")

        return df_result
    except Exception as e:
        logger.error(f" 批量拉取失败（{len(ts_codes)}只股票）：{str(e)}")
        return pd.DataFrame()


def insert_batch_data(daily_coll, df: pd.DataFrame, update: bool) -> int:
    if df.empty:
        logger.error(f"df empty：{len(df)}条数据")
        return 0

    try:
        data_list: List[Dict] = df.to_dict("records")
        for idx, data in enumerate(data_list):
            for k, v in data.items():
                if pd.isna(v) or v is None:
                    logger.error(f"数据出错：{data["ts_code"]}的 {k} 为空：{data[k]}")
                    data[k] = None
        # logger.info(f"数据预处理完成：共{len(data_list)}条，均转为MongoDB兼容格式")
    except Exception as e:
        logger.error(f"数据预处理失败（无法转为MongoDB兼容格式）：{str(e)}", exc_info=True)
        return 0
    try:
        inserted_count = 0
        if update:
            for data in data_list:
                result = daily_coll.update_one({"ts_code": data["ts_code"], "trade_date": data["trade_date"]}, {"$set": data}, upsert=True)
                if result.upserted_id:
                    inserted_count += 1
        else:
            result = daily_coll.insert_many(data_list, ordered=False)
            inserted_count = len(result.inserted_ids)
        # logger.info(f"插入成功：共{inserted_count}条数据（无重复或错误）")
        return inserted_count
    except errors.BulkWriteError as e:
        # 提取错误详情（关键：定位具体失败原因）
        details = e.details
        inserted_count = details.get("nInserted", 0)  # 成功插入数
        duplicate_count = details.get("nDuplicates", 0)  # 重复数据数
        write_errors = details.get("writeErrors", [])  # 具体错误列表（如字段类型错误）

        # 日志输出：先打印统计信息
        logger.error(f"批量插入部分成功：成功插入{inserted_count}条，跳过重复{duplicate_count}条")

        # 关键：打印具体错误（若存在非重复错误，需重点关注）
        if write_errors:
            logger.error(f"存在{len(write_errors)}个非重复错误，前3个错误详情：")
            for i, err in enumerate(write_errors[:3]):  # 只打印前3个，避免日志过长
                err_msg = err.get("errmsg", "未知错误")
                err_doc = err.get("op", {})  # 导致错误的具体文档
                logger.error(
                    f"  错误{i + 1}：msg={err_msg}，涉及文档：ts_code={err_doc.get('ts_code')}, trade_date={err_doc.get('trade_date')}")

        # 即使有重复，仍返回成功插入数
        return inserted_count

    # 处理其他致命错误（如MongoDB连接中断、权限不足）
    except Exception as e:
        logger.error(f"插入MongoDB失败（致命错误）：{str(e)}", exc_info=True)
        return 0


def main_get_daily_share(days, update):
    pro = tushare_ctl.init_tushare_client()
    basic_coll = mongoDb_ctl.init_mongo_collection(const.MONGO_BASIC_COLL)
    daily_coll = mongoDb_ctl.init_mongo_collection(const.MONGO_DAILY_COLL)
    if update: # 如果是更新则先算要拉多少天的数据
        cursor = daily_coll.find({"ts_code":"000002.SZ"},{"trade_date":1})
        existing_dates = []
        for doc in cursor:
            if "trade_date" in doc and doc["trade_date"]:
                existing_dates.append(doc["trade_date"])
        if not existing_dates:
            update = False
        else:
            latest_date = sorted(existing_dates, reverse=True)[0] #  db中的最新日期
            today = datetime.now().date()
            today_str = today.strftime(const.DATE_FORMAT)
            while True:
                test_cal = utils.get_tushare_trading_cal(pro, today_str, today_str)
                if test_cal:  # 若返回非空列表，说明是交易日
                    break
                # 不是交易日，向前调整1天
                time.sleep(1)
                today -= timedelta(days=1)
                today_str = today.strftime(const.DATE_FORMAT)
                # 安全阈值：最多向前调整10天（避免无限循环）
                if (datetime.now().date() - today).days > 10:
                    logger.error("未找到最近10天内的交易日，可能Tushare接口异常")
                    raise Exception("无法确定有效结束日期（交易日）")
            days = (today - datetime.strptime(latest_date, '%Y%m%d').date()).days
            if days < 0:
                return
    days += 24  # 满足bbi需求
    logger.info("=" * 60)
    logger.info(f"启动A股日线数据更新（前{days}个交易日")
    logger.info("=" * 60)
    # 获取上市股票列表
    try:
        stock_cursor = basic_coll.find(
            {"$and": [{"$or": [{"market": "主板"}, {"market": "创业板"}, {"market": "科创板"}]}, {"is_st": "N"}]},
            projection={"ts_code": 1, "_id": 0})
        stock_list = [doc["ts_code"] for doc in stock_cursor]
        if not stock_list:
            raise ValueError("未获取到上市股票列表（基础信息集合可能为空）")
        logger.info(f"获取上市股票【主板、创业、科创】排除st的：{len(stock_list)}只")
    except Exception as e:
        logger.error(f"获取股票列表失败：{str(e)}")
        return
    if not update: # 不是更新就先清空
        daily_coll.delete_many({})

    date_range_info = utils.calculate_date_range(pro, days)
    logger.info(
        f"获取日线开始日期(包含公式计算所以多向前取24天)：{date_range_info["start_date"]}, 获取日线结束日期: {date_range_info["end_date"]}, 日期列表: {date_range_info["trading_days"]}")

    total_inserted = 0
    batch_size = 5
    stock_batches = [stock_list[i:i + batch_size] for i in range(0, len(stock_list), batch_size)]

    # 外层进度条：显示批次处理进度
    with tqdm(total=len(stock_batches), desc="拉取日线数据进度", unit="批") as batch_pbar:
        for batch in stock_batches:
            # logger.info(f"\nmissing_dict: {missing_dict}")
            # 2.拉取数据
            df = fetch_batch_daily(pro, batch, date_range_info)
            # 3.批量插入MongoDB
            inserted = insert_batch_data(daily_coll, df, update)

            # 5.累计插入条数，更新进度条
            total_inserted += inserted
            batch_pbar.update(1)
            time.sleep(0.5)

    # 输出最终结果
    logger.info("=" * 60)
    logger.info(f"更新完成！总处理股票：{len(stock_list)}只，总插入数据：{total_inserted}条")
    logger.info("=" * 60)


# --------------------------
# 脚本入口（可指定时间范围）
# --------------------------
if __name__ == "__main__":
    main_get_daily_share(days=60, update=True) # 追新or全量更新
