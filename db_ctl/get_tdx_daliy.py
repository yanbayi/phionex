# 通达信板块日线获取
import time

import pandas as pd
from datetime import datetime, timedelta
import logging
from data_ctl import tushare_ctl
from util import mongoDb_ctl
from tqdm import tqdm  # 进度条库（核心新增）
from typing import List, Dict  # 类型提示（提升代码可读性）
from common import const, utils

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


def validate_quant_daily_data(raw_df: pd.DataFrame, ts_code: str) -> pd.DataFrame:
    if raw_df.empty:
        logger.error(f"板块{ts_code}原始数据为空")
        return pd.DataFrame()
    cleaned_df = raw_df.copy()

    cleaned_df["up_time"] = datetime.now()
    mongo_fields = ["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_change",
                    "vol", "amount", "rise", "vol_ratio", "turnover_rate","swing", "up_num", "limit_up_num", "lu_days",
                    "3day", "5day", "10day", "20day", "60day", "up_time","bm_buy_net","bm_buy_ratio","bm_net","bm_ratio"]
    cleaned_df = cleaned_df[mongo_fields].copy()
    # logger.info(f"板块{ts_code}数据清洗完成，有效记录数：{len(cleaned_df)}")
    return cleaned_df


def fetch_tdx_daily_single(pro, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    try:
        df = pro.tdx_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if not df.empty:
            # logger.info(f"板块{ts_code}拉取成功，记录数：{len(df)}")
            return df
        else:
            logger.error(f"板块{ts_code}在{start_date}~{end_date}期间无数据")
            return pd.DataFrame()
    except Exception as e:
        logger.error(
            f"板块{ts_code}拉取失败（已达最大重试次数）：{str(e)}",
            exc_info=True
        )
        return pd.DataFrame()


def insert_tdx_daily_to_mongo(data: pd.DataFrame, tdx_daily_col, update) -> int:
    if data.empty:
        logger.warning("无有效数据可插入MongoDB")
        return 0

    # 转换为字典列表（MongoDB存储格式），处理NaN值（转为None）
    data_dict = data.to_dict("records")
    for doc in data_dict:
        # print("1", doc)
        for key, value in doc.items():
            if pd.isna(value) or value is None:
                doc[key] = None  # MongoDB不支持NaN，统一转为None
                if key in [ "turnover_rate", "up_num","limit_up_num", "lu_days",
                 "3day", "5day", "10day", "20day", "60day","bm_buy_net","bm_buy_ratio","bm_net","bm_ratio"]:
                    doc[key] = 0
                else:
                    logger.error("缺失字段: ", key)
    # print("3", data_dict)
    inserted_count = 0
    # 批量插入（带进度条）
    try:
        if not update:
            result = tdx_daily_col.insert_many(data_dict, ordered=False)
            inserted_count = len(result.inserted_ids)
        else:
            for doc in data_dict:
                # print(doc)
                update_result = tdx_daily_col.update_one(
                    filter={"ts_code": doc["ts_code"], "trade_date": doc["trade_date"]},
                    update={
                        "$set": doc,
                    },
                    upsert=True  # 不存在则插入，存在则更新
                )
                if update_result.upserted_id:
                    inserted_count += 1
    except Exception as e:
        logger.error(
            f"插入记录失败: {str(e)}",
            exc_info=True
        )

    # logger.info(f"MongoDB插入完成：新增{inserted_count}条，更新{len(data_dict) - inserted_count}条")
    return inserted_count

def main_get_tdx_daily(days, update):
    pro = tushare_ctl.init_tushare_client()
    tdx_index_coll = mongoDb_ctl.init_mongo_collection(const.TDX_INDEX)
    tdx_daily_coll = mongoDb_ctl.init_mongo_collection(const.TDX_DAILY)
    if update:  # 如果是更新则先算要拉多少天的数据
        tdx_one = tdx_index_coll.find_one({},{"ts_code": 1})
        cursor = tdx_daily_coll.find({"ts_code": tdx_one["ts_code"]}, {"trade_date": 1})
        existing_dates = []
        for doc in cursor:
            if "trade_date" in doc and doc["trade_date"]:
                existing_dates.append(doc["trade_date"])
        if not existing_dates:
            update = False
        else:
            latest_date = sorted(existing_dates, reverse=True)[0]  # db中的最新日期
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

    logger.info("=" * 60)
    logger.info(f"启动通达信板块{days}日日线数据增量拉取流程")
    logger.info("=" * 60)

    try:
        valid_boards = tdx_index_coll.find({}, projection={"ts_code": 1, "_id": 0})
        board_codes = list({doc["ts_code"] for doc in valid_boards if "ts_code" in doc})
        if not board_codes:
            logger.error("从tdx_index集合未读取到任何有效板块代码，终止流程")
            return
        logger.info(f"成功读取{len(board_codes)}个有效板块代码（示例：{board_codes[:3]}）")
    except Exception as e:
        logger.error(f"读取tdx_index集合失败：{str(e)}", exc_info=True)
        return

    if not update: # 不是更新就先清空
        tdx_daily_coll.delete_many({})

    date_range_info = utils.calculate_date_range(pro, days)
    logger.info(
        f"获取日线开始日期(包含公式计算所以多向前取n天)：{date_range_info["start_date"]}, 获取日线结束日期: {date_range_info["end_date"]}, 日期列表: {date_range_info["trading_days"]}")
    start_date_str = date_range_info["start_date"]
    end_date_str = date_range_info["end_date"]
    current_trading_days = date_range_info["trading_days"]


    global_inserted_count = 0
    for idx, ts_code in enumerate(tqdm(board_codes, desc="整体处理进度", unit="板块"), 1):
        raw_daily_df = fetch_tdx_daily_single(pro=pro, ts_code=ts_code, start_date=start_date_str,
                                              end_date=end_date_str)
        if raw_daily_df.empty:
            logger.error(f"板块{ts_code}未拉取到任何原始数据，跳过后续处理")
            continue
        cleaned_df = validate_quant_daily_data(raw_df=raw_daily_df,ts_code=ts_code)

        if cleaned_df.empty:
            logger.error(f"板块{ts_code}清洗后无有效数据，跳过插入")
            continue

        batch_inserted = insert_tdx_daily_to_mongo(
            data=cleaned_df,
            tdx_daily_col=tdx_daily_coll,
            update=update
        )
        global_inserted_count += batch_inserted
        time.sleep(0.5)
    logger.info("=" * 50)
    logger.info(f"通达信板块{days}日日线数据拉取流程完成")
    logger.info(f"总统计：处理{len(board_codes)}个板块，新增{global_inserted_count}条日线数据")
    logger.info(f"覆盖交易日：{start_date_str} ~ {end_date_str}（共{len(current_trading_days)}个交易日）")
    logger.info("=" * 50)


if __name__ == "__main__":
    try:
        main_get_tdx_daily(days=60, update=True) # 追新or全量更新
    except Exception as e:
        logger.warning(f"脚本执行失败，）")
