# 获取A股所有股票基本信息
import pandas as pd
from datetime import datetime
import logging
import time
import chinadata.ca_data as ts
from pymongo import MongoClient
from data_ctl import tushare_ctl
from util import mongoDb_ctl
from tqdm import tqdm  # 进度条库（核心新增）
from typing import List, Dict  # 类型提示（提升代码可读性）
from common import const

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


def fetch_a_share_full_data(pro: ts.pro_api) -> (pd.DataFrame, pd.DataFrame):
    logger.info("开始全量拉取A股基础信息")
    try:
        # 1. 先获取总数据量（用于进度条计算）
        # ts_code	    str	N	TS股票代码
        # name	        str	N	名称
        # market	    str	N	市场类别 （主板/创业板/科创板/CDR/北交所）
        # list_status	str	N	上市状态 L上市 D退市 P暂停上市，默认是L
        # exchange	    str	N	交易所 SSE上交所 SZSE深交所 BSE北交所
        # is_hs	        str	N	是否沪深港通标的，N否 H沪股通 S深股通
        total_count = pro.stock_basic(
            list_status='L',
            exchange='',
            fields='ts_code'  # 仅获取计数所需字段，减少数据传输
        ).shape[0]
        logger.info(f"预计拉取总条数：{total_count} 条")

        if total_count == 0:
            raise ValueError("拉取到0条数据，可能是接口权限或参数错误")

        # 2. 分页拉取配置（Tushare单次最大拉取5000条，此处设1000条/页）
        page_size = 1000
        total_pages = (total_count + page_size - 1) // page_size  # 向上取整计算总页数
        all_data: List[pd.DataFrame] = []

        # 3. 分页拉取+进度条展示
        with tqdm(total=total_count, desc="拉取A股数据", unit="条") as pbar:
            for page in range(1, total_pages + 1):
                # 计算分页参数（offset=跳过的条数）
                offset = (page - 1) * page_size
                # 拉取当前页数据
                page_df = pro.stock_basic(
                    list_status='L',
                    exchange='',
                    fields='''ts_code,name,market,exchange,area,
                              industry,list_date,list_status''',
                    limit=page_size,
                    offset=offset
                )
                # 累加数据
                all_data.append(page_df)
                # 更新进度条（增加当前页条数）
                pbar.update(page_df.shape[0])
                time.sleep(1)

        # 4. 合并所有分页数据
        full_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"A股数据拉取完成，实际获取条数：{full_df.shape[0]} 条")
        # 5. 数据预处理（带进度展示）
        full_df = preprocess_data(full_df)

        # 6. 获取所有非上市状态的股票，后面剔除这些数据 上市状态 L上市 D退市 P暂停上市，默认是L
        dll_df = pro.stock_basic(
            list_status='D,P',
            exchange='',
            fields='ts_code'  # 仅获取计数所需字段，减少数据传输
        )
        return full_df, dll_df
    except Exception as e:
        logger.error(f"A股数据拉取失败：{str(e)}")
        raise

def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """数据预处理：格式转换、新增字段，带进度条展示"""
    logger.info("开始数据预处理...")
    # 1. 新增数据获取时间（当前时间）
    df['up_time'] = datetime.now()
    # 2. 新增扩展
    for idx, row in df.iterrows():
        df.loc[idx, 'is_st'] = "Y" if "ST" in str(row['name']) else "N"# 判断是否ST股
    # 3. 清理无效数据（删除ts_code为空的记录）
    before_clean = len(df)
    df = df.dropna(subset=['ts_code']).reset_index(drop=True)
    after_clean = len(df)
    if before_clean > after_clean:
        logger.warning(f"清理无效数据：删除 {before_clean - after_clean} 条ts_code为空的记录")
    logger.info(f"数据预处理完成，最终有效数据条数：{len(df)} 条")
    return df


def full_update_mongo(full_df: pd.DataFrame, dll_df: pd.DataFrame):
    basic_mongo_coll = mongoDb_ctl.init_mongo_collection(const.MONGO_BASIC_COLL)
    daily_mongo_coll = mongoDb_ctl.init_mongo_collection(const.MONGO_DAILY_COLL)

    logger.info("开始MongoDB全量更新...")
    try:
        # 1. 准备待操作的ts_code列表（用于删除历史数据）
        ts_codes = full_df['ts_code'].tolist()
        total_records = len(ts_codes)

        if total_records == 0:
            raise ValueError("无有效数据可更新到MongoDB")

        # 2. 批量删除历史数据（按ts_code分批次删除，避免单次删除压力过大）
        logger.info("开始删除MongoDB历史数据...")
        batch_size = 500  # 每批次删除500条
        total_batches = (total_records + batch_size - 1) // batch_size
        # deleted_basic_total = 0
        # for i in range(total_batches):
        #     # 截取当前批次的ts_code
        #     batch_ts_codes = ts_codes[i * batch_size: (i + 1) * batch_size]
        #     # 执行删除
        delete_result = basic_mongo_coll.delete_many({})
        deleted_count = delete_result.deleted_count
        logger.info(f"basic历史数据删除完成，共删除 {deleted_count} 条")

        # 3. 删除日线里以停牌的股票
        ts_codes_dll = dll_df['ts_code'].tolist()
        total_dll_records = len(ts_codes_dll)
        deleted_daily_total = 0
        total_dll_batches = (total_dll_records + batch_size - 1) // batch_size
        for i in range(total_dll_batches):
            # 截取当前批次的ts_code
            batch_ts_codes = ts_codes_dll[i * batch_size: (i + 1) * batch_size]
            # 执行删除
            delete_result = daily_mongo_coll.delete_many({"ts_code": {"$in": batch_ts_codes}})
            deleted_count = delete_result.deleted_count
            deleted_daily_total += deleted_count
        logger.info(f"日线历史数据删除完成，共删除 {deleted_daily_total} 条")

        # 3. 转换DataFrame为MongoDB可接受的字典列表（处理NaT为None）
        logger.info("转换数据格式")
        data_list: List[Dict] = []
        for _, row in full_df.iterrows():
            data = row.to_dict()
            # 处理pandas的NaT（MongoDB不支持，转为None）
            for key, value in data.items():
                if isinstance(value, pd.Timestamp) and pd.isna(value):
                    data[key] = None
                if data[key] is None and key in ["industry","area"]:
                    data[key] = ""
                    logger.info(f"有股票缺数据：   {data} ")
            data_list.append(data)

        # 4. 批量插入新数据（分批次插入，避免单次插入数据量过大）
        logger.info("开始插入MongoDB新数据...")
        inserted_total = 0
        with tqdm(total=len(data_list), desc="插入新数据", unit="条") as pbar:
            for i in range(total_batches):
                # 截取当前批次的数据
                batch_data = data_list[i * batch_size: (i + 1) * batch_size]
                # 执行插入
                insert_result = basic_mongo_coll.insert_many(batch_data)
                inserted_count = len(insert_result.inserted_ids)
                inserted_total += inserted_count
                pbar.update(len(batch_data))
        # 5. 验证更新结果
        if inserted_total == total_records:
            logger.info(f"MongoDB全量更新成功！插入 {inserted_total} 条新数据（与拉取数据量一致）")
        else:
            logger.error(
                f"MongoDB更新警告：拉取 {total_records} 条，但仅插入 {inserted_total} 条（可能存在重复或约束错误）")

    except Exception as e:
        logger.error(f"MongoDB全量更新失败：{str(e)}")
        raise


def main_get_all_stock_basic():
    logger.info("=" * 60)
    logger.info("启动A股基础信息每日全量更新脚本")
    logger.info("=" * 60)

    # 初始化客户端
    pro = tushare_ctl.init_tushare_client()
    try:
        full_df, dll_df = fetch_a_share_full_data(pro)
        full_update_mongo(full_df, dll_df)
        logger.info("=" * 60)
        logger.info("A股基础信息每日全量更新脚本执行完成！")
        logger.info("=" * 60)
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"A股基础信息脚本执行失败：{str(e)}")
        logger.error("=" * 60)
        raise


if __name__ == "__main__":
    try:
        main_get_all_stock_basic()
    except Exception as e:
        logger.error(f"脚本执行失败，）")
