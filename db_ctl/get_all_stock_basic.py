# 获取A股所有股票基本信息
import pandas as pd
from datetime import datetime
import time
import chinadata.ca_data as ts
from pymongo import MongoClient
from data_ctl import tushare_ctl
from db_ctl.util import mongoDb_ctl
from tqdm import tqdm  # 进度条库（核心新增）
from typing import List, Dict  # 类型提示（提升代码可读性）
from common import const


def fetch_a_share_full_data(pro: ts.pro_api) -> (pd.DataFrame, pd.DataFrame):
    print("开始全量拉取A股基础信息")
    try:
        # 1. 先获取总数据量（用于进度条计算）
        # ts_code	    str	N	TS股票代码
        # name	        str	N	名称
        # market	    str	N	市场类别 （主板/创业板/科创板/CDR/北交所）
        # list_status	str	N	上市状态 L上市 D退市 P暂停上市，默认是L
        # exchange	    str	N	交易所 SSE上交所 SZSE深交所 BSE北交所
        # is_hs	        str	N	是否沪深港通标的，N否 H沪股通 S深股通
        total_count = pro.stock_basic(fields='ts_code').shape[0]
        print(f"预计拉取总条数：{total_count} 条")

        if total_count == 0:
            raise ValueError("拉取到0条数据，可能是接口权限或参数错误")

        # 2. 分页拉取配置（Tushare单次最大拉取5000条，此处设1000条/页）
        page_size = 2000
        total_pages = (total_count + page_size - 1) // page_size  # 向上取整计算总页数
        all_data: List[pd.DataFrame] = []

        # 3. 分页拉取+进度条展示
        with tqdm(total=total_count, desc="拉取A股数据", unit="条") as pbar:
            for page in range(1, total_pages + 1):
                # 计算分页参数（offset=跳过的条数）
                offset = (page - 1) * page_size
                # 拉取当前页数据
                page_df = pro.stock_basic(
                    fields='''ts_code,name,market,exchange,list_date,list_status''',
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
        print(f"A股数据拉取完成，实际获取条数：{full_df.shape[0]} 条")
        # 5. 数据预处理（带进度展示）
        full_df = preprocess_data(full_df)
        return full_df
    except Exception as e:
        print(f"A股数据拉取失败：{str(e)}")
        raise

def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    print("开始数据预处理...")
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
        print(f"清理无效数据：删除 {before_clean - after_clean} 条ts_code为空的记录")
    print(f"数据预处理完成，最终有效数据条数：{len(df)} 条")
    return df


def full_update_mongo(full_df: pd.DataFrame):
    basic_mongo_coll = mongoDb_ctl.init_mongo_collection(const.MONGO_BASIC_COLL)
    ts_codes = full_df['ts_code'].tolist()
    total_records = len(ts_codes)
    try:
        delete_result = basic_mongo_coll.delete_many({})
        print(f"basic历史数据删除完成，共删除 {delete_result.deleted_count} 条")
        data_list: List[Dict] = []
        for _, row in full_df.iterrows():
            data = row.to_dict()
            for key, value in data.items():
                if isinstance(value, pd.Timestamp) and pd.isna(value):
                    data[key] = None
                    print(f"有股票缺数据：{data} ")
            data_list.append(data)
        insert_result = basic_mongo_coll.insert_many(data_list)
        # 5. 验证更新结果
        if len(insert_result.inserted_ids) == total_records:
            print(f"MongoDB全量更新成功！插入 {total_records} 条新数据（与拉取数据量一致）")
        else:
            print(
                f"MongoDB更新警告：拉取 {total_records} 条，但仅插入 {len(insert_result.inserted_ids)} 条（可能存在重复或约束错误）")
    except Exception as e:
        print(f"MongoDB全量更新失败：{str(e)}")
        raise


def main_get_all_stock_basic() -> str | None:
    print("=" * 60)
    print("启动A股基础信息每日全量更新脚本")
    print("=" * 60)

    # 初始化客户端
    pro = tushare_ctl.init_tushare_client()
    try:
        full_df = fetch_a_share_full_data(pro)
        full_update_mongo(full_df)
        print("=" * 60)
        print("A股基础信息每日全量更新脚本执行完成！")
        print("=" * 60)
        return None
    except Exception as e:
        err = "A股基础信息脚本执行失败"+str(e)
        print("=" * 60)
        print(err)
        print("=" * 60)
        return err


if __name__ == "__main__":
    try:
        main_get_all_stock_basic()
    except Exception as e:
        print(f"脚本执行失败，）")
