# 通达信板块成分、信息获取
import pandas as pd
from datetime import datetime, timedelta
from data_ctl import tushare_ctl
from util import mongoDb_ctl
from tqdm import tqdm  # 进度条库（核心新增）
from typing import List, Dict  # 类型提示（提升代码可读性）
from common import const, utils


def fetch_tdx_index_data(pro, ts_codes: List[str] = None, trade_date: str = None,
                         idx_types: List[str] = None) -> pd.DataFrame:
    # 处理默认交易日期
    if not trade_date:
        trade_date = utils.get_end_date(pro, datetime.now())
        trade_date = trade_date.strftime(const.DATE_FORMAT)
        print(f"未指定交易日期，使用默认日期: {trade_date}")
    all_data = []
    if idx_types:
        for idx_type in tqdm(idx_types, desc="拉取不同类型板块", unit="类型"):
            try:
                # print(f"\n交易日期日期: {trade_date}, 类型板块:{idx_type}")
                df = pro.tdx_index(trade_date=trade_date, idx_type=idx_type)
                if not df.empty:
                    all_data.append(df)
                    # print(f"成功获取{idx_type}类型板块{len(df)}条数据")
            except Exception as e:
                print(f"获取{idx_type}类型板块失败: {str(e)}")
    else:
        print("未指定板块代码和板块类型，无法拉取数据")
        return pd.DataFrame()

    # 合并数据
    if all_data:
        result_df = pd.concat(all_data, ignore_index=True)
        result_df["up_time"] = datetime.now()
        return result_df
    return pd.DataFrame()


def fetch_tdx_member_data(pro, ts_codes: List[str] = None, trade_date: str = None) -> pd.DataFrame:
    if not ts_codes:
        print("未提供板块代码列表，无法拉取成分股数据")
        return pd.DataFrame()

    all_data = []
    # 处理默认交易日期（前一交易日）
    if not trade_date:
        trade_date = utils.get_end_date(pro, datetime.now())
        trade_date = trade_date.strftime(const.DATE_FORMAT)
        print(f"未指定交易日期，使用默认日期: {trade_date}")
    # 添加tqdm进度条
    for i in tqdm(range(0, len(ts_codes)), desc="拉取成分股数据", total=len(ts_codes), unit="批"):
        try:
            df = pro.tdx_member(
                ts_code=ts_codes[i],
                trade_date=trade_date
            )
            if not df.empty:
                all_data.append(df)
        except Exception as e:
            print(f"获取板块{ts_codes[i]}的成分股失败: {str(e)}")

    # 合并数据并补充字段
    if all_data:
        result_df = pd.concat(all_data, ignore_index=True)
        result_df["up_time"] = datetime.now()  # 数据获取时间
        print(f"共获取到{len(result_df)}条成分股信息")
        return result_df
    return pd.DataFrame()


def insert_tdx_index_to_mongodb(data: pd.DataFrame, tdx_index_coll) -> int:
    if data.empty:
        print("没有数据需要插入MongoDB")
        return 0
    # 转换为字典列表并处理空值
    data_dict = data.to_dict("records")
    for doc in data_dict:
        for key, value in doc.items():
            if pd.isna(value) or value is None:
                doc[key] = None
    # 插入数据并显示进度
    inserted_count = 0
    for doc in data_dict:
        try:
            # 使用upsert实现存在则更新，不存在则插入
            result = tdx_index_coll.update_one(
                {"ts_code": doc["ts_code"]},
                {"$set": doc},
                upsert=True
            )
            if result.upserted_id:
                inserted_count += 1
        except Exception as e:
            # 捕获并记录验证失败的错误
            print(f"插入记录失败 (ts_code: {doc.get('ts_code')}, trade_date: {doc.get('trade_date')}): {str(e)}")

    print(f"插入完成：新增{inserted_count}条，更新{len(data_dict) - inserted_count}条")
    return inserted_count


def insert_tdx_member_to_mongodb(data: pd.DataFrame, tdx_member_coll) -> int:
    if data.empty:
        print("没有成分股数据需要插入MongoDB")
        return 0
    # 转换为字典列表并处理空值
    data_dict = data.to_dict("records")
    for doc in data_dict:
        for key, value in doc.items():
            if pd.isna(value) or value is None:
                doc[key] = None
    # 插入数据并显示进度
    inserted_count = 0
    for doc in tqdm(data_dict, desc=f"插入成分股到{tdx_member_coll.name}", unit="条"):
        try:
            # 使用upsert实现存在则更新，不存在则插入
            result = tdx_member_coll.update_one(
                {"ts_code": doc["ts_code"], "con_code": doc["con_code"]},
                {"$set": doc},
                upsert=True
            )
            if result.upserted_id:
                inserted_count += 1
        except Exception as e:
            print(
                f"插入成分股记录失败 (ts_code: {doc.get('ts_code')}, con_code: {doc.get('con_code')}): {str(e)}")

    print(f"成分股插入完成：新增{inserted_count}条，更新{len(data_dict) - inserted_count}条")
    return inserted_count


def main_get_tdx_basic():
    print("=" * 60)
    print("启动tdx模块信息、成分每日全量更新脚本")
    print("=" * 60)

    # 初始化客户端
    pro = tushare_ctl.init_tushare_client()
    tdx_index_coll = mongoDb_ctl.init_mongo_collection(const.TDX_INDEX)
    tdx_member_coll = mongoDb_ctl.init_mongo_collection(const.TDX_MEMBER)

    try:
        # 1. 配置参数
        # target_date = ""  # 可指定日期，如"20250616"，None则使用默认日期
        target_types = ["行业板块", "概念板块"]  # 要拉取的板块类型, "风格板块", "地区板块"
        # 如需指定特定板块，可添加板块代码列表
        # specific_boards = ["880324.TDX", "880325.TDX"]  # 示例板块代码

        # 2. 拉取板块基本信息
        print("===== 开始拉取板块基本信息 =====")
        index_df = fetch_tdx_index_data(
            pro=pro,
            # ts_codes=specific_boards,  # 如需指定特定板块，取消注释
            # trade_date=target_date,
            idx_types=target_types
        )
        # 3. 存储板块信息到MongoDB
        if not index_df.empty:
            insert_tdx_index_to_mongodb(index_df, tdx_index_coll)
            # 提取板块代码用于拉取成分股
            board_codes = index_df["ts_code"].unique().tolist()
            print(f"共获取到{len(board_codes)}个概念板块，准备拉取成分股")
        else:
            print("未获取到任何板块信息，终止流程")

        # 4. 提取板块代码和日期，用于拉取成分股
        board_codes = index_df["ts_code"].unique().tolist()
        trade_date = index_df["trade_date"].iloc[0]
        print(f"共获取到{len(board_codes)}个板块，准备拉取{trade_date}的成分股数据")

        # 5. 拉取并存储成分股数据
        print("===== 处理成分股数据 =====")
        member_df = fetch_tdx_member_data(pro=pro, ts_codes=board_codes)

        if not member_df.empty:
            insert_tdx_member_to_mongodb(member_df, tdx_member_coll)
        else:
            print("未获取到有效成分股数据")

        print("=" * 60)
        print("通达信板块数据处理完成 ！")
        print("=" * 60)
    except Exception as e:
        print("=" * 60)
        print(f"脚本执行失败：{str(e)}")
        print("=" * 60)
        raise


if __name__ == "__main__":
    try:
        main_get_tdx_basic()
    except Exception as e:
        print(f"脚本执行失败，）")
