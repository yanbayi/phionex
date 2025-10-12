# 通达信板块成分、信息获取
import pandas as pd
from datetime import datetime, timedelta
from data_ctl import tushare_ctl
from util import mongoDb_ctl
from tqdm import tqdm
from common import const, utils


def main_get_tdx_basic():
    print("=" * 60)
    print("启动tdx模块信息、成分每日全量更新脚本")
    print("=" * 60)
    pro = tushare_ctl.init_tushare_client()
    tdx_index_coll = mongoDb_ctl.init_mongo_collection(const.TDX_INDEX)
    tdx_member_coll = mongoDb_ctl.init_mongo_collection(const.TDX_MEMBER)

    now_date = datetime.now() - timedelta(days=1)
    trade_date = utils.get_end_date(pro, now_date)
    trade_date = trade_date.strftime(const.DATE_FORMAT)

    try:
        target_types = ["行业板块", "概念板块"]
        print("===== 开始拉取板块基本信息 =====")
        # 处理默认交易日期
        all_data = []
        for idx_type in tqdm(target_types, desc="拉取不同类型板块", unit="类型"):
            df = pro.tdx_index(trade_date=trade_date, idx_type=idx_type)
            if not df.empty:
                all_data.append(df)
        result_df = pd.concat(all_data, ignore_index=True)
        data_dict = result_df.to_dict("records")
        for doc in data_dict:
            for key, value in doc.items():
                if pd.isna(value) or value is None:
                    doc[key] = None
        tdx_index_coll.delete_many({})
        tdx_index_coll.insert_many(data_dict)

        # 提取板块代码用于拉取成分股
        board_codes = result_df["ts_code"].unique().tolist()
        print(f"共获取到{len(board_codes)}个概念板块，准备拉取成分股")
        print("===== 处理成分股数据 =====")
        all_data_member = []
        for i in tqdm(range(0, len(board_codes)), desc="拉取成分股数据", total=len(board_codes), unit="批"):
            df = pro.tdx_member(
                ts_code=board_codes[i],
                trade_date=trade_date
            )
            if not df.empty:
                all_data_member.append(df)
        result_df_member = pd.concat(all_data_member, ignore_index=True)
        if not result_df_member.empty:
            tdx_member_coll.delete_many({})
            data_dict = result_df_member.to_dict("records")
            for doc in data_dict:
                for key, value in doc.items():
                    if pd.isna(value) or value is None:
                        doc[key] = None
            tdx_member_coll.insert_many(data_dict)
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
    main_get_tdx_basic()
