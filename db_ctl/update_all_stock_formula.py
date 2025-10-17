import time

import pandas as pd
from tqdm import tqdm
from db_ctl.util import mongoDb_ctl
from common import const, utils, formula
from data_ctl import tushare_ctl

is_check = False


class StockFormula:
    def __init__(self):
        self.pro = tushare_ctl.init_tushare_client()
        self.stock_basic_coll = mongoDb_ctl.init_mongo_collection(const.MONGO_BASIC_COLL)
        self.stock_daily_coll = mongoDb_ctl.init_mongo_collection(const.MONGO_DAILY_COLL)

    def formula_main(self) -> (str | None, pd.DataFrame | None):
        # 获取所有股票
        try:
            stock_cursor = self.stock_basic_coll.find(
                {"$and": [{"$or": [{"market": "主板"}, {"market": "创业板"}]}], "list_status": "L"},
                projection={"ts_code": 1, "_id": 0})
            # stock_cursor = self.stock_basic_coll.find({"ts_code": {"$in": ["000001.SZ", "000559.SZ", "002446.SZ"]}},
            #                                           projection={"ts_code": 1, "_id": 0})
            stock_list = [doc["ts_code"] for doc in stock_cursor]
            if not stock_list:
                raise ValueError("未获取到上市股票列表（基础信息集合可能为空）")
            print(f"获取上市股票【主板、创业】排除st\科创的：{len(stock_list)}只")
        except Exception as e:
            err = "获取股票列表失败：" + str(e)
            print(err)
            return err, None
        # 循环股票
        batch_size = 20
        stock_batches = [stock_list[i:i + batch_size] for i in range(0, len(stock_list), batch_size)]
        # 外层进度条：显示批次处理进度
        with tqdm(total=len(stock_batches), desc="更新日线指标进度", unit="批") as batch_pbar:
            for batch in stock_batches:
                # 获取一只股票的数据
                try:
                    # time1 = time.time()
                    stock_cursor = self.stock_daily_coll.find({"ts_code": {"$in": batch}}, {"_id": 0})
                    # time2 = time.time()
                    # print(f"alltime2: {time2 - time1:.6f} 秒")
                    df = pd.DataFrame(list(stock_cursor))
                    # time3 = time.time()
                    # print(f"alltime3: {time3 - time2:.6f} 秒")
                    df["date"] = pd.to_datetime(df["trade_date"], format=const.DATE_FORMAT)
                    df_sorted = df.sort_values(by=['ts_code', 'date'])
                    new_adj_factors = df_sorted.groupby('ts_code')['adj_factor'].last().reset_index()
                    new_adj_factors.columns = ['ts_code', 'new_adj_factor']
                    # 将new_adj_factor合并回原始DataFrame
                    df_with_new = pd.merge(df_sorted, new_adj_factors, on='ts_code', how='left')
                    # 计算前复权价格
                    df_with_new['open_q'] = df_with_new['open'] * (
                            df_with_new['adj_factor'] / df_with_new['new_adj_factor'])
                    df_with_new['high_q'] = df_with_new['high'] * (
                            df_with_new['adj_factor'] / df_with_new['new_adj_factor'])
                    df_with_new['low_q'] = df_with_new['low'] * (
                            df_with_new['adj_factor'] / df_with_new['new_adj_factor'])
                    df_with_new['close_q'] = df_with_new['close'] * (
                            df_with_new['adj_factor'] / df_with_new['new_adj_factor'])
                    result_df = df_with_new[['ts_code', 'trade_date', 'date', 'open_q', 'high_q', 'low_q', 'close_q']]
                except Exception as e:
                    err = "批量拉取失败：" + str(e)
                    print(err)
                    return err, None

                result_df = formula.formula_main(result_df)
                batch_pbar.update(1)
        return None, result_df


if __name__ == "__main__":
    stockFormula = StockFormula()
    stockFormula.formula_main()
