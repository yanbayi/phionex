import re
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from typing import List, Dict, Callable, Any
from db_ctl.util import mongoDb_ctl
from common import const, utils
from data_ctl import tushare_ctl


class TdxFilter:
    def __init__(self):
        self.condition_handlers = {
            "1": self.filter_name,
            "2": self.filter_limit_up1,
            "3": self.filter_limit_up2,
            "4": self.filter_vol1,
            "5": self.filter_amount1,
            "6": self.filter_vol_ratio1,
        }
        self.pro = tushare_ctl.init_tushare_client()
        self.tdx_index_coll = mongoDb_ctl.init_mongo_collection(const.TDX_INDEX)
        self.tdx_daily_coll = mongoDb_ctl.init_mongo_collection(const.TDX_DAILY)

    def filter_name(self, df: pd.DataFrame, params: Dict) -> (List, pd.DataFrame):
        #     # 板块、概念名称带“半导体”的所有板块概念,实例:["半导体"]   ["半导体", "机器人", "矿"]
        #     {"type": "1", "name": "名称包含", "enable": False, "params": {"name": ["半导体", "机器人"]}},
        stock_list = []
        name_condition = params.get('name', [])
        if len(name_condition) > 0:
            query_conditions = []
            for keyword in name_condition:
                query_conditions.append({"name": {"$regex": keyword}})
            cursor = self.tdx_index_coll.find({"$or": query_conditions}, {"ts_code": 1, "_id": 0, "name": 1})
            stock_list = [doc["ts_code"] for doc in cursor]
            df = pd.DataFrame(list(cursor))
        return stock_list, df

    def filter_limit_up1(self, df: pd.DataFrame, params: Dict) -> (List, pd.DataFrame):
        # # 最近day1天中有day2天在涨，且其中有day3天涨幅超过n%的top m
        # {"type": "2", "name": "涨幅条件1", "enable": True,
        #  "params": {"day1": 5, "day2": 3, "day3": 1, "n": 1, "m": 50}},
        day1 = params.get('day1', 1)
        day2 = params.get('day2', 1)
        day3 = params.get('day3', 1)
        n = params.get('n', 100)
        m = params.get('m', 100)
        # 筛选最近day1天的数据
        latest_date = df['date'].max()
        latest_date_str = latest_date.strftime(const.DATE_FORMAT)
        start_date_str, _ = utils.get_start_date(self.pro, day1, latest_date_str)
        start_date = datetime.strptime(start_date_str, const.DATE_FORMAT)
        recent_df = df[df['date'] >= start_date].copy()
        if recent_df.empty:
            print(f"在最近{day1}天内没有找到股票数据")
            return [], pd.DataFrame()

        # 按股票代码分组,计算符合条件的天数
        def calculate_conditions(group):
            group_sorted = group.sort_values('date')
            # 统计上涨的天数（涨幅>0）
            up_days = (group_sorted['pct_change'] > 0).sum()
            # 统计涨幅超过n%的天数
            over_n_days = (group_sorted['pct_change'] > n).sum()
            # 计算区间总涨幅（从第一天到最后一天的累计涨幅）
            # 公式：(最后一天收盘价 / 第一天收盘价 - 1) * 100
            first_close = group_sorted.iloc[0]['close']
            last_close = group_sorted.iloc[-1]['close']
            total_growth = (last_close / first_close - 1) * 100
            return pd.Series({
                'up_days': up_days,
                'over_n_days': over_n_days,
                'valid_days': len(group_sorted),
                'total_growth': total_growth
            })

        # 应用分组计算
        stock_stats = recent_df.groupby('ts_code').apply(calculate_conditions).reset_index()
        # print(stock_stats.to_string())
        # 筛选条件:
        # 1. 上涨天数 >= day2
        # 2. 涨幅超n%天数 >= day3
        # 3. 有效交易天数 >= 统计天数的80%（避免因停牌导致数据不全）
        filtered_stocks = stock_stats[
            (stock_stats['up_days'] >= day2) &
            (stock_stats['over_n_days'] >= day3) &
            (stock_stats['valid_days'] >= int(day1 * 1))
            ]

        top_m_stocks = filtered_stocks.sort_values('total_growth', ascending=False).head(m)
        top_m_stocks = top_m_stocks.assign(rank=range(len(top_m_stocks)))  # 新增排名列
        # 1. 提取符合条件的股票代码列表
        top_codes = top_m_stocks['ts_code'].tolist()
        # 2. 从原始数据中筛选这些股票的最近day1天数据
        result_df = recent_df[recent_df['ts_code'].isin(top_codes)].copy()
        # 3. 合并排名信息，按排名和日期排序
        # 将股票代码与排名关联
        rank_mapping = top_m_stocks[['ts_code', 'rank']]
        # 合并排名到结果数据框
        result_df = result_df.merge(rank_mapping, on='ts_code', how='left')
        # 按排名（升序，涨幅高的在前）和日期（升序，旧到新）排序
        result_df = result_df.sort_values(by=['rank', 'date'])
        # 移除临时的rank列（可选，根据需要保留）
        result_df = result_df.drop(columns='rank')

        # 返回股票代码列表和按top m排序的原始数据框
        return top_codes, result_df

    def filter_limit_up2(self, df: pd.DataFrame, params: Dict) -> (List, pd.DataFrame):
        #     # 最近day1天中涨幅top n
        #     {"type": "3", "name": "涨幅条件2", "enable": True, "params": {"day1": 5, "n": 50}},
        day1 = params.get('day1', 1)
        n = params.get('n', 100)

        # 筛选最近day1天的数据
        latest_date = df['date'].max()
        latest_date_str = latest_date.strftime(const.DATE_FORMAT)
        start_date_str, _ = utils.get_start_date(self.pro, day1, latest_date_str)
        start_date = datetime.strptime(start_date_str, const.DATE_FORMAT)
        recent_df = df[df['date'] >= start_date].copy()

        if recent_df.empty:
            print(f"在最近{day1}天内没有找到股票数据")
            return []

        # 按股票代码分组,获取每个股票的首尾交易日收盘价
        def calculate_growth(group):
            # 按日期排序
            group_sorted = group.sort_values('date')
            # 首日收盘价
            first_close = group_sorted.iloc[0]['close']
            # 最后一日收盘价
            last_close = group_sorted.iloc[-1]['close']
            # 计算区间涨幅
            growth = (last_close - first_close) / first_close * 100
            return pd.Series({
                'growth': growth,
                'days_count': len(group_sorted)  # 实际交易天数
            })

        # 计算每个股票的区间涨幅
        stock_growth = recent_df.groupby('ts_code').apply(calculate_growth).reset_index()

        # 过滤掉交易天数不足的股票（至少达到统计天数的80%）
        valid_stocks = stock_growth[stock_growth['days_count'] >= int(day1 * 1)]

        if valid_stocks.empty:
            print(f"没有足够交易数据的股票（至少需要{int(day1 * 1)}个交易日）")
            return []

        # 按涨幅降序排序,取前n名
        top_stocks = valid_stocks.sort_values('growth', ascending=False).head(n)
        top_stocks = top_stocks.assign(rank=range(len(top_stocks)))  # 新增排名列
        # 1. 提取符合条件的股票代码列表
        top_codes = top_stocks['ts_code'].tolist()
        # 2. 从原始数据中筛选这些股票的最近day1天数据
        result_df = recent_df[recent_df['ts_code'].isin(top_codes)].copy()
        # 3. 合并排名信息，按排名和日期排序
        # 将股票代码与排名关联
        rank_mapping = top_stocks[['ts_code', 'rank']]
        # 合并排名到结果数据框
        result_df = result_df.merge(rank_mapping, on='ts_code', how='left')
        # 按排名（升序，涨幅高的在前）和日期（升序，旧到新）排序
        result_df = result_df.sort_values(by=['rank', 'date'])
        # 移除临时的rank列（可选，根据需要保留）
        result_df = result_df.drop(columns='rank')

        # 返回股票代码列表和按top m排序的原始数据框
        return top_codes, result_df

    def filter_vol1(self, df: pd.DataFrame, params: Dict) -> (List, pd.DataFrame):
        #     # 挑选day1天内成交量均值的top n
        #     {"type": "4", "name": "成交量条件1", "enable": True, "params": {"day1": 2, "n": 20}},
        day1 = params.get('day1', 1)
        n = params.get('n', 100)

        latest_date = df['date'].max()
        latest_date_str = latest_date.strftime(const.DATE_FORMAT)
        start_date_str, _ = utils.get_start_date(self.pro, day1, latest_date_str)
        start_date = datetime.strptime(start_date_str, const.DATE_FORMAT)
        recent_df = df[df['date'] >= start_date].copy()

        if recent_df.empty:
            print(f"在最近{day1}天内没有找到股票数据")
            return []

        # 按股票代码分组,计算成交量均值和有效交易天数
        def calculate_volume_stats(group):
            return pd.Series({
                'vol_mean': group['vol'].mean(),  # 成交量均值
                'days_count': len(group)  # 有效交易天数
            })

        # 计算每个股票的成交量统计数据
        volume_stats = recent_df.groupby('ts_code').apply(calculate_volume_stats).reset_index()

        # 过滤掉交易天数不足的股票（至少达到统计天数的80%）和成交量异常的股票
        valid_stocks = volume_stats[
            (volume_stats['days_count'] >= int(day1 * 1)) &
            (volume_stats['vol_mean'] > 0)  # 排除成交量为0的异常数据
            ]

        if valid_stocks.empty:
            print(f"没有足够交易数据的股票（至少需要{int(day1 * 1)}个交易日且成交量正常）")
            return []

        # 按成交量均值降序排序,取前n名
        top_stocks = valid_stocks.sort_values('vol_mean', ascending=False).head(n)
        top_stocks = top_stocks.assign(rank=range(len(top_stocks)))  # 新增排名列
        # 1. 提取符合条件的股票代码列表
        top_codes = top_stocks['ts_code'].tolist()
        # 2. 从原始数据中筛选这些股票的最近day1天数据
        result_df = recent_df[recent_df['ts_code'].isin(top_codes)].copy()
        # 3. 合并排名信息，按排名和日期排序
        # 将股票代码与排名关联
        rank_mapping = top_stocks[['ts_code', 'rank']]
        # 合并排名到结果数据框
        result_df = result_df.merge(rank_mapping, on='ts_code', how='left')
        # 按排名（升序，涨幅高的在前）和日期（升序，旧到新）排序
        result_df = result_df.sort_values(by=['rank', 'date'])
        # 移除临时的rank列（可选，根据需要保留）
        result_df = result_df.drop(columns='rank')

        # 返回股票代码列表和按top m排序的原始数据框
        return top_codes, result_df

    def filter_amount1(self, df: pd.DataFrame, params: Dict) -> (List, pd.DataFrame):
        #     # 挑选day1天内成交量均值的top n
        #     {"type": "5", "name": "成交额条件1", "enable": True, "params": {"day1": 2, "n": 20}},
        day1 = params.get('day1', 1)
        n = params.get('n', 100)

        latest_date = df['date'].max()
        latest_date_str = latest_date.strftime(const.DATE_FORMAT)
        start_date_str, _ = utils.get_start_date(self.pro, day1, latest_date_str)
        start_date = datetime.strptime(start_date_str, const.DATE_FORMAT)
        recent_df = df[df['date'] >= start_date].copy()

        if recent_df.empty:
            print(f"在最近{day1}天内没有找到股票数据")
            return []

        # 按股票代码分组,计算成交量均值和有效交易天数
        def calculate_volume_stats(group):
            return pd.Series({
                'amount_mean': group['amount'].mean(),  # 成交量均值
                'days_count': len(group)  # 有效交易天数
            })

        # 计算每个股票的成交量统计数据
        volume_stats = recent_df.groupby('ts_code').apply(calculate_volume_stats).reset_index()

        # 过滤掉交易天数不足的股票（至少达到统计天数的80%）和成交量异常的股票
        valid_stocks = volume_stats[
            (volume_stats['days_count'] >= int(day1 * 1)) &
            (volume_stats['amount_mean'] > 0)  # 排除成交量为0的异常数据
            ]

        if valid_stocks.empty:
            print(f"没有足够交易数据的股票（至少需要{int(day1 * 1)}个交易日且成交量正常）")
            return []

        # 按成交量均值降序排序,取前n名
        top_stocks = valid_stocks.sort_values('amount_mean', ascending=False).head(n)
        top_stocks = top_stocks.assign(rank=range(len(top_stocks)))  # 新增排名列
        # 1. 提取符合条件的股票代码列表
        top_codes = top_stocks['ts_code'].tolist()
        # 2. 从原始数据中筛选这些股票的最近day1天数据
        result_df = recent_df[recent_df['ts_code'].isin(top_codes)].copy()
        # 3. 合并排名信息，按排名和日期排序
        # 将股票代码与排名关联
        rank_mapping = top_stocks[['ts_code', 'rank']]
        # 合并排名到结果数据框
        result_df = result_df.merge(rank_mapping, on='ts_code', how='left')
        # 按排名（升序，涨幅高的在前）和日期（升序，旧到新）排序
        result_df = result_df.sort_values(by=['rank', 'date'])
        # 移除临时的rank列（可选，根据需要保留）
        result_df = result_df.drop(columns='rank')

        # 返回股票代码列表和按top m排序的原始数据框
        return top_codes, result_df

    def filter_vol_ratio1(self, df: pd.DataFrame, params: Dict) -> (List, pd.DataFrame):
        #     # 最近day1天量比均大于n
        #     {"type": "6", "name": "量比条件1", "enable": True, "params": {"day1": 1, "n": 1.5}}
        day1 = params.get('day1', 1)
        n = params.get('n', 100)

        latest_date = df['date'].max()
        latest_date_str = latest_date.strftime(const.DATE_FORMAT)
        start_date_str, _ = utils.get_start_date(self.pro, day1, latest_date_str)
        start_date = datetime.strptime(start_date_str, const.DATE_FORMAT)
        recent_df = df[df['date'] >= start_date].copy()

        if recent_df.empty:
            print(f"在最近{day1}天内没有找到股票数据")
            return []

        # 按股票代码分组,检查每天量比是否都大于n
        def check_vol_ratio_condition(group):
            # 1. 量比均大于n
            all_above_threshold = (group['vol_ratio'] > n).all()
            # 2. 有效交易天数是否足够（达到统计天数的80%）
            days_enough = len(group) >= int(day1 * 1)
            # 3. 排除量比为NaN的异常数据
            no_missing_data = not group['vol_ratio'].isna().any()

            return pd.Series({
                'all_above_threshold': all_above_threshold,
                'days_enough': days_enough,
                'no_missing_data': no_missing_data,
            })

        # 应用分组检查
        vol_ratio_check = recent_df.groupby('ts_code').apply(check_vol_ratio_condition).reset_index()
        # 筛选同时满足所有条件的股票
        filtered_stocks = vol_ratio_check[
            vol_ratio_check['all_above_threshold'] &
            vol_ratio_check['days_enough'] &
            vol_ratio_check['no_missing_data']
            ]
        if filtered_stocks.empty:
            print(f"没有找到最近{day1}天内每天量比均大于{n}的股票")
            return []
        # 提取符合条件的股票代码列表
        qualified_codes = filtered_stocks['ts_code'].tolist()
        # 从原始数据中筛选这些股票的最近day1天数据
        result_df = recent_df[recent_df['ts_code'].isin(qualified_codes)].copy()
        # 按股票代码和日期排序（同一只股票内按时间升序）
        result_df = result_df.sort_values(by=['ts_code', 'date'])
        # 返回股票代码列表和对应的原始数据框
        return qualified_codes, result_df

    def filter_stocks(self, target_date_str: str, conditions: List[Dict]) -> List:
        final_mask = []
        get_days = 2
        for i, condition in enumerate(conditions):
            if not condition.get('enable', False):
                continue
            condition_type = condition.get('type')
            if condition_type not in self.condition_handlers:
                raise ValueError(f"不支持的条件类型: {condition_type}")
            params = condition.get('params', {})
            day1 = params.get('day1', 0)
            get_days = max(get_days, day1)
        start_date_str, end_date_str = utils.get_start_date(self.pro, get_days, target_date_str)
        print("获取总交易日:", get_days, ", 获取数据开始日期:", start_date_str, ",结束日期:", end_date_str)
        results = self.tdx_daily_coll.find({"trade_date": {"$gte": start_date_str, "$lte": end_date_str}},
                                           {"_id": 0})
        df = pd.DataFrame(list(results))
        try:
            df["date"] = pd.to_datetime(df["trade_date"], format=const.DATE_FORMAT)
        except ValueError as e:
            raise ValueError(f"日期转换失败: {str(e)}")
        df = df.sort_values(by="date").reset_index(drop=False)  # 按日期降序排列
        for i, condition in enumerate(conditions):
            if not condition.get('enable', False):
                print("跳过执行: ", condition.get('name', ""))
                continue
            condition_type = condition.get('type')
            handler = self.condition_handlers[condition_type]
            params = condition.get('params', {})
            print("开始执行-", condition.get('name', ""), "总共:", df.groupby('ts_code').ngroups, "个概念股")
            result, df_new = handler(df, params)
            df = df_new
            final_mask = result
            print("执行结果-", condition.get('name', ""), ", 筛选后总共:", len(result), ", 结果:", result)
            # condition_results[condition_type] = result
        # print("执行公式:", logic_expr)
        # final_mask = self.parse_logic_expression(logic_expr, condition_results)
        print("最终结果:", final_mask)
        return final_mask
