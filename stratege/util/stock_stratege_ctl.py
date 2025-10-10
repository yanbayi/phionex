import re
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple
from db_ctl.util import mongoDb_ctl
from common import const, utils
from data_ctl import tushare_ctl

is_check = False

class StockFilter:
    def __init__(self, get_tdx_list: List, target_date_str: str, is_use_tdx_conditions: bool):
        self.condition_handlers = {
            "1": self.filter_bbi_crossover,
            "2": self.filter_limit_up1,
            "3": self.filter_limit_up2,
            "4": self.filter_vol1,
            "5": self.filter_vol2,
            "6": self.filter_vol_ratio1,
            "7": self.filter_ma1,
            "8": self.filter_limit_up3,
            "9": self.filter_boll1,
            "10": self.filter_boll2,
            "11": self.filter_macd1,
        }
        self.pro = tushare_ctl.init_tushare_client()
        self.stock_basic_coll = mongoDb_ctl.init_mongo_collection(const.MONGO_BASIC_COLL)
        self.stock_daily_coll = mongoDb_ctl.init_mongo_collection(const.MONGO_DAILY_COLL)
        self.tdx_member_coll = mongoDb_ctl.init_mongo_collection(const.TDX_MEMBER)
        self.tdx_list = get_tdx_list
        self.stock_base_df = pd.DataFrame()
        self.target_date_str = target_date_str
        self.is_use_tdx_conditions = is_use_tdx_conditions
        # day1 = params.get('day1', 1)
        # day2 = params.get('day2', 1)
        # day3 = params.get('day3', 1)
        # n = params.get('n', 100)
        # m = params.get('m', 100)
        # j = params.get('j', 100)

    def get_stock_from_tdx(self, get_tdx_list: List, start_date_str: str, end_date_str: str) -> int:
        stock = self.tdx_member_coll.find({"ts_code": {"$in": get_tdx_list}}, {"con_code": 1})
        stock_list = set([doc["con_code"] for doc in stock])
        print("概念板块中包含股票:", len(stock_list), "只")
        if not self.is_use_tdx_conditions:
            stock_cursor = self.stock_daily_coll.find(
                {"trade_date": {"$gte": start_date_str, "$lte": end_date_str}},
                {"_id": 0})
        else:
            stock_cursor = self.stock_daily_coll.find(
                {"ts_code": {"$in": list(stock_list)}, "trade_date": {"$gte": start_date_str, "$lte": end_date_str}},
                {"_id": 0})
        df = pd.DataFrame(list(stock_cursor))
        try:
            df["date"] = pd.to_datetime(df["trade_date"], format=const.DATE_FORMAT)
        except ValueError as e:
            raise ValueError(f"日期转换失败: {str(e)}")
        self.stock_base_df = df.sort_values(by=['ts_code', 'date']).reset_index(drop=False)
        return self.stock_base_df.groupby('ts_code').ngroups

    def filter_bbi_crossover(self, params: Dict) -> List:

        day1 = params.get('day1', 1)

        def check_crossover(group):
            recent_data = group.tail(day1)  # 只取最近n天的数据
            if len(recent_data) < 2:
                return False
            # 计算每日是否收盘价在BBI上方
            recent_data['above_bbi'] = recent_data['close'] > recent_data['bbi']
            has_crossover = (
                    (recent_data['above_bbi'].shift(1) == False) &
                    (recent_data['above_bbi'] == True)
            ).any()
            return has_crossover

        # 按股票代码分组检查
        crossover_result = self.stock_base_df.groupby('ts_code').apply(check_crossover)
        # 提取有上穿信号的股票代码
        qualified_stocks = crossover_result[crossover_result].index.tolist()
        return qualified_stocks

    def filter_limit_up1(self, params: Dict) -> List:
        day1 = params.get('day1', 1)
        day2 = params.get('day2', 1)
        day3 = params.get('day3', 1)
        n = params.get('n', 100)

        def check_up_conditions(group):
            recent_data = group.tail(day1)
            if len(recent_data) < day1:
                return False
            # 1. 统计上涨天数（涨跌幅>0）
            up_days_count = (recent_data['pct_chg'] > 0).sum()
            # 2. 统计涨幅超过n%的天数（涨跌幅>n）
            over_n_days_count = (recent_data['pct_chg'] > n).sum()
            # 同时满足两个条件
            return (up_days_count >= day2) and (over_n_days_count >= day3)

        condition_met = self.stock_base_df.groupby('ts_code').apply(check_up_conditions)
        qualified_stocks = condition_met[condition_met].index.tolist()
        return qualified_stocks

    def filter_limit_up2(self, params: Dict) -> List:
        day1 = params.get('day1', 1)
        n = params.get('n', 100)

        def check_growth_condition(group):
            recent_data = group.tail(day1)
            if len(recent_data) < day1:
                return False
            # 计算day1天内的累计涨幅：(最后一天收盘价 / 第一天收盘价 - 1) * 100
            first_close = recent_data.iloc[0]['close']
            last_close = recent_data.iloc[-1]['close']
            total_growth = (last_close / first_close - 1) * 100
            # 判断是否超过涨幅阈值
            return total_growth > n

        condition_met = self.stock_base_df.groupby('ts_code').apply(check_growth_condition)
        qualified_stocks = condition_met[condition_met].index.tolist()
        return qualified_stocks

    def filter_limit_up3(self, params: Dict) -> List:
        day1 = params.get('day1', 1)
        n = params.get('n', 1)

        def check_growth_condition(group):
            recent_data = group.tail(day1)
            if len(recent_data) < day1:
                return False
            # 计算day1天内的累计涨幅：(最后一天收盘价 / 第一天收盘价 - 1) * 100
            first_close = recent_data.iloc[0]['close']
            last_close = recent_data.iloc[-1]['close']
            total_growth = (last_close / first_close - 1) * 100
            # 判断是否超过涨幅阈值
            return total_growth < n

        condition_met = self.stock_base_df.groupby('ts_code').apply(check_growth_condition)
        qualified_stocks = condition_met[condition_met].index.tolist()
        return qualified_stocks

    def filter_vol1(self, params: Dict) -> List:
        day1 = params.get('day1', 1)
        n = params.get('n', 100)

        def check_volume_condition(group):
            recent_data = group.tail(day1)
            if len(recent_data) < day1:
                return False
            # 计算近期（除当天外的day1-1天）平均成交量作为基准
            # 避免用当天数据计算均值，防止放大效应被稀释
            if len(recent_data) >= 5:  # 数据充足时用前5天均值更稳定
                base_period = recent_data.iloc[:-1].head(5)  # 取前5天作为基准
            else:
                base_period = recent_data.iloc[:-1]  # 数据较少时用除当天外的所有数据
            if len(base_period) == 0:
                return False  # 避免除数为0
            base_volume = base_period['vol'].mean()
            # 检查最近day1天内是否有一天成交量≥基准的n倍
            has_spike = (recent_data['vol'] >= base_volume * n).any()
            return has_spike

        condition_met = self.stock_base_df.groupby('ts_code').apply(check_volume_condition)
        qualified_stocks = condition_met[condition_met].index.tolist()
        return qualified_stocks

    def filter_vol2(self, params: Dict) -> List:
        day1 = params.get('day1', 1)
        day2 = params.get('day2', 1)
        n = params.get('n', 100)

        def check_volume_ratio(group):
            # 总需要的数据量：day1（近期） + day2（前期）
            required_days = day1 + day2
            if len(group) < required_days:
                return False
            window_data = group.tail(required_days)
            # 拆分近期和前期窗口
            # 近期：最后day1天
            recent_period = window_data.tail(day1)
            # 前期：近期之前的day2天（不重叠）
            previous_period = window_data.iloc[:day2]

            # 计算两个窗口的成交量均值
            recent_mean = recent_period['vol'].mean()
            previous_mean = previous_period['vol'].mean()
            # 避免前期均值为0导致的除零错误
            if previous_mean == 0:
                return False
            # 检查近期均值是否达到前期均值的n倍以上
            return (recent_mean / previous_mean) > n

        condition_met = self.stock_base_df.groupby('ts_code').apply(check_volume_ratio)
        qualified_stocks = condition_met[condition_met].index.tolist()
        return qualified_stocks

    def filter_vol_ratio1(self, params: Dict) -> List:
        day1 = params.get('day1', 1)
        n = params.get('n', 100)

        def check_vol_ratio_condition(group):
            # 取最近day1天的数据
            recent_data = group.tail(day1)
            # 数据不足day1天则排除
            if len(recent_data) < day1:
                return False
            # 检查是否所有天数的量比都大于n
            all_above = (recent_data['volume_ratio'] > n).all()
            # 检查是否存在量比为NaN的异常数据
            no_missing = not recent_data['volume_ratio'].isna().any()
            # 同时满足两个条件
            return all_above and no_missing

        # 按股票代码分组检查条件
        condition_met = self.stock_base_df.groupby('ts_code').apply(check_vol_ratio_condition)
        # 提取符合条件的股票代码
        qualified_stocks = condition_met[condition_met].index.tolist()
        return qualified_stocks

    def filter_ma1(self, params: Dict) -> List:
        # 最近day1天内，ma10上穿ma20 n 次以上
        # {"type": "7", "name": "均线条件1", "enable": True, "params": {"day1": 60, "n": 2}}
        day1 = params.get('day1', 1)
        n = params.get('n', 2)

        def check_crossover_condition(group):
            # 取最近day1天的数据
            recent_data = group.tail(day1)
            # 数据不足2天无法形成交叉，直接排除
            if len(recent_data) < 2:
                return False
            # 标记MA10在MA20上方的日期
            recent_data['ma10_above'] = recent_data['ma10'] > recent_data['ma20']

            # 识别上穿信号：前一天在下方，当天在上方
            # 使用fill_value=False处理第一天无前置数据的情况
            crossover_signals = (
                    recent_data['ma10_above'] &
                    ~recent_data['ma10_above'].shift(1, fill_value=False)
            )

            # 统计上穿次数并判断是否达标
            return crossover_signals.sum() >= n

        # 按股票代码分组检查条件
        condition_met = self.stock_base_df.groupby('ts_code').apply(check_crossover_condition)

        # 提取符合条件的股票代码
        qualified_stocks = condition_met[condition_met].index.tolist()

        return qualified_stocks

    def filter_boll1(self, params: Dict) -> List:
        # 最近day1天排除最近day2天内的数据 ((boll_upper-boll_lower)/boll_mid * 100)/close * 100 一直在n ~ m区间的股票
        #  {"type": "9", "name": "BOLL条件1", "enable": True, "params": {"day1": 10,"day2": 10, "n": 5, "m": 8}}
        day1 = params.get('day1', 1)
        day2 = params.get('day2', 1)
        n = params.get('n', 10)
        m = params.get('m', 2)
        stock_calculations = {} # aa
        def check_band_ratio_condition(group):
            ts_code = group['ts_code'].iloc[0]  # aa 获取当前股票代码

            # 取最近day天的数据
            recent_data = group.tail(day1).head(day2*-1)
            # 验证区间数据量是否正确
            if len(recent_data) != (day1 - day2):
                return False

            # 计算BOLL带宽 = 上轨 - 下轨
            recent_data['boll_bandwidth'] = recent_data['boll_upper'] - recent_data['boll_lower']

            # 计算(带宽/收盘价)的百分比
            # 避免收盘价为0导致除零错误
            recent_data['band_ratio_pct'] = (recent_data['boll_bandwidth'] / recent_data['close']) * 100

            # 检查是否存在收盘价为0或比例计算异常的情况
            if recent_data['band_ratio_pct'].isna().any():
                print(recent_data.to_string(index=False))
                return False

            # 检查所有天数的比例是否都在[n%, m%]区间内
            within_range = (
                    (recent_data['band_ratio_pct'] >= n) &
                    (recent_data['band_ratio_pct'] <= m)
            ).all()
            stock_calculations[ts_code] = (within_range, recent_data) # aa
            return within_range

        # 按股票代码分组检查条件
        condition_met = self.stock_base_df.groupby('ts_code').apply(check_band_ratio_condition)
        # 提取符合条件的股票代码
        qualified_stocks = condition_met[condition_met].index.tolist()

        if is_check:
            print("\n===== BOLL条件验证数据 =====")
            for ts_code in qualified_stocks:
                is_qualified, data = stock_calculations[ts_code]
                print(f"\n股票代码: {ts_code} (符合条件: {is_qualified})")
                # 选择关键列打印，包含原始数据和计算结果
                display_cols = [
                    'trade_date', 'boll_upper', 'boll_lower', 'boll_mid',
                    'close', 'boll_bandwidth', 'band_ratio_pct'
                ]
                # 只显示存在的列
                available_cols = [col for col in display_cols if col in data.columns]
                print(data[available_cols].to_string(index=False))

        return qualified_stocks

    def filter_boll2(self, params: Dict) -> List:
        day1 = params.get('day1', 1)
        day2 = params.get('day2', 1)
        def check_close_above_mid(group):
            # 取最近day天的数据
            recent_data = group.tail(day1).head(day2 * -1)
            # 验证区间数据量是否正确
            if len(recent_data) != (day1 - day2):
                return False
            all_above = (recent_data['close'] > recent_data['boll_mid']).all()
            return all_above
        condition_met = self.stock_base_df.groupby('ts_code').apply(check_close_above_mid)
        qualified_stocks = condition_met[condition_met].index.tolist()
        return qualified_stocks

    def filter_macd1(self, params: Dict) -> List:
        day1 = params.get('day1', 1)
        day2 = params.get('day2', 1)

        def check_close_above_mid(group):
            # 取最近day天的数据
            recent_data = group.tail(day1).head(day2 * -1)
            # 验证区间数据量是否正确
            if len(recent_data) != (day1 - day2):
                return False
            all_above = (recent_data['macd_dif'] > 0).all()
            return all_above
        condition_met = self.stock_base_df.groupby('ts_code').apply(check_close_above_mid)
        qualified_stocks = condition_met[condition_met].index.tolist()
        return qualified_stocks

    def parse_logic_expression(self, logic_expr: str, condition_results: Dict[str, List[str]]) -> List:
        expr = logic_expr.replace('（', '(').replace('）', ')')
        expr = expr.replace('and', '&').replace('or', '|')
        keys = re.findall(r'\b\d+\b', expr)
        for key in keys:
            if key not in condition_results:
                raise ValueError(f"表达式中的键 '{key}' 不存在于数据中")
        for key in keys:
            expr = re.sub(rf'\b{key}\b', f"set(condition_results['{key}'])", expr)
        try:
            result_set = eval(expr)
            return list(result_set)
        except Exception as e:
            raise ValueError(f"表达式执行错误: {str(e)}, 转换后的表达式: {expr}")

    def filter_stocks(self, conditions: List[Dict], logic_expr: str) -> List:
        condition_results: Dict[str, List[str]] = {}
        get_days = 5
        for i, condition in enumerate(conditions):
            if not condition.get('enable', False):
                continue
            condition_type = condition.get('type')
            if condition_type not in self.condition_handlers:
                raise ValueError(f"不支持的条件类型: {condition_type}")
            params = condition.get('params', {})
            day1 = params.get('day1', 0)
            day2 = params.get('day2', 0)
            day_sum = day1 + day2
            get_days = max(get_days, day_sum)
        start_date_str, end_date_str = utils.get_start_date(self.pro, get_days, self.target_date_str)
        print("获取总交易日:", get_days, ", 获取数据开始日期:", start_date_str, ",结束日期:", end_date_str)
        count_stock = self.get_stock_from_tdx(self.tdx_list, start_date_str, end_date_str)
        for i, condition in enumerate(conditions):
            if not condition.get('enable', False):
                print("跳过执行: ", condition.get('name', ""))
                continue
            condition_type = condition.get('type')
            handler = self.condition_handlers[condition_type]
            params = condition.get('params', {})
            print("开始执行-", condition.get('name', ""), "总共:", count_stock, "只股")
            result = handler(params)
            print("执行结果-", condition.get('name', ""), ", 筛选后总共:", len(result), ", 结果:", result)
            condition_results[condition_type] = result
        print("执行公式:", logic_expr)
        final_mask = self.parse_logic_expression(logic_expr, condition_results)
        print("最终结果共", len(final_mask), "只股票, 具体是:", final_mask)
        return final_mask

    def backtest_stocks(self, stock_list: List[str], backtest_day: int):
        start_date_str, end_date_str = utils.get_start_date(self.pro, 1, self.target_date_str)
        start_date_str = end_date_str
        end_date = datetime.strptime(start_date_str, const.DATE_FORMAT)
        end_date += timedelta(days=backtest_day)
        end_date_str = end_date.strftime(const.DATE_FORMAT)
        # 3. 批量查询股票数据
        pipeline = [
            {
                "$match": {
                    "ts_code": {"$in": stock_list},
                    "trade_date": {"$gte": start_date_str, "$lte": end_date_str},
                }
            },
            {
                "$sort": {"ts_code": 1, "trade_date": 1}
            }
        ]
        cursor = self.stock_daily_coll.aggregate(pipeline)
        df = pd.DataFrame(list(cursor))
        if df.empty:
            raise ValueError(f"在{start_date_str}之后没有找到指定股票的数据")
        try:
            df["date"] = pd.to_datetime(df["trade_date"], format=const.DATE_FORMAT)
        except ValueError as e:
            raise ValueError(f"日期转换失败: {str(e)}")

        # 5. 计算每日收益（(当日收盘价/前一日收盘价 - 1)）
        df = df.sort_values(by=["ts_code", "date"])
        result_dfs = []  # 存储每只股票的处理结果
        total_returns = {}  # 存储每只股票的总收益
        for code in stock_list:
            # 筛选单只股票数据
            stock_data = df[df["ts_code"] == code].copy()
            if len(stock_data) < 2:
                total_returns[code] = 0.0  # 数据不足时总收益记为0
                continue  # 数据不足则跳过
            # 计算每日收益 (当日收盘价/前一日收盘价 - 1)
            stock_data["daily_return"] = stock_data["close"].pct_change()
            start_close = stock_data.iloc[0]["close"]
            stock_data["cumulative_total"] = (stock_data["close"] / start_close) - 1
            # 记录最终总收益（最后一个交易日的累计收益）
            final_total = stock_data["cumulative_total"].iloc[-1]
            total_returns[code] = final_total
            # 重命名列，区分不同股票
            stock_data = stock_data.rename(columns={
                "daily_return": f"{code}_daily",
                "cumulative_total": f"{code}_total"
            })

            # 保留日期和计算结果列
            result_dfs.append(stock_data[["date", f"{code}_daily", f"{code}_total"]])

        # 合并所有股票的结果（按日期对齐）
        if not result_dfs:
            raise ValueError("没有足够的有效数据用于计算收益")

        returns_df = result_dfs[0]
        for df_part in result_dfs[1:]:
            returns_df = returns_df.merge(df_part, on="date", how="outer")

        # 按日期排序
        returns_df = returns_df.sort_values("date").set_index("date")
        # 打印结果
        print("每日收益（百分比形式）：")
        print((returns_df * 100).to_string())  # 转换为百分比显示

        # 打印每只股票的总收益
        print("\n每只股票的总收益（百分比）：")
        for code, ret in total_returns.items():
            print(f"{code}: {ret * 100:.2f}%")

        # 计算并打印所有股票的平均总收益
        valid_returns = [ret for ret in total_returns.values() if ret != 0.0]
        if valid_returns:
            avg_return = sum(valid_returns) / len(valid_returns)
            print(f"\n所有股票的平均总收益：{avg_return * 100:.2f}%")
