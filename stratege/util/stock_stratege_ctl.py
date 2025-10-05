import pandas as pd
import numpy as np
from typing import List, Dict, Callable, Any


class StockFilter:
    def __init__(self):
        """初始化股票筛选器，注册所有可用的筛选条件"""
        self.condition_handlers = {
            # 注册条件处理函数，键为条件类型，值为对应的处理函数
            "涨停天数": self.filter_limit_up_days,
            "成交量放大": self.filter_volume_increase,
            "BBI上穿": self.filter_bbi_crossover,
            "收盘价上涨": self.filter_close_rise,
            "MACD金叉": self.filter_macd_golden_cross,
            # 可以根据需要添加更多条件
        }

    def add_condition_handler(self, condition_type: str, handler: Callable):
        """添加自定义条件处理函数"""
        self.condition_handlers[condition_type] = handler

    def filter_limit_up_days(self, df: pd.DataFrame, params: Dict) -> pd.Series:
        """筛选连续涨停天数满足条件的股票
        params: {"days": 整数}
        """
        # 假设涨跌幅超过9.8%视为涨停
        limit_up = (df['pct_change'] >= 9.8)

        # 计算连续涨停天数
        consecutive_days = (limit_up.groupby((~limit_up).cumsum()).cumcount() + 1) * limit_up

        # 至少有一次达到连续涨停天数要求
        return consecutive_days.groupby(level=0).max() >= params['days']

    def filter_volume_increase(self, df: pd.DataFrame, params: Dict) -> pd.Series:
        """筛选成交量放大的股票
        params: {"days1": 近期天数, "days2": 前期天数, "multiple": 放大倍数}
        """
        # 计算近期平均成交量
        recent_vol_mean = df['volume'].groupby(level=0).tail(params['days1']).groupby(level=0).mean()

        # 计算前期平均成交量
        prior_vol_mean = df.groupby(level=0)['volume'].apply(
            lambda x: x.iloc[-(params['days1'] + params['days2']):-params['days1']].mean()
            if len(x) >= (params['days1'] + params['days2']) else 0
        )

        # 避免除零错误
        prior_vol_mean = prior_vol_mean.replace(0, np.nan)

        # 近期成交量 > 前期成交量 * 放大倍数
        return (recent_vol_mean / prior_vol_mean) >= params['multiple']

    def filter_bbi_crossover(self, df: pd.DataFrame, params: Dict) -> pd.Series:
        """筛选BBI上穿的股票
        params: {"enabled": 布尔值}
        """
        if not params.get('enabled', True):
            return pd.Series(True, index=df.index.unique())

        # 计算BBI指标 (多空指标)
        df = df.copy()
        df['bbi'] = (df['close'].rolling(window=3).mean() +
                     df['close'].rolling(window=6).mean() +
                     df['close'].rolling(window=12).mean() +
                     df['close'].rolling(window=24).mean()) / 4

        # 检查是否上穿：前一天收盘价 < BBI，当前收盘价 > BBI
        df['cross_over'] = (df['close'] > df['bbi']) & (df['close'].shift(1) < df['bbi'].shift(1))

        # 最近出现过上穿
        return df.groupby(level=0)['cross_over'].any()

    def filter_close_rise(self, df: pd.DataFrame, params: Dict) -> pd.Series:
        """筛选连续上涨天数达标的股票
        params: {"days": 整数}
        """
        # 收盘价上涨
        price_rise = df['close'] > df['close'].shift(1)

        # 计算连续上涨天数
        consecutive_rise = (price_rise.groupby((~price_rise).cumsum()).cumcount() + 1) * price_rise

        # 至少有一次达到连续上涨天数要求
        return consecutive_rise.groupby(level=0).max() >= params['days']

    def filter_macd_golden_cross(self, df: pd.DataFrame, params: Dict) -> pd.Series:
        """筛选MACD出现金叉的股票
        params: {"enabled": 布尔值}
        """
        if not params.get('enabled', True):
            return pd.Series(True, index=df.index.unique())

        # 计算MACD指标
        df = df.copy()
        df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['dif'] = df['ema12'] - df['ema26']
        df['dea'] = df['dif'].ewm(span=9, adjust=False).mean()

        # 金叉：DIF由下向上穿过DEA
        df['golden_cross'] = (df['dif'] > df['dea']) & (df['dif'].shift(1) < df['dea'].shift(1))

        # 最近出现过金叉
        return df.groupby(level=0)['golden_cross'].any()

    def parse_logic_expression(self, logic_expr: str, condition_results: List[pd.Series]) -> pd.Series:
        """解析逻辑表达式，组合多个条件的结果"""
        # 创建条件映射，用变量名替代条件索引（如将1替换为cond_1）
        for i in range(len(condition_results)):
            logic_expr = logic_expr.replace(str(i + 1), f"cond_{i}")

        # 创建局部变量字典，存储每个条件的结果
        locals_dict = {f"cond_{i}": cond for i, cond in enumerate(condition_results)}

        try:
            # 执行逻辑表达式计算最终筛选结果
            return eval(logic_expr, globals(), locals_dict)
        except Exception as e:
            raise ValueError(f"逻辑表达式解析错误: {str(e)}，表达式: {logic_expr}")

    def filter_stocks(self, df: pd.DataFrame, conditions: List[Dict], logic_expr: str) -> pd.DataFrame:
        condition_results = []
        for i, condition in enumerate(conditions):
            condition_type = condition.get('type')
            params = condition.get('params', {})

            if condition_type not in self.condition_handlers:
                raise ValueError(f"不支持的条件类型: {condition_type}")

            # 执行条件筛选
            handler = self.condition_handlers[condition_type]
            result = handler(df, params)
            condition_results.append(result)

        # 解析逻辑表达式，组合筛选结果
        final_mask = self.parse_logic_expression(logic_expr, condition_results)

        # 应用筛选结果
        selected_stocks = final_mask[final_mask].index
        filtered_df = df[df.index.get_level_values(0).isin(selected_stocks)]

        return filtered_df



