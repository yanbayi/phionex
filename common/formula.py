import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from db_ctl.util import mongoDb_ctl
from common import const
from typing import List, Dict, Optional
import chinadata.ca_data as ts

# asi asit   dfma atr dmi dpo  expma  ktn ema  mass  mfi  mtm  psy   roc  taq  trix vr wr  xsii bias cci emv

# 趋势指标 bbi macd? sar?
# 反趋势指标 kdj rsi
# 压力支撑指标  boll
# 量价指标 obv
# 量能指标 brar cr
# 多日均线 ma5 ma6 ma10 ma20 ma60
def formula_main(indicators: str, data: pd.DataFrame) -> pd.DataFrame:
    required_columns = ['open', 'high', 'low', 'close', 'trade_date']
    if not set(required_columns).issubset(data.columns):
        missing = [col for col in required_columns if col not in data.columns]
        raise ValueError(f"数据缺少必要的列: {missing}")
    try:
        data["date"] = pd.to_datetime(data["trade_date"], format=const.DATE_FORMAT)
    except ValueError as e:
        raise ValueError(f"日期转换失败: {str(e)}")
    data = data.sort_values(by="date").reset_index(drop=True) #按日期升序排列（确保均线计算顺序正确）

    match indicators:
        case "ma":  # 计算多日均线 5 6 10 20 60
            return ma_formula(data)
        case "bbi":  # 多空指数 3 6 12 20/3 6 12 24
            return bbi_formula(data)
        case "macd":  # 指数平滑异同移动平均线
            return macd_formula(data)
        # case "sar":  #
        #     return sar_formula(data)
        # case "kdj":  #
        #     return kdj_formula(data)
        case "rsi":  #
            return rsi_formula(data)
        # case "":  #
        #     return brar_formula(data)
        # case "":  #
        #     return cci_formula(data)
        # case "ma":  #
        #     return ma_formula(data)
        # case "":  #
        #     return emv_formula(data)
        # case "":  #
        #     return kdj_formula(data)
        # case "":  # 1
        #     return macd_formula(data)
        case _:
            return data

def ma_formula(data: pd.DataFrame) -> pd.DataFrame:
    def calc_ma_per_stock(group):
        # 计算各周期均线，日期不足时结果为0
        for i, period in enumerate(const.MA_PERIODS):
            group[f"ma{period}"] = group["close"].rolling(
                window=period,
                min_periods=period
            ).mean()
            group[f"ma{period}"] = group[f"ma{period}"].fillna(0)
        return group
    result = data.groupby('ts_code', group_keys=False).apply(
        calc_ma_per_stock,
        include_groups=False
    )
    for i, period in enumerate(const.MA_PERIODS):
        result[f"ma{period}"] = result[f"ma{period}"].round(5)
    return result

def bbi_formula(data: pd.DataFrame) -> pd.DataFrame:
    def calculate_bbi_per_stock(group):
        for i, period in enumerate(const.BBI_PERIODS):
            group[f"MA{i}"] = group["close"].rolling(
                window=period,
                min_periods=period
            ).mean()
        valid_mask20 = group[["MA0", "MA1", "MA2", "MA3"]].notna().all(axis=1)
        valid_mask24 = group[["MA0", "MA1", "MA2", "MA4"]].notna().all(axis=1)
        group["bbi20"] = 0.0
        group["bbi24"] = 0.0
        group.loc[valid_mask20, "bbi20"] = group.loc[valid_mask20, ["MA0", "MA1", "MA2", "MA3"]].sum(axis=1) / 4.0
        group.loc[valid_mask24, "bbi24"] = group.loc[valid_mask24, ["MA0", "MA1", "MA2", "MA4"]].sum(axis=1) / 4.0
        group = group.drop(columns=["MA0", "MA1", "MA2", "MA3", "MA4"])
        return group

    df_result = data.groupby("ts_code", group_keys=False).apply(calculate_bbi_per_stock, include_groups=False)
    df_result["bbi20"] = df_result["bbi20"].round(5)  # 可选：保留2位小数，便于阅读
    df_result["bbi24"] = df_result["bbi24"].round(5)  # 可选：保留2位小数，便于阅读

    return df_result

def macd_formula(data: pd.DataFrame) -> pd.DataFrame:
    fast_period = const.MACD_PERIODS[0]
    slow_period = const.MACD_PERIODS[1]
    signal_period = const.MACD_PERIODS[2]
    def calc_macd_per_stock(group):
        # 按日期升序排列（确保时间顺序正确）
        group = group.sort_values('trade_date').reset_index(drop=True)
        n = len(group)

        # 1. 计算EMA12（快速指数移动平均线）
        # 初始值使用前fast_period天的简单平均值
        group['macd_ema12'] = np.nan
        if n >= fast_period:
            # 初始EMA值
            group.loc[fast_period - 1, 'macd_ema12'] = group['close'].iloc[:fast_period].mean()
            # 后续EMA值：EMA = 当日收盘价×2/(N+1) + 前一日EMA×(N-1)/(N+1)
            for i in range(fast_period, n):
                group.loc[i, 'macd_ema12'] = (group.loc[i, 'close'] * 2 / (fast_period + 1) +
                                              group.loc[i - 1, 'macd_ema12'] * (fast_period - 1) / (fast_period + 1))

        # 2. 计算EMA26（慢速指数移动平均线）
        group['macd_ema26'] = np.nan
        if n >= slow_period:
            # 初始EMA值
            group.loc[slow_period - 1, 'macd_ema26'] = group['close'].iloc[:slow_period].mean()
            # 后续EMA值
            for i in range(slow_period, n):
                group.loc[i, 'macd_ema26'] = (group.loc[i, 'close'] * 2 / (slow_period + 1) +
                                              group.loc[i - 1, 'macd_ema26'] * (slow_period - 1) / (slow_period + 1))

        # 3. 计算MACD线（差离值 = EMA12 - EMA26）
        group['macd_line'] = group['macd_ema12'] - group['macd_ema26']

        # 4. 计算信号线DEA（MACD线的signal_period日EMA）
        group['macd_dea'] = np.nan
        # 找到第一个有效的MACD值位置
        first_valid_macd = group['macd_line'].first_valid_index()
        if first_valid_macd is not None and (n - first_valid_macd) >= signal_period:
            # 初始DEA值
            start_idx = first_valid_macd + signal_period - 1
            group.loc[start_idx, 'macd_dea'] = group['macd_line'].iloc[first_valid_macd:start_idx + 1].mean()
            # 后续DEA值
            for i in range(start_idx + 1, n):
                group.loc[i, 'macd_dea'] = (group.loc[i, 'macd_line'] * 2 / (signal_period + 1) +
                                            group.loc[i - 1, 'macd_dea'] * (signal_period - 1) / (signal_period + 1))

        # 5. 计算MACD柱状图（BAR = (MACD线 - DEA) × 2）
        group['macd_bar'] = (group['macd_line'] - group['macd_dea']) * 2

        # 处理周期不足的情况（填充为0）
        group[['macd_ema12', 'macd_ema26', 'macd_line', 'macd_dea', 'macd_bar']] = \
            group[['macd_ema12', 'macd_ema26', 'macd_line', 'macd_dea', 'macd_bar']].fillna(0)

        return group

    # 分组计算并消除警告
    result = data.groupby('ts_code', group_keys=False).apply(
        calc_macd_per_stock,
        include_groups=False
    )

    result = result.drop(columns=["macd_ema12", "macd_ema26"])
    return result

def sar_formula(data: pd.DataFrame) -> pd.DataFrame:
    return data

def kdj_formula(data: pd.DataFrame) -> pd.DataFrame:
    return data

def rsi_formula(data: pd.DataFrame) -> pd.DataFrame:
    df = data.copy()
    for i, period in enumerate(const.RSI_PERIODS):
        price_col = 'close'
        df[f"delta{period}"] = df[price_col].diff(1)  # 当日 - 前一日

        # 2. 区分上涨和下跌幅度
        df[f"gain{period}"] = np.where(df[f"delta{period}"] > 0, df[f"delta{period}"], 0)  # 上涨幅度
        df[f"loss{period}"] = np.where(df[f"delta{period}"] < 0, -df[f"delta{period}"], 0)  # 下跌幅度（取绝对值）

        # 3. 计算初始N天的平均增益和平均损失
        # 前N天的简单平均值
        initial_avg_gain = df[f"gain{period}"].iloc[1:period + 1].mean()  # 第1到第N天的gain平均
        initial_avg_loss = df[f"loss{period}"].iloc[1:period + 1].mean()  # 第1到第N天的loss平均

        # 初始化RSI列
        df[f"rsi{period}"] = np.nan

        # 计算第N天的RSI值（索引为period）
        if initial_avg_loss == 0:
            df.loc[period, f"rsi{period}"] = 100.0
        else:
            rs = initial_avg_gain / initial_avg_loss
            df.loc[period, f"rsi{period}"] = 100 - (100 / (1 + rs))

        # 4. 计算后续日期的RSI（从第N+1天开始）
        for i in range(period + 1, len(df)):
            # 平滑计算平均增益和损失
            current_avg_gain = (initial_avg_gain * (period - 1) + df[f"gain{period}"].iloc[i]) / period
            current_avg_loss = (initial_avg_loss * (period - 1) + df[f"loss{period}"].iloc[i]) / period

            # 计算RSI
            if current_avg_loss == 0:
                df.loc[i, f"rsi{period}"] = 100.0
            else:
                rs = current_avg_gain / current_avg_loss
                df.loc[i, f"rsi{period}"] = 100 - (100 / (1 + rs))

            # 更新平均值用于下一次计算
            initial_avg_gain = current_avg_gain
            initial_avg_loss = current_avg_loss
        df = df.drop(columns=[f"gain{period}", f"loss{period}", f"delta{period}"])

    return df

# def cci_formula(data: pd.DataFrame) -> pd.DataFrame:
#     return data
#
#
# def ma_formula(data: pd.DataFrame) -> pd.DataFrame:
#     return data
#
#
# def emv_formula(data: pd.DataFrame) -> pd.DataFrame:
#     return data
#
#
# def kdj_formula(data: pd.DataFrame) -> pd.DataFrame:
#     return data
#
#
# def macd_formula(data: pd.DataFrame) -> pd.DataFrame:
#     return data

#
# if __name__ == "__main__":
#     pro = ts.pro_api('j80872d274089319b71f9e5ca3966abf009')
#
#     daily_coll = mongoDb_ctl.init_mongo_collection(const.MONGO_DAILY_COLL)
#     cursor = daily_coll.find({},
#                              {"_id": 0, "up_time": 0, "vol": 0, "amount": 0, "vol_ratio": 0, "turn_over": 0, "swing": 0,
#                               "selling": 0, "buying": 0, "change": 0, "pct_change": 0})
#     data = pd.DataFrame(list(cursor))
#     # print(data.to_string())
#     df = formula_main("rsi", data)
#     print(df.to_string())

