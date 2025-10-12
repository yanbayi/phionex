import pandas as pd
from common import const

def formula_main(indicators: str, data: pd.DataFrame) -> pd.DataFrame:
    required_columns = ['ts_code', 'trade_date', 'date', 'open_q', 'high_q', 'low_q', 'close_q']
    if not set(required_columns).issubset(data.columns):
        missing = [col for col in required_columns if col not in data.columns]
        raise ValueError(f"数据缺少必要的列: {missing}")
    match indicators:
        case "ma":  # 计算多日均线
            return ma_formula(data)
        case "bbi":  # 多空指数 3 6 12 20/3 6 12 24
            return bbi_formula(data)
        case "macd1":  # 指数平滑异同移动平均线12 26 9
            return macd1_formula(data)
        case "macd2":  # 指数平滑异同移动平均线60 130 45
            return macd2_formula(data)
        case "kdj": # 9 3 3
            return kdj_formula(data)
        case "boll":
            return kdj_formula(data)
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
        # valid_mask24 = group[["MA0", "MA1", "MA2", "MA4"]].notna().all(axis=1)
        group["bbi20"] = 0.0
        # group["bbi24"] = 0.0
        group.loc[valid_mask20, "bbi20"] = group.loc[valid_mask20, ["MA0", "MA1", "MA2", "MA3"]].sum(axis=1) / 4.0
        # group.loc[valid_mask24, "bbi24"] = group.loc[valid_mask24, ["MA0", "MA1", "MA2", "MA4"]].sum(axis=1) / 4.0
        group = group.drop(columns=["MA0", "MA1", "MA2", "MA3"])
        return group

    df_result = data.groupby("ts_code", group_keys=False).apply(calculate_bbi_per_stock, include_groups=False)
    df_result["bbi20"] = df_result["bbi20"].round(5)  # 可选：保留2位小数，便于阅读
    # df_result["bbi24"] = df_result["bbi24"].round(5)  # 可选：保留2位小数，便于阅读

    return df_result

def macd1_formula(data: pd.DataFrame) -> pd.DataFrame:
    fast_period = const.MACD1_PERIODS[0]
    slow_period = const.MACD1_PERIODS[1]
    signal_period = const.MACD1_PERIODS[2]
    def calculate_macd_per_stock(group):
        # 计算12日和26日指数移动平均线（EMA）
        group['ema12'] = group['close'].ewm(span=fast_period, min_periods=fast_period).mean()
        group['ema26'] = group['close'].ewm(span=slow_period, min_periods=slow_period).mean()
        # 计算DIF（离差值）= EMA12 - EMA26
        group['macd_dif'] = group['ema12'] - group['ema26']
        # 计算DEA（信号线）= DIF的9日指数移动平均线
        group['macd_dea'] = group['macd_dif'].ewm(span=signal_period, min_periods=signal_period).mean()
        # 计算MACD柱状线 = (DIF - DEA) * 2
        group['macd'] = (group['macd_dif'] - group['macd_dea']) * 2
        # 初始化无效值为0（数据不足时）
        valid_mask = group[['macd_dif', 'macd_dea', 'macd']].notna().all(axis=1)
        group['macd_dif'] = group['macd_dif'].where(valid_mask, 0)
        group['macd_dea'] = group['macd_dea'].where(valid_mask, 0)
        group['macd'] = group['macd'].where(valid_mask, 0)
        # 删除临时计算的EMA列
        group = group.drop(columns=['ema12', 'ema26'])
        return group
    # 按股票代码分组计算MACD，确保每只股票独立计算
    df_result = data.groupby('ts_code', group_keys=False).apply(
        calculate_macd_per_stock,
        include_groups=False
    )
    # 保留小数位数，避免精度问题
    df_result['macd_dif'] = df_result['macd_dif'].round(5)
    df_result['macd_dea'] = df_result['macd_dea'].round(5)
    df_result['macd'] = df_result['macd'].round(5)
    return df_result

def macd2_formula(data: pd.DataFrame) -> pd.DataFrame:
    fast_period = const.MACD2_PERIODS[0]
    slow_period = const.MACD2_PERIODS[1]
    signal_period = const.MACD2_PERIODS[2]

    def calculate_macd_per_stock(group):
        # 计算12日和26日指数移动平均线（EMA）
        group['ema12'] = group['close'].ewm(span=fast_period, min_periods=fast_period).mean()
        group['ema26'] = group['close'].ewm(span=slow_period, min_periods=slow_period).mean()
        # 计算DIF（离差值）= EMA12 - EMA26
        group['macd_dif'] = group['ema12'] - group['ema26']
        # 计算DEA（信号线）= DIF的9日指数移动平均线
        group['macd_dea'] = group['macd_dif'].ewm(span=signal_period, min_periods=signal_period).mean()
        # 计算MACD柱状线 = (DIF - DEA) * 2
        group['macd'] = (group['macd_dif'] - group['macd_dea']) * 2
        # 初始化无效值为0（数据不足时）
        valid_mask = group[['macd_dif', 'macd_dea', 'macd']].notna().all(axis=1)
        group['macd_dif'] = group['macd_dif'].where(valid_mask, 0)
        group['macd_dea'] = group['macd_dea'].where(valid_mask, 0)
        group['macd'] = group['macd'].where(valid_mask, 0)
        # 删除临时计算的EMA列
        group = group.drop(columns=['ema12', 'ema26'])
        return group

    # 按股票代码分组计算MACD，确保每只股票独立计算
    df_result = data.groupby('ts_code', group_keys=False).apply(
        calculate_macd_per_stock,
        include_groups=False
    )
    # 保留小数位数，避免精度问题
    df_result['macd_dif'] = df_result['macd_dif'].round(5)
    df_result['macd_dea'] = df_result['macd_dea'].round(5)
    df_result['macd'] = df_result['macd'].round(5)
    return df_result

def kdj_formula(data: pd.DataFrame) -> pd.DataFrame:
    def calculate_kdj_per_stock(group):
        # 计算9日最低价和最高价
        group['low9'] = group['low'].rolling(window=9, min_periods=9).min()
        group['high9'] = group['high'].rolling(window=9, min_periods=9).max()

        # 计算RSV（未成熟随机值）
        group['rsv'] = (group['close'] - group['low9']) / (group['high9'] - group['low9']) * 100

        # 计算K值（初始K值设为50，后续为前一日K值*2/3 + 当日RSV*1/3）
        group['kdj_k'] = 50.0  # 初始值
        for i in range(1, len(group)):
            if pd.notna(group.loc[group.index[i], 'rsv']) and pd.notna(group.loc[group.index[i - 1], 'kdj_k']):
                group.loc[group.index[i], 'kdj_k'] = group.loc[group.index[i - 1], 'kdj_k'] * 2 / 3 + group.loc[
                    group.index[i], 'rsv'] * 1 / 3

        # 计算D值（初始D值设为50，后续为前一日D值*2/3 + 当日K值*1/3）
        group['kdj_d'] = 50.0  # 初始值
        for i in range(1, len(group)):
            if pd.notna(group.loc[group.index[i], 'kdj_k']) and pd.notna(group.loc[group.index[i - 1], 'kdj_d']):
                group.loc[group.index[i], 'kdj_d'] = group.loc[group.index[i - 1], 'kdj_d'] * 2 / 3 + group.loc[
                    group.index[i], 'kdj_k'] * 1 / 3

        # 计算J值 = 3*K - 2*D
        group['kdj_j'] = 3 * group['kdj_k'] - 2 * group['kdj_d']

        # 处理无效值（数据不足时设为0）
        valid_mask = group[['kdj_k', 'kdj_d', 'kdj_j']].notna().all(axis=1)
        group['kdj_k'] = group['kdj_k'].where(valid_mask, 0)
        group['kdj_d'] = group['kdj_d'].where(valid_mask, 0)
        group['kdj_j'] = group['kdj_j'].where(valid_mask, 0)

        # 删除临时计算列
        group = group.drop(columns=['low9', 'high9', 'rsv'])

        return group

    # 按股票代码分组计算KDJ，确保每只股票独立计算
    df_result = data.groupby('ts_code', group_keys=False).apply(
        calculate_kdj_per_stock,
        include_groups=False
    )

    # 保留小数位数，避免精度问题
    df_result['kdj_k'] = df_result['kdj_k'].round(5)
    df_result['kdj_d'] = df_result['kdj_d'].round(5)
    df_result['kdj_j'] = df_result['kdj_j'].round(5)

    return df_result

def boll_formula(data: pd.DataFrame) -> pd.DataFrame:
    def calculate_boll_per_stock(group):
        # 计算中间轨（N日收盘价的简单移动平均线）
        # 通常N取20，可通过const配置，这里默认使用20
        n_period = getattr(const, 'BOLL_PERIOD', 20)
        group['boll_mid'] = group['close'].rolling(window=n_period, min_periods=n_period).mean()
        # 计算收盘价与中间轨的标准差（衡量波动幅度）
        group['close_std'] = group['close'].rolling(window=n_period, min_periods=n_period).std()
        # 计算上轨和下轨（中间轨 ± 2倍标准差，2是常见参数）
        k = getattr(const, 'BOLL_K', 2)
        group['boll_up'] = group['boll_mid'] + k * group['close_std']
        group['boll_low'] = group['boll_mid'] - k * group['close_std']
        # 处理无效值（数据不足时设为0）
        valid_mask = group[['boll_mid', 'boll_up', 'boll_low']].notna().all(axis=1)
        group['boll_mid'] = group['boll_mid'].where(valid_mask, 0)
        group['boll_up'] = group['boll_up'].where(valid_mask, 0)
        group['boll_low'] = group['boll_low'].where(valid_mask, 0)
        # 删除临时计算列
        group = group.drop(columns=['close_std'])
        return group
        # 按股票代码分组计算BOLL，确保每只股票独立计算
    df_result = data.groupby('ts_code', group_keys=False).apply(
        calculate_boll_per_stock,
        include_groups=False
    )
    # 保留小数位数，避免精度问题
    df_result['boll_mid'] = df_result['boll_mid'].round(5)
    df_result['boll_up'] = df_result['boll_up'].round(5)
    df_result['boll_low'] = df_result['boll_low'].round(5)

    return df_result