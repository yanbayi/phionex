import time
import pandas as pd
from common import const

def formula_main(data: pd.DataFrame) -> pd.DataFrame:
    required_columns = ['ts_code', 'trade_date', 'date', 'open_q', 'high_q', 'low_q', 'close_q']
    if not set(required_columns).issubset(data.columns):
        missing = [col for col in required_columns if col not in data.columns]
        raise ValueError(f"数据缺少必要的列: {missing}")

    def formula_all(group):

        ########################## 计算各周期均线，日期不足时结果为0 #########################
        # time_ma = time.time()

        for i, period in enumerate(const.MA_PERIODS):
            group[f"ma{period}"] = group["close_q"].rolling(
                window=period,
                min_periods=period
            ).mean()
            group[f"ma{period}"] = group[f"ma{period}"].fillna(0).round(5)


        # time_ma_end = time.time()
        # print(f"ma耗时: {time_ma_end - time_ma:.6f} 秒")
        ########################## 计算bbi                     #########################
        # time_bbi = time.time()

        for i, period in enumerate(const.BBI_PERIODS):
            group[f"MA{i}"] = group["close_q"].rolling(
                window=period,
                min_periods=period
            ).mean()
        valid_mask20 = group[["MA0", "MA1", "MA2", "MA3"]].notna().all(axis=1)
        group["bbi20"] = 0.0
        group.loc[valid_mask20, "bbi20"] = group.loc[valid_mask20, ["MA0", "MA1", "MA2", "MA3"]].sum(axis=1) / 4.0
        for col in ['bbi20']:
            group[col] = group[col].round(5)
            group[col] = group[col].where(group[col] != -0.0, 0.0)
        group = group.drop(columns=["MA0", "MA1", "MA2", "MA3"])


        # time_bbi_end = time.time()
        # print(f"bbi耗时: {time_bbi_end - time_bbi:.6f} 秒")
        # ########################## 计算macd1                   #########################
        # time_macd1 = time.time()

        fast_period1 = const.MACD1_PERIODS[0]
        slow_period1 = const.MACD1_PERIODS[1]
        signal_period1 = const.MACD1_PERIODS[2]
        group['ema121'] = group['close_q'].ewm(span=fast_period1,min_periods=1,adjust=False).mean()
        group['ema261'] = group['close_q'].ewm(span=slow_period1,min_periods=1,adjust=False).mean()
        group['macd_dif1'] = group['ema121'] - group['ema261']
        group['macd_dea1'] = group['macd_dif1'].ewm(span=signal_period1,min_periods=1,adjust=False).mean()
        group['macd1'] = (group['macd_dif1'] - group['macd_dea1']) * 2
        valid_mask = group[['macd_dif1', 'macd_dea1', 'macd1']].notna().all(axis=1)
        group['macd_dif1'] = group['macd_dif1'].where(valid_mask, 0)
        group['macd_dea1'] = group['macd_dea1'].where(valid_mask, 0)
        group['macd1'] = group['macd1'].where(valid_mask, 0)
        for col in ['macd_dif1', 'macd_dea1', 'macd1']:
            group[col] = group[col].round(5)
            group[col] = group[col].where(group[col] != -0.0, 0.0)
        group = group.drop(columns=['ema121', 'ema261'])


        # time_macd1_end = time.time()
        # print(f"macd1耗时: {time_macd1_end - time_macd1:.6f} 秒")
        ########################## 计算macd2                   #########################
        # time_macd2 = time.time()

        fast_period2 = const.MACD2_PERIODS[0]
        slow_period2 = const.MACD2_PERIODS[1]
        signal_period2 = const.MACD2_PERIODS[2]
        group['ema122'] = group['close_q'].ewm(span=fast_period2,min_periods=1,adjust=False).mean()
        group['ema262'] = group['close_q'].ewm(span=slow_period2,min_periods=1,adjust=False).mean()
        group['macd_dif2'] = group['ema122'] - group['ema262']
        group['macd_dea2'] = group['macd_dif2'].ewm(span=signal_period2,min_periods=1,adjust=False).mean()
        group['macd2'] = (group['macd_dif2'] - group['macd_dea2']) * 2
        valid_mask = group[['macd_dif2', 'macd_dea2', 'macd2']].notna().all(axis=1)
        group['macd_dif2'] = group['macd_dif2'].where(valid_mask, 0)
        group['macd_dea2'] = group['macd_dea2'].where(valid_mask, 0)
        group['macd2'] = group['macd2'].where(valid_mask, 0)
        for col in ['macd_dif2', 'macd_dea2', 'macd2']:
            group[col] = group[col].round(5)
            group[col] = group[col].where(group[col] != -0.0, 0.0)
        group = group.drop(columns=['ema122', 'ema262'])


        # time_macd2_end = time.time()
        # print(f"macd2耗时: {time_macd2_end - time_macd2:.6f} 秒")
        ########################## 计算kdj                    #########################
        # time_kdj = time.time()

        n, m1, m2 = const.KDJ_PERIODS
        group['llv_low'] = group['low_q'].rolling(window=n, min_periods=n).min()
        group['hhv_high'] = group['high_q'].rolling(window=n, min_periods=n).max()
        valid_mask = (group['hhv_high'] - group['llv_low']) != 0
        group['rsv'] = 0.0
        group.loc[valid_mask, 'rsv'] = (
                (group.loc[valid_mask, 'close_q'] - group.loc[valid_mask, 'llv_low']) /
                (group.loc[valid_mask, 'hhv_high'] - group.loc[valid_mask, 'llv_low']) * 100
        )
        group['kdj_k_qfq'] = 0.0
        group['kdj_d_qfq'] = 0.0
        k_vals = group['kdj_k_qfq'].values
        d_vals = group['kdj_d_qfq'].values
        rsv_vals = group['rsv'].values
        group_len = len(group)
        if group_len >= n:
            k_vals[n - 1] = rsv_vals[n - 1]
            d_vals[n - 1] = k_vals[n - 1]
            for i in range(n, group_len):
                k_vals[i] = (k_vals[i - 1] * (m1 - 1) + rsv_vals[i]) / m1
                d_vals[i] = (d_vals[i - 1] * (m2 - 1) + k_vals[i]) / m2
        group['kdj_k_qfq'] = k_vals
        group['kdj_d_qfq'] = d_vals
        group['kdj_j_qfq'] = 3 * group['kdj_k_qfq'] - 2 * group['kdj_d_qfq']
        for col in ['kdj_k_qfq', 'kdj_d_qfq', 'kdj_j_qfq']:
            group[col] = group[col].round(5)
            group[col] = group[col].where(group[col] != -0.0, 0.0)
        group = group.drop(columns=['llv_low', 'hhv_high', 'rsv'])


        # time_kdj_end = time.time()
        # print(f"kdj耗时: {time_kdj_end - time_kdj:.6f} 秒")
        ########################## 计算boll                   #########################
        # time_boll = time.time()

        n_period = const.BOLL_PERIODS[0]  # BOLL周期
        m_factor = const.BOLL_PERIODS[1]  # 标准差倍数
        group['boll_mid'] = group['close_q'].rolling(window=n_period, min_periods=n_period).mean()
        group['close_std'] = group['close_q'].rolling(window=n_period, min_periods=n_period).std(ddof=0)
        group['boll_upper'] = group['boll_mid'] + m_factor * group['close_std']
        group['boll_lower'] = group['boll_mid'] - m_factor * group['close_std']
        for col in ['boll_mid', 'boll_upper', 'boll_lower']:
            group[col] = group[col].round(5)
            group[col] = group[col].where(group[col] != -0.0, 0.0)
        group[['boll_mid', 'boll_upper', 'boll_lower']] = group[['boll_mid', 'boll_upper', 'boll_lower']].fillna(0)
        group = group.drop(columns=['close_std'])


        # time_boll_end = time.time()
        # print(f"boll耗时: {time_boll_end - time_boll:.6f} 秒")
        return group

    result = data.groupby("ts_code").apply(formula_all,include_groups=False)
    result = result.reset_index(level='ts_code')
    return result