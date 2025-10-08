from util import stock_stratege_ctl, tdx_stratege_ctl

############################################ 挑选日期 ################################################
date_str = "20250918"  # 预测哪一天的股票
is_need_backtest = False  # 是否要回测
backtest_day = 10  # 回测跑的天数（自然日）
########################################## 概念指标条件 ###############################################
# 板块：日线挑选5天涨幅最高，至少有3天在涨，并且成交额大的20个概念板块
tdx_conditions = [
    # 板块、概念名称带“半导体”的所有板块概念，实例：["半导体"]   ["半导体", "机器人", "矿"]
    {"type": "1", "name": "名称包含", "enable": False, "params": {"name": ["半导体", "机器人", "有色"]}},
    # 最近day1天中有day2天在涨，且其中有day3天涨幅超过n%的top m
    {"type": "2", "name": "涨幅条件1", "enable": True, "params": {"day1": 5, "day2": 3, "day3": 1, "n": 1, "m": 20}},
    # 最近day1天中涨幅top n
    {"type": "3", "name": "涨幅条件2", "enable": False, "params": {"day1": 5, "n": 50}},
    # 挑选day1天内成交量均值的top n
    {"type": "4", "name": "成交量条件1", "enable": False, "params": {"day1": 5, "n": 30}},
    # 挑选day1天内成交额均值的top n
    {"type": "5", "name": "成交额条件1", "enable": True, "params": {"day1": 5, "n": 20}},
    # 最近day1天量比均大于n
    {"type": "6", "name": "量比条件1", "enable": False, "params": {"day1": 3, "n": 1.1}}
]

########################################### 股票条件 ################################################
# 股票:筛选5天内有过bbi上穿，15天内有过1天超过8%涨幅，成交量在5天内放大1.2倍以上，10天涨幅超过10%
stock_conditions = [
    # 最近day1天内有过bbi上穿
    {"type": "1", "name": "BBI上穿", "enable": True, "params": {"day1": 5}},
    # 最近day1天中有day2天在涨，且其中有day3天涨幅超过n%
    {"type": "2", "name": "涨幅条件1", "enable": False, "params": {"day1": 15, "day2": 5, "day3": 1, "n": 8}},
    # 最近day1天涨幅超过n%
    {"type": "3", "name": "涨幅条件2", "enable": False, "params": {"day1": 10, "n": 10}},
    # 最近day1天内存在某一天的成交量达到成交量均值的n倍以上
    {"type": "4", "name": "成交量条件1", "enable": False, "params": {"day1": 5, "n": 1.3}},
    # 最近day1天成交量均值高于最近(day1天前的)day2天成交量均值n倍以上
    {"type": "5", "name": "成交量条件2", "enable": False, "params": {"day1": 3, "day2": 10, "n": 1.5}},
    # 最近day1天量比均大于n
    {"type": "6", "name": "量比条件1", "enable": False, "params": {"day1": 3, "n": 1.5}},
    # 最近day1天内，ma10上穿ma20 n 次以上
    {"type": "7", "name": "均线条件1", "enable": True, "params": {"day1": 60, "n": 2}},
    # 最近day1天涨幅不超过n%
    {"type": "8", "name": "涨幅条件3", "enable": False, "params": {"day1": 10, "n": 1}},
]
stock_logic_expr = "1 & 7"

if __name__ == "__main__":
    # 筛选所有概念板块
    print("=" * 30, "开始筛选概念、板块")
    tdx_filter = tdx_stratege_ctl.TdxFilter()
    get_tdx_list = tdx_filter.filter_stocks(date_str, tdx_conditions)
    print("=" * 30, "完成筛选概念、板块", "\n" * 2)
    # 基于板块筛选股票
    print("=" * 30, "开始筛选股票")
    stock_filter = stock_stratege_ctl.StockFilter(get_tdx_list, date_str)
    get_stock_list = stock_filter.filter_stocks(stock_conditions, stock_logic_expr)
    print("=" * 30, "完成筛选股票", "\n" * 2)
    # 回测数据
    if is_need_backtest:
        print("=" * 30, "开始回测")
        stock_filter.backtest_stocks(get_stock_list, backtest_day)
        print("=" * 30, "完成回测")
