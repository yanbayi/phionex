from util import stock_stratege_ctl, tdx_stratege_ctl

# 板块：日线挑选3-5天涨幅最高，至少有3天在涨，并且成交额大的20个概念板块
# 股票:筛选5天内有过bbi上穿，15天内有过1天超过8%涨幅，成交量在5天内放大1.2倍以上，10天涨幅超过10%
########################################## 概念指标条件 ###############################################
tdx_conditions = [
    # 板块、概念名称带“半导体”的所有板块概念，实例：["半导体"]   ["半导体", "机器人", "矿"]
    {"type": 1, "name": "名称包含", "enable": False, "params": {"name": ["半导体", "机器人"]}},
    #
    {"type": 2, "name": "成交量放大", "enable": True, "params": {"days1": 1, "days2": 2, "multiple": 1.5}},
    {"type": 3, "name": "BBI上穿", "enable": True, "params": {"enabled": True}}
]
tdx_logic_expr = "1 AND (2 OR 3)"

########################################### 股票条件 #################################################
# stock_conditions = [
#     {"type": "涨停天数", "enable": True, "params": {"days": 2}},
#     {"type": "成交量放大", "enable": True, "params": {"days1": 1, "days2": 2, "multiple": 1.5}},
#     {"type": "BBI上穿", "enable": True, "params": {"enabled": True}}
# ]
# stock_logic_expr = "1 AND (2 OR 3)"


if __name__ == "__main__":
    # 日期
    date_str = "20251001"
    # 判断每一步中是否包含以下股票,空就是不判断，举例 ["000583.SZ"]  ["000583.SZ", "000001,SZ"]
    has_ts_code = []

    # 筛选所有概念板块
    tdx_filter = tdx_stratege_ctl.TdxFilter()
    result_df = tdx_filter.filter_stocks(tdx_conditions, tdx_logic_expr)

    # 基于板块筛选股票


    # 回测数据
