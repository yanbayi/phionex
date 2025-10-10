from stratege.util import tdx_stratege_ctl
from stratege.util import stock_stratege_ctl

def daily_stock_main_logic(param):
    is_use_tdx_conditions = param.get('is_use_tdx_conditions', True)
    date_str = param.get('date_str', '20250918')
    is_need_backtest = param.get('is_need_backtest', False)
    backtest_day = param.get('backtest_day', 10)

    tdx_conditions = param.get('tdx_conditions', [])
    if len(tdx_conditions) == 0:
        raise ValueError(f"概念策略配置缺失，检查配置是否正确: {tdx_conditions}")

    stock_conditions = param.get('stock_conditions', [])
    if len(stock_conditions) == 0:
        raise ValueError(f"股票策略配置缺失，检查配置是否正确: {stock_conditions}")

    stock_logic_expr = param.get('stock_logic_expr', '')
    if stock_logic_expr == "":
        raise ValueError(f"股票策略逻辑配置缺失，检查配置是否正确: {stock_logic_expr}")

    # 筛选所有概念板块
    get_tdx_list = []
    if is_use_tdx_conditions:
        print("=" * 30, "开始筛选概念、板块")
        tdx_filter = tdx_stratege_ctl.TdxFilter()
        get_tdx_list = tdx_filter.filter_stocks(date_str, tdx_conditions)
        print("=" * 30, "完成筛选概念、板块", "\n" * 2)
    # 基于板块筛选股票
    print("=" * 30, "开始筛选股票")
    stock_filter = stock_stratege_ctl.StockFilter(get_tdx_list, date_str, is_use_tdx_conditions)
    get_stock_list = stock_filter.filter_stocks(stock_conditions, stock_logic_expr)
    print("=" * 30, "完成筛选股票", "\n" * 2)
    # 回测数据
    if is_need_backtest:
        print("=" * 30, "开始回测")
        stock_filter.backtest_stocks(get_stock_list, backtest_day)
        print("=" * 30, "完成回测")