import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
from common import utils, const, formula , push
from data_ctl import tushare_ctl
from db_ctl.util import mongoDb_ctl

tdx_index_coll = mongoDb_ctl.init_mongo_collection(const.TDX_INDEX)
tdx_member_coll = mongoDb_ctl.init_mongo_collection(const.TDX_MEMBER)
tdx_daily_coll = mongoDb_ctl.init_mongo_collection(const.TDX_DAILY)
basic_coll = mongoDb_ctl.init_mongo_collection(const.MONGO_BASIC_COLL)
daily_coll = mongoDb_ctl.init_mongo_collection(const.MONGO_DAILY_COLL)

# ========================== 核心参数配置区（可根据需求调整）==========================
CONFIG = {
    # 概念板块筛选参数
    "concept_filter": {
        "recent_days": 5,  # 板块筛选参考的交易日天数
        "min_valid_days": 3,  # 板块至少需要的有效交易日数
        "min_up_days": 3,  # 至少上涨天数
        "top_n_concepts": 20  # 选取的top概念板块数量
    },

    # 股票筛选参数
    "stock_filter": {
        "recent_days": 15,  # 股票筛选参考的交易日天数
        "vol_growth_ratio": 1.2,  # 成交量放大倍数阈值
        "surge_threshold": 8,  # 单日涨幅阈值(%)
        "price_rise_threshold": 10  # 10天涨幅阈值(%)
    }
}




def get_database():
    """获取数据库实例"""
    client = MongoClient(
        const.MONGODB_HOST,
        serverSelectionTimeoutMS=5000,
        socketTimeoutMS=10000
    )
    db = client[const.DB_NAME]
    return db


def get_recent_trade_dates(collection, days=5):
    """获取最近N个交易日的日期列表（降序）"""
    dates = collection.distinct("trade_date")
    # 转换为日期格式并排序（假设日期格式为YYYYMMDD）
    dates = sorted([datetime.strptime(d, "%Y%m%d") for d in dates], reverse=True)
    return [d.strftime("%Y%m%d") for d in dates[:days]]


def get_concept_name(ts_code):
    """从a_tdx_index集合获取概念板块名称"""
    db = get_database()
    # 查询该板块代码对应的名称（取最新一条记录的name）
    concept_info = db.a_tdx_index.find_one(
        {"ts_code": ts_code},
        {"name": 1}
    )
    return concept_info.get("name", "未知板块") if concept_info else "未知板块"


def filter_top_concepts():
    db = get_database()
    # 从配置获取参数
    concept_cfg = CONFIG["concept_filter"]
    recent_days = concept_cfg["recent_days"]
    min_valid_days = concept_cfg["min_valid_days"]
    min_up_days = concept_cfg["min_up_days"]
    top_n = concept_cfg["top_n_concepts"]

    # 获取最近N个交易日
    recent_dates = get_recent_trade_dates(db.a_tdx_daily, days=recent_days)
    if len(recent_dates) < min_valid_days:
        print(f"交易日数据不足{min_valid_days}天，无法筛选板块")
        return []

    # 获取所有概念板块代码
    concept_codes = db.a_tdx_daily.distinct("ts_code")
    print(f"初始概念板块总数: {len(concept_codes)}")

    concept_results = []

    for code in concept_codes:
        # 查询该板块最近N天的日线数据
        daily_data = list(db.a_tdx_daily.find(
            {"ts_code": code, "trade_date": {"$in": recent_dates}},
            {"trade_date": 1, "pct_change": 1, "amount": 1}
        ).sort("trade_date", 1))  # 按日期升序

        # 过滤出有足够天数数据的板块
        if not (min_valid_days <= len(daily_data) <= recent_days):
            print(f"初始概念板块总数: {len(concept_codes)}")
            continue

        # 计算上涨天数
        up_days = sum(1 for d in daily_data if d.get("pct_change", 0) > 0)
        if up_days < min_up_days:
            continue

        # 计算累计涨幅和平均成交额
        total_pct = sum(d.get("pct_change", 0) for d in daily_data)
        avg_amount = sum(d.get("amount", 0) for d in daily_data) / len(daily_data)

        # 获取板块名称
        concept_name = get_concept_name(code)

        # 记录结果
        concept_results.append({
            "ts_code": code,
            "name": concept_name,
            "total_pct": round(total_pct, 2),
            "up_days": up_days,
            "avg_amount": round(avg_amount, 2)
        })

    print(f"经过数据完整性和上涨天数筛选后剩余板块数: {len(concept_results)}")
    # 按涨幅和成交额排序，取前N个
    concept_results.sort(key=lambda x: (-x["total_pct"], -x["avg_amount"]))
    print(f"取前{top_n}个板块后剩余: {len(concept_results[:top_n])}")
    return concept_results[:top_n]


def filter_target_stocks(concept_codes):
    """第二步：从选中板块筛选符合条件的股票"""
    if not concept_codes:
        return []
    db = get_database()
    stock_cfg = CONFIG["stock_filter"]
    recent_days = stock_cfg["recent_days"]
    vol_ratio = stock_cfg["vol_growth_ratio"]
    surge_thresh = stock_cfg["surge_threshold"]
    price_rise_thresh = stock_cfg["price_rise_threshold"]

    # 获取板块成分股（去重）
    member_stocks = db.a_tdx_member.distinct(
        "con_code",
        {"ts_code": {"$in": concept_codes}}
    )
    print(f"\n从选中板块获取的初始成分股总数: {len(member_stocks)}")
    if not member_stocks:
        return []

    # 获取最近N个交易日
    recent_dates = get_recent_trade_dates(db.a_share_daily, days=recent_days)
    if len(recent_dates) < recent_days:
        print(f"股票日线数据不足{recent_days}天，无法筛选股票")
        return []

    target_stocks = []
    # 1. 先筛选出数据完整的股票（N天数据）
    valid_data_stocks = []
    for stock_code in member_stocks:
        daily_data = list(db.a_share_daily.find(
            {"ts_code": stock_code, "trade_date": {"$in": recent_dates}},
            {"trade_date": 1, "close": 1, "pct_change": 1,
             "vol": 1, "bbi": 1}
        ).sort("trade_date", 1))  # 按日期升序

        if len(daily_data) == recent_days:  # 必须有完整N天数据
            valid_data_stocks.append((stock_code, daily_data))

    print(f"1. 经过{recent_days}天完整数据筛选后剩余股票数: {len(valid_data_stocks)}")
    if not valid_data_stocks:
        return []

    # 2. 筛选BBI上穿条件
    bbi_pass_stocks = []
    for stock_code, daily_data in valid_data_stocks:
        last5 = daily_data[-5:]  # 最近5天数据（BBI判断窗口固定为5天）
        has_bbi_cross = False
        for i in range(1, len(last5)):
            prev = last5[i - 1]
            curr = last5[i]
            prev_bbi = prev.get("bbi", 0)
            prev_close = prev.get("close", 0)
            curr_bbi = curr.get("bbi", 0)
            curr_close = curr.get("close", 0)

            # 前一天BBI < 收盘价，当天BBI > 收盘价（上穿）
            if prev_bbi < prev_close and curr_bbi > curr_close:
                has_bbi_cross = True
                break
        if has_bbi_cross:
            bbi_pass_stocks.append((stock_code, daily_data))

    print(f"2. 经过BBI上穿条件筛选后剩余股票数: {len(bbi_pass_stocks)}")
    if not bbi_pass_stocks:
        return []

    # 3. 筛选N天内有1天涨幅超阈值
    surge_pass_stocks = []
    for stock_code, daily_data in bbi_pass_stocks:
        pct_list = [d.get("pct_change", 0) for d in daily_data]
        if any(pct > surge_thresh for pct in pct_list):
            surge_pass_stocks.append((stock_code, daily_data))

    print(f"3. 经过{recent_days}天内涨幅超{surge_thresh}%条件筛选后剩余股票数: {len(surge_pass_stocks)}")
    if not surge_pass_stocks:
        return []

    # 4. 筛选5天内成交量放大阈值以上
    vol_pass_stocks = []
    for stock_code, daily_data in surge_pass_stocks:
        last5_vol = [d.get("vol", 0) for d in daily_data[-5:]]
        if last5_vol[0] > 0 and (last5_vol[-1] > last5_vol[0] * vol_ratio or last5_vol[-1] > last5_vol[1] * vol_ratio or last5_vol[-1] > last5_vol[2] * vol_ratio or last5_vol[-1] > last5_vol[3] * vol_ratio):
            vol_pass_stocks.append((stock_code, daily_data))

    print(f"4. 经过5天内成交量放大{vol_ratio}倍条件筛选后剩余股票数: {len(vol_pass_stocks)}")
    if not vol_pass_stocks:
        return []

    # 5. 筛选10天涨幅超过阈值
    final_stocks = []
    for stock_code, daily_data in vol_pass_stocks:
        last10 = daily_data[-10:]
        start_close = last10[0].get("close", 0)
        end_close = last10[-1].get("close", 0)
        if start_close > 0 and (end_close - start_close) / start_close * 100 > price_rise_thresh:
            final_stocks.append(stock_code)

    print(f"5. 经过10天涨幅超{price_rise_thresh}%条件筛选后剩余股票数: {len(final_stocks)}")
    return final_stocks




def get_concept_info(stock_code):
    """
    获取股票所属的概念板块信息
    :param stock_code: 股票代码（如000001.SZ）
    :return: 板块代码字符串(逗号分隔)、板块名称字符串(逗号分隔)
    """
    db = get_database()
    # 查询该股票所属的所有概念板块
    member_cursor = db.a_tdx_member.find(
        {"con_code": stock_code}
    ).distinct("ts_code")

    if not member_cursor:
        return "", ""

    # 获取板块代码列表
    concept_codes = list(member_cursor)

    # 查询板块名称
    concept_names = []
    i = 0
    for code in concept_codes:
        i +=1
        if i > 5:
            break
        concept = db.a_tdx_index.find_one(
            {"ts_code": code},
            {"name": 1}
        )
        if concept and "name" in concept:
            concept_names.append(concept["name"])

    # 合并为逗号分隔的字符串
    return ",".join(concept_codes), ",".join(concept_names)


def get_recommended_stocks(target_stocks, trade_date):
    """获取推荐的股票列表并按要求格式处理"""
    db = get_database()
    # 示例：获取最近交易日的股票数据（实际逻辑可替换为你的筛选策略）
    stock_cursor = db.a_share_daily.find(
        {"ts_code": {"$in":target_stocks}, "trade_date": trade_date},
        {"ts_code": 1, "close": 1, "pct_change": 1, "trade_date": 1}
    )
    recommended = []
    for stock in stock_cursor:
        # 获取股票基本信息
        basic_info = db.a_share_basic.find_one(
            {"ts_code": stock["ts_code"]},
            {"name": 1, "industry": 1}
        )
        # 获取概念板块信息
        concept_codes, concept_names = get_concept_info(stock["ts_code"])

        # "table_headers": ["股票代码", "股票名称", "日期", "价格", "涨跌幅(%)", "概念板块"],
        stock_data = [
            stock["ts_code"],
            basic_info.get("name", ""),
            trade_date,
            round(stock.get("close", 0), 2),
            round(stock.get("pct_change", 0), 2),
            concept_names,
        ]
        recommended.append(stock_data)

    return recommended



if __name__ == "__main__":
    # 第一步：筛选概念板块
    print("===== 筛选符合条件的概念板块 =====")
    top_concepts = filter_top_concepts()
    if top_concepts:
        for i, concept in enumerate(top_concepts, 1):
            print(f"{i}. 代码: {concept['ts_code']}, 名称: {concept['name']}, "
                  f"累计涨幅: {concept['total_pct']}%, 上涨天数: {concept['up_days']}, "
                  f"平均成交额: {concept['avg_amount']}万元")

        # 第二步：筛选股票
        print("\n===== 筛选符合条件的股票 =====")
        concept_codes = [c['ts_code'] for c in top_concepts]
        target_stocks = filter_target_stocks(concept_codes)
        if target_stocks:
            print(f"\n最终符合所有条件的股票共 {len(target_stocks)} 只:")
            print(", ".join(target_stocks))

            recommend = get_recommended_stocks(target_stocks, "20250925")
            for stock_code in recommend:
                print(stock_code)
            # push.send_dingtalk_stock_message(recommend)

        else:
            print("\n未找到符合条件的股票")
    else:
        print("未找到符合条件的概念板块")


