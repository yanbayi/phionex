//==========================================1. 股票基础信息表 - 存储股票基本资料==============================================
//use stock;
if (db.a_share_basic.exists()) {
    db.a_share_basic.drop();
    print("已删除旧的 a_share_basic 集合（含索引）");
} else {
    print("a_share_basic 集合不存在，无需删除");
}

db.createCollection("a_share_basic", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      properties: {
        "ts_code": {
          bsonType: "string",
          description: "股票唯一代码"
        },
        "name": {
          bsonType: "string",
          description: "股票简称"
        },
        "market": {
          bsonType: "string",
          enum: ["主板", "创业板", "科创板", "北交所"],
          description: "市场分层，必须为 [主板/创业板/科创板/北交所] 之一"
        },
        "exchange": {
          bsonType: "string",
          description: "交易所，必须为 [SH/SZ/BJ] 之一"
        },
        "industry": {
          bsonType: "string",
          description: "所属行业"
        },
        "area": {
          bsonType: "string",
          description: "所属地域"
        },
        "list_date": {
          bsonType: "string",
          description: "上市日期"
        },
        "list_status": {
          bsonType: "string",
          description: "上市状态 L上市 D退市 P暂停上市"
        },
         "is_st": {
          bsonType: "string",
          description: "上市日期"
         },
         "up_time": {
          bsonType: "date",
          description: "数据获取时间，必须为标准Date类型"
        },
      }
    }
  },
  validationLevel: "strict",
  validationAction: "error"
});

db.a_share_basic.createIndex({ "ts_code": 1, "market":1, "is_st":1})

//===================================================2.日线==============================================================
//use stock;
if (db.a_share_daily.exists()) {
    db.a_share_daily.drop();
    print("已删除旧的 a_share_daily 集合，准备创建新结构");
}

db.createCollection("a_share_daily", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            properties: {
                "ts_code": {
                  bsonType: "string",
                  description: "股票唯一代码"
                },
                "trade_date": {
                    bsonType: "string",
                    description: "交易日"
                },
                "open": {
                    bsonType: "number",
                    description: "开盘价"
                },
                "high": {
                    bsonType: "number",
                    description: "最高价"
                },
                "low": {
                    bsonType: "number",
                    description: "最低价"
                },
                "close": {
                    bsonType: "number",
                    description: "收盘价"
                },
                "pre_close": {
                    bsonType: "number",
                    description: "前收盘价"
                },
                "change": {
                    bsonType: "number",
                    description: "涨跌额"
                },
                "pct_change": {
                    bsonType: "number",
                    description: "涨跌幅 %（今收-除权昨收）/除权昨收"
                },
                "vol": {
                    bsonType: "number",
                    description: "成交量 手"
                },
                "amount": {
                    bsonType: "number",
                    description: "成交额 千元"
                },
                "vol_ratio": {
                    bsonType: "number",
                    description: "量比"
                },
                "turn_over": {
                    bsonType: "number",
                    description: "换手率"
                },
                "swing": {
                    bsonType: "number",
                    description: "振幅 (high-low)/pre_close*100，范围[0,20]，保留2位小数"
                },
                "selling": {
                    bsonType: "number",
                    description: "内盘（主动卖，手）"
                },
                "buying": {
                    bsonType: "number",
                    description: "外盘（主动买， 手）"
                },
                "strength": {
                    bsonType: "number",
                    description: "强弱度(%)"
                },
                "activity": {
                    bsonType: "number",
                    description: "活跃度(%)"
                },
                "attack": {
                    bsonType: "number",
                    description: "攻击波(%)"
                },
                 "avg_price": {
                    bsonType: "number",
                    description: "当日均价amount*10000/(vol*100)，保留2位小数"
                },
                "bbi": {
                    bsonType: "number",
                    description: "bbi，保留2位小数"
                },
                "up_time": {
                    bsonType: "date",
                    description: "数据获取时间"
                },
            }
        }
    },
    validationLevel: "strict",
    validationAction: "error",
    storageEngine: {
        wiredTiger: {}
    }
});

db.a_share_daily.createIndex({ "ts_code": 1, "trade_date": -1},{unique: true,background: true,name: "idx_uniq_code_date"});

//===================================================3.概念板块信息==============================================================
//use stock;
if (db.a_tdx_index.exists()) {
    db.a_tdx_index.drop();
    print("已删除旧的 a_tdx_index 集合，准备创建新结构");
}
db.createCollection("a_tdx_index", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            properties: {
                "ts_code": {
                  bsonType: "string",
                  description: "概念唯一代码"
                },
                "trade_date": {
                    bsonType: "string",
                    description: "交易日"
                },
                "up_time": {
                    bsonType: "date",
                    description: "数据获取时间"
                },
                "name": {
                    bsonType: "string",
                    description: "板块名称"
                },
                "idx_type": {
                    bsonType: "string",
                    description: "板块类型"
                },
                "idx_count": {
                    bsonType: "number",
                    description: "成分个数"
                },
                "total_share": {
                    bsonType: "number",
                    description: "总股本(亿)"
                },
                "float_share": {
                    bsonType: "number",
                    description: "流通股(亿)"
                },
                "total_mv": {
                    bsonType: "number",
                    description: "总市值(亿)"
                },
                "float_mv": {
                    bsonType: "number",
                    description: "流通市值(亿)"
                },
            }
        }
    },
    validationLevel: "strict",
    validationAction: "error",
    storageEngine: {
        wiredTiger: {}
    }
});

// 创建辅助索引（提高查询效率）
db.a_tdx_index.createIndex({ "idx_type": 1 });
db.a_tdx_index.createIndex({ "ts_code": 1 },{unique: true, background: true, name: "idx_uniq_code_date"});



//===================================================4.概念板块成分==============================================================
//use stock;

if (db.a_tdx_member.exists()) {
    db.a_tdx_member.drop();
    print("已删除旧的 a_tdx_member 集合，准备创建新结构");
}

db.createCollection("a_tdx_member", {
  // 文档验证规则（确保字段类型和完整性）
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["trade_date", "con_code", "con_name"], // 必选核心字段
      properties: {
        ts_code: {
          bsonType: "string",
        },
        trade_date: {
          bsonType: "string",
        },
        con_code: {
          bsonType: "string",
        },
        con_name: {
          bsonType: "string",
        },
        up_time: {
          bsonType: "date",
        },
      }
    }
  },
  validationLevel: "strict",
  validationAction: "error"
});

db.a_tdx_member.createIndex({ "con_code": 1 });


//===================================================5.概念板块日线==============================================================
//use stock;
if (db.a_tdx_daily.exists()) {
    db.a_tdx_daily.drop();
    print("已删除旧的 a_tdx_daily 集合，准备创建新结构");
}
db.createCollection("a_tdx_daily", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      properties: {
        ts_code: {
          bsonType: "string",
        },
        trade_date: {
          bsonType: "string",
          description: "交易日期",
        },
        open: {
          bsonType: "number",
          description: "板块开盘价（单位：点）",
        },
        high: {
          bsonType: "number",
          description: "板块最高价（单位：点）",
        },
        low: {
          bsonType: "number",
          description: "板块最低价（单位：点）",
        },
        close: {
          bsonType: "number",
          description: "板块收盘价（单位：点）",
        },
        pre_close: {
          bsonType: "number",
          description: "昨日收盘点（单位：点）",
        },
        change: {
          bsonType: "number",
          description: "涨跌点位",
        },
        pct_change: {
          bsonType: "number",
          description: "涨跌幅字段",
        },
        vol: {
          bsonType: "number",
          description: "板块成交量（单位：手），不能为空",
        },
        amount: {
          bsonType: "number",
          description: "板块成交额（单位：万元）",
        },
        rise: {
          bsonType: "string",
          description: "收盘涨速%",
        },
        vol_ratio: {
          bsonType: "number",
          description: "量比",
        },
        turnover_rate: {
          bsonType: "number",
          description: "换手%",
        },
        swing: {
          bsonType: "number",
          description: "振幅%",
        },
        up_num: {
          bsonType: "number",
          description: "上涨家数",
        },
        limit_up_num: {
          bsonType: "number",
          description: "涨停家数",
        },
        lu_days: {
          bsonType: "number",
          description: "连涨天数",
        },
        "3day": {
          bsonType: "number",
          description: "3日涨幅%",
        },
        "5day": {
          bsonType: "number",
          description: "5日涨幅%",
        },
        "10day": {
          bsonType: "number",
          description: "10日涨幅%",
        },
        "20day": {
          bsonType: "number",
          description: "20日涨幅%",
        },
        "60day": {
          bsonType: "number",
          description: "60日涨幅%",
        },
        bm_buy_net: {
          bsonType: "number",
          description: "主买净额(元)",
        },
        bm_buy_ratio: {
          bsonType: "number",
          description: "主买占比%",
        },
        bm_net: {
          bsonType: "number",
          description: "主力净额",
        },
        bm_ratio: {
          bsonType: "number",
          description: "主力占比%",
        },
        up_time: {
          bsonType: "date",
        },
      }
    }
  },
  validationLevel: "strict",
  validationAction: "error",
  storageEngine: {
    wiredTiger: {}
  }
});

db.a_tdx_daily.createIndex({ "ts_code": 1, "trade_date": 1 },{unique: true,background: true,name: "idx_uniq_code_date"});
db.a_tdx_daily.createIndex({ "trade_date": 1 }, {background: true, name: "idx_trade_date", description: "优化按日期的批量查询（如查询某日所有板块行情）"});

