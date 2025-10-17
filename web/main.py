import pandas as pd
from flask import Flask, render_template, jsonify, request, redirect, url_for
import time
from datetime import datetime, timedelta
from common import const
from db_ctl.util import mongoDb_ctl
from db_ctl import update_all_data, update_all_stock_formula

app = Flask(__name__)


# 模拟数据库状态存储
# 在实际应用中，这些数据应存储在数据库中
class DataStatus:
    def __init__(self):
        self.conf_coll = mongoDb_ctl.init_mongo_collection(const.CONF_COLL)
        self.stock_date_start = None
        self.stock_date_end = None
        self.tdx_date_start = None
        self.tdx_date_end = None
        self.test_prepare_status = "fail"  # 初始状态为fail
        self.stock_daily_data = pd.DataFrame()
        self.tdx_daily_data = pd.DataFrame()
        self.refresh_stock_tdx_date()

    def refresh_stock_tdx_date(self):
        cursor = self.conf_coll.find({})
        for doc in cursor:
            if doc["name"] == "daily_up_date":
                self.stock_date_end = doc.get("value", "")
            elif doc["name"] == "tdx_daily_up_date":
                self.tdx_date_end = doc.get("value", "")
            elif doc["name"] == "daily_start_date":
                self.stock_date_start = doc.get("value", "")
            elif doc["name"] == "tdx_daily_start_date":
                self.tdx_date_start = doc.get("value", "")
    def refresh_test_prepare_status(self):
        if self.stock_daily_data.empty:
            self.test_prepare_status = "fail"
        else:
            self.test_prepare_status = "ok"
# 创建全局状态实例
data_status = DataStatus()


# 首页路由
@app.route('/')
def index():
    return render_template('index.html',
                           stock_date_start=data_status.stock_date_start,
                           stock_date_end = data_status.stock_date_end,
                           tdx_date_start = data_status.tdx_date_start,
                           tdx_date_end = data_status.tdx_date_end,
                           test_prepare_status=data_status.test_prepare_status)

# 拉取数据接口
@app.route('/pull_data', methods=['POST'])
def pull_data():
    try:
        err = update_all_data.update_all_data_main()
        if err is not None:
            raise Exception(err)
        data_status.refresh_stock_tdx_date()
        data_status.refresh_test_prepare_status()
        return jsonify({'success': True, 'message': '数据拉取成功'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'数据拉取失败: {str(e)}'})


# 测试准备接口
@app.route('/prepare_data', methods=['POST'])
def prepare_test():
    try:
        stock_formula = update_all_stock_formula.StockFormula()
        err, data_status.stock_daily_data = stock_formula.formula_main()
        if err is not None:
            raise Exception(err)
        if data_status.stock_daily_data is None or type(data_status.stock_daily_data) is not pd.DataFrame or data_status.stock_daily_data.empty:
            raise Exception("获取到的日线数据为空")
        data_status.refresh_stock_tdx_date()
        data_status.refresh_test_prepare_status()
        return jsonify({'success': True, 'message': '测试准备成功'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'测试准备失败: {str(e)}'})


if __name__ == '__main__':
    app.run(port=8999,debug=False)
