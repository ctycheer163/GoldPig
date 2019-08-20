# coding:utf-8
""" 以tushare为源的市场数据表

"""
import loguru

import datetime
import tushare as ts
import pandas as pd
import matplotlib.pyplot as plt

from .sqldata import SqlDayManager, SqlBaseManager

import sys,os
sys.path.append(os.path.abspath("../timedata"))
import settings

from loguru import logger



class StockBasics(SqlBaseManager):
    """ 获取此刻有意义的股票列表
    
    依据 tushare的get_stock_basic
    同时去掉 暂停上市的，终止上市的，风险警示
    
    """
    def stock_basic_fun(self):
        pro = ts.pro_api()
        data = pro.stock_basic()
        if type(data) != pd.DataFrame:
            logger.info('从tushare获取stock_basic数据更新失败')
            return None
        if data.empty:
            logger.info('数据为空，从tushare获取stock_basic数据更新失败')
            return None   
        
        return data

    def __init__(self):
        SqlBaseManager.__init__(self)
        self.table_name = 'stock_basics'
        self.data_fun = self.stock_basic_fun

# class StockMeaning(SqlBaseManager):
#     """日常有用的stock，运行正常的stock
        
#     """
#     def stock_meaning_fun(self, THRESHOLD=50):
#         sb = StockBasics()
#         sb_data = sb.read()
#         filter_stock = []

#         # 过滤规则
#         # 近2个月有交易，最后一个交易日价格在50以下
#         start_day = datetime.datetime.now() - datetime.timedelta(days=14)
#         start_day_str = start_day.strftime('%Y-%m-%d')

#         hd = HistData()
#         for code in sb_data.code:
#             temp = hd.read(code, start=start_day_str)
#             if not temp.empty:
#                 if 5 < temp.iloc[0]['high'] < THRESHOLD:
#                     filter_stock.append(code)
#                     print code

#         result = sb_data[sb_data.code.isin(filter_stock)]
#         return result

#     def __init__(self):
#         SqlBaseManager.__init__(self)
#         self.table_name = 'stock_meaning'
#         self.data_fun = self.stock_meaning_fun


# class HistData(SqlDayManager):
#     """ 以tushare为数据源的历史天的数据
#     数据源是Hist_DATA

#     """
#     def __init__(self):
#         SqlDayManager.__init__(self)
#         self.table_name = 'hist_data'
#         self.data_fun = ts.get_hist_data

#     def add_all(self):
#         """遍历所有code,把所有数据新增
#         """
#         sb = StockBasics()
#         AllStocks = sb.read()

#         no_data_code = []  # 没有数据，或者没有更新数据的code
#         for code in AllStocks.code:
#             logger.debug(u"add %s" % code)
#             is_success = self.add(code)
#             if not is_success:
#                 no_data_code.append(code)

#         return no_data_code



#     def plot_code_box(self, code, start='2015-11-01',end=None,):
#         """画出code的时间蜡烛图
#         Args:
#             code： str| 代码code
#             flag: str or list of str| code返回数据中指定的列名
#             start_day: str|样式'2017-01-01'|开始时间
#             end_day: str|样式'2017-01-01'|结束时间

#         eg:
#             dm = DataManager()
#             dm.plot_code_line('300254')

#         """
#         data = self.read(code, start, end)
#         data.get(['open','high','close','low']).T.plot.box()
#         plt.show()

# class IndustryClassified( SqlBaseManager):
#     """工业分类的类
    
#     """

#     def __init__(self):

#         SqlBaseManager.__init__(self)
#         self.table_name = 'industry_classified'
#         self.data_fun = ts.get_industry_classified
