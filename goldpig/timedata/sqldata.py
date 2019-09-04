#coding:utf-8
"""数据管理基础包

　　所有的数据都被设计为二维的、带时间的。

"""
import datetime 
import pandas as pd
from pandas.core.frame import DataFrame
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

import sys,os
sys.path.append(os.path.abspath("../timedata"))
import settings

SQL_SERVER = settings.SQL_SERVER

from loguru import logger



class SqlDayManager():
    """ 按日索引的数据表
    
    每一个表的都有date 索引，目前主要从ｄａｙ拿数，只有ｄａｔｅ
    每一个数据表，必须继承SqlDayManager，并遵循init函数中的限制
    
    每一个表的都有date 索引
    """

    def __init__(self):
        """ 数据初始化
        1.建立数据链接
        2.配置对应的数据表 table_name
        3.配置数据获取函数 data_fun
        约定：
            1.data_fun的格式限制于 data_fun(code, start, end)
            2.任何数据表的索引都是code + date
            3.每新增一个表对应的类，就必须修改table_name,data_fun

        使用例子：
            class HistData(SqlManager):
                def __init__(self):
                    DataManager.__init__(self)
                    self.table_name = 'hist_data'
                    self.data_fun = ts.get_hist_data

            if __name__ == "__main__":
                hd = HistData()
                hd.plot_line('300254',['open','ma5','ma10'])
        """
        self.table_name = None
        self.get_data_fun = None
        self.db_engine = create_engine(SQL_SERVER,echo=False,client_encoding='utf8')

    def add(self, code):
        """ 自动增加数据
        自行判断code下是否获取最新数据，把数据填满
        Args：
            code： str| 代码code
        """

        # 查询code上次时间
        DB_Session = sessionmaker(bind=self.db_engine)
        session = DB_Session()

        # 判断数据表是否存在
        temp = session.execute("select count(*) from pg_class where relname = '%s';" % self.table_name ).first()[0]
        if temp == 0:
            last_day = None
        else:
            last_day = session.execute("select max(trade_date) from %(table_name)s where code = '%(code)s'"% {'table_name': self.table_name,'code': code}).first()[0]

        # 获得code的最新数据，
        data = None
        if last_day:
            start_day = datetime.datetime.strptime(last_day, '%Y%m%d') + datetime.timedelta(days=1)
            start_day_str = start_day.strftime('%Y%m%d')
            try:
                data = self.get_data_fun(ts_code=code,start_date=start_day_str)

            except Exception as e:
                logger.debug(e)
        else:
            try:
                data = self.get_data_fun(ts_code=code)
            except Exception as e:
                logger.debug(e)

        # 把获取的数据写入数据库
        if type(data) != DataFrame:
            logger.debug(u'股票%s: 数据为None' % code)
        else:
            if not data.empty:
                    data['code'] = code
                    try:
                        data.to_sql(self.table_name,self.db_engine,if_exists='append')
                    except Exception as e:
                        logger.debug(e)
            else:
                logger.debug(u'空数据')
        session.close()

    def update(self,code,start,end):
        """更新数据
        获取源，网上最新的数据源，覆盖已有的数据
        Args：
            code： str| 代码code
            start: str|样式'2017-01-01'|开始时间
            end: str|样式'2017-01-01'|结束时间

        """

        # 获取code数据
        data = self.get_data_fun(code,start=start,end=end)

        if type(data) != DataFrame:
            logger.debug('数据为None')  # 对异常hist_data的处理
        else:
            if not data.empty:
                data['code'] = code
                # 删除原有库里的数据
                self.delete(code,start,end)
                data.to_sql(self.table_name,self.db_engine,if_exists='append')
            else:
                logger.debug('空数据')

    def delete(self,code,start,end):
        """删除数据
        Args：
            code： str| 代码code
            start: str|样式'2017-01-01'|开始时间
            end: str|样式'2017-01-01'|结束时间
        """
        DB_Session = sessionmaker(bind=self.db_engine)
        session = DB_Session()
        sql = "delete from %(table_name)s where code='%(code)s' and date>='%(start)s' and date<='%(end)s';" % {'table_name':self.table_name,'code': code,'start':start,'end':end}
        r = session.execute(sql)
        session.commit()
        session.close()
        return r

    def read(self, code, start=None,end=None):
        """从数据库获取code的指定时间段的历史数据
        """
        if not start:
            start = '2015-01-01'
        if not end:
            end_day = datetime.date.today()
            end = end_day.strftime('%Y-%m-%d')

        sql = "select * from %(table_name)s where code = '%(code)s' and date>='%(start)s' and date<='%(end)s';" % {'table_name':self.table_name, 'code': code,'start':start,'end':end}
        all_data = pd.read_sql(sql,self.db_engine,index_col='date')
        result = all_data.sort_index(ascending=0)
        return result


class SqlBaseManager():
    """ 按原始数据存入的数据表

    只有全部写入或者全部读出的功能    
    """

    def __init__(self):
        """ 数据初始化
        1.建立数据链接
        2.配置对应的数据表 table_name
        3.配置数据获取函数 data_fun
        约定：
            1.data_fun的格式限制于 data_fun(code, start, end)
            2.任何数据表的索引都是code + date
            3.每新增一个表对应的类，就必须修改table_name,data_fun

        使用例子：
            class HistData(SqlManager):
                def __init__(self):
                    DataManager.__init__(self)
                    self.table_name = 'hist_data'
                    self.data_fun = ts.get_hist_data

            if __name__ == "__main__":
                hd = HistData()
                hd.plot_line('300254',['open','ma5','ma10'])
        """
        self.table_name = None
        self.data_fun = None
        self.db_engine = create_engine(SQL_SERVER, echo=False, client_encoding='utf8')

    def add_update(self):
        """ 增加或更新数据
        """
        data = self.data_fun()

        # 把获取的数据写入数据库
        if type(data) != DataFrame:
            logger.debug('数据为None')
        else:
            if not data.empty:
                    try:
                        data.to_sql(self.table_name,self.db_engine, if_exists='replace')
                        logger.debug(u'更新%s完毕' % self.table_name)
                    except Exception as e:
                        logger.debug(e)
            else:
                logger.debug('空数据')

    def read(self):
        """从数据库获取全部数据
        """
        sql = "select * from %(table_name)s ;" % {'table_name':self.table_name, }
        all_data = pd.read_sql(sql,self.db_engine)
        result = all_data.sort_index(ascending=0)
        return result

    def delete(self):
        """删除数据
       
        """
        DB_Session = sessionmaker(bind=self.db_engine)
        session = DB_Session()
        sql = "delete from %(table_name)s ;" % {'table_name':self.table_name,}
        r = session.execute(sql)
        session.commit()
        session.close()
        return r