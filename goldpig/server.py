#coding:utf-8
import settings
from timedata.tushare_data import StockBasics,HistData

if __name__ == "__main__":
    hd = HistData()
    hd.add_all()
