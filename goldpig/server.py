#coding:utf-8
import settings
from timedata.tushare_data import StockBasics

if __name__ == "__main__":
    sb = StockBasics()
    sb.add_update()
