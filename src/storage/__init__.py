# coding: utf-8
import sqlite3 as sqlite
import pandas as pd
from define import Singleton, Market
from define.log_config import logger
from data_source import DataSource

"""
数据库相关接口，上层应用从此层获取数据，如果数据不存在，通过datasource进行获取
"""


@Singleton
class Storage(object):
    def __init__(self):
        logger.info("Initialize Storage...")
        self.conn = sqlite.connect('data.db')

        self.cur = self.conn.cursor()

        # 初始化股票基础信息表
        res = self.cur.execute("SELECT name FROM sqlite_master WHERE name='target'")
        if res.fetchone() is None:
            logger.info("target table not exist, init...")
            self.cur.execute("""CREATE TABLE target( 
                        pindex VARCHAR(50) PRIMARY KEY, 
                        market VARCHAR(20) NOT NULL, 
                        code VARCHAR(50) NOT NULL, 
                        name VARCHAR(200) NOT NULL)""")
            ds = DataSource()
            data = ds.query_all_stocks()
            data.to_sql(name="target", con=self.conn, if_exists="append", index=False)
        else:
            logger.info("target table exist.")

        # 初始化股票财务信息表
        res = self.cur.execute("SELECT name FROM sqlite_master WHERE name='stock_basic_finance'")
        if res.fetchone() is None:
            logger.info("create stock_finance table...")
            self.cur.execute("""CREATE TABLE stock_basic_finance(
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                code VARCHAR(20) NOT NULL,
                                market VARCHAR(20) NOT NULL,
                                period INTEGER NOT NULL,
                                NI FLOAT,
                                NI_YOY FLOAT,
                                NON_NI FLOAT,
                                NON_NI_YOY FLOAT,
                                OI FLOAT,
                                OI_YOY FLOAT,
                                EPS FLOAT,
                                NAVPS FLOAT,
                                OCFPS FLOAT,
                                NPM FLOAT,
                                ROE FLOAT,
                                ROE_DILUTED FLOAT,
                                DR FLOAT)""")
            self.cur.execute("""CREATE INDEX stock_basic_finance_period_index ON stock_basic_finance (period)""")
            self.cur.execute("""CREATE INDEX stock_basic_finance_code_index ON stock_basic_finance (code)""")

        logger.info("Storage Initialize Finished.")

    def query_all_stock(self):
        query = "SELECT * from target"
        res = self.cur.execute(query)
        row = res.fetchall()
        columns = [description[0] for description in self.cur.description]
        return pd.DataFrame(row, columns=columns)

    def query_stock(self, market: Market, code: str):
        market_str = market.to_string()
        query = "SELECT * from target WHERE market = ? AND code = ?"
        res = self.cur.execute(query, (market_str, code))
        row = res.fetchall()
        columns = [description[0] for description in self.cur.description]
        return pd.DataFrame(row, columns=columns)

    def query_stock_basic_financial(self, market: Market, code: str):
        market_str = market.to_string()
        query = "SELECT * from stock_basic_finance WHERE market = ? AND code = ?"
        res = self.cur.execute(query, (market_str, code))
        row = res.fetchall()
        columns = [description[0] for description in self.cur.description]
        return pd.DataFrame(row, columns=columns)

    def remove_stock_basic_financial(self, market: Market, code: str):
        market_str = market.to_string()
        query = "DELETE from stock_basic_finance WHERE market = ? AND code = ?"
        return self.cur.execute(query, (market_str, code))

    def update_stock_basic_financial(self, market: Market, code: str):
        logger.info(f"Update basic financial info of {code}, {market.to_string()}")
        try:
            old_df = self.query_stock_basic_financial(market=market, code=code)
            if old_df.empty:
                data = DataSource.query_stock_financial_keyinfo(code=code, market=market)
                if not data.empty:
                    self.remove_stock_basic_financial(market=market, code=code)
                    data.to_sql(name="stock_basic_finance", con=self.conn, if_exists="append", index=False)
            else:
                logger.info("already exist, skip")
        except Exception as e:
            logger.warning(f"Failed due to exception: {str(e)}")


if __name__ == "__main__":
    storage = Storage()
