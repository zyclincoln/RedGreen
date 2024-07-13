import akshare as ak
from entity.stock import StockGenerator
from define import Market
from data_source import DataSource
from storage import Storage
from define.log_config import logger

if __name__ == "__main__":
    all_stock = Storage().query_all_stock()

    # def update_hk_stock(row):
    #     logger.info(row["name"])
    #     Storage().update_stock_basic_financial(code=row['code'], market=Market.HK)
    #
    # hk_stock = all_stock[all_stock["market"] == Market.HK.to_string()]
    # hk_stock.apply(update_hk_stock, axis=1)

    def update_sse_stock(row):
        logger.info(row["name"])
        Storage().update_stock_basic_financial(code=row['code'], market=Market.China_SSE)

    china_sse_stock = all_stock[all_stock["market"] == Market.China_SSE.to_string()]
    china_sse_stock.apply(update_sse_stock, axis=1)

    def update_szse_stock(row):
        logger.info(row["name"])
        Storage().update_stock_basic_financial(code=row['code'], market=Market.China_SZSE)

    china_szse_stock = all_stock[all_stock["market"] == Market.China_SZSE.to_string()]
    china_szse_stock.apply(update_szse_stock, axis=1)

    # stock_target = StockGenerator().create(market=Market.China_SSE, code="600000")
    # data = DataSource.query_stock_financial_keyinfo(code="00700", market=Market.HK)
    # data.to_sql(name="stock_basic_finance", con=Storage().conn, if_exists="append", index=False)
    # # print(data.keys())
    # data.to_csv("test_hk.csv")
    # Storage().update_stock_basic_financial(code="00700", market=Market.HK)

    exit(0)

    # build ROE operator: in recent 7 years, the roe is larger than 15% in 5 years at least
    def roe_5_year_operator(target: stock):
        roe_list = target.roe(years=7)
        lower_than_15 = 0
        for roe in roe_list:
            if roe < 0.15:
                lower_than_15 += 1
        return lower_than_15 < 3

    print(stock.all().filter(roe_5_year_operator))
