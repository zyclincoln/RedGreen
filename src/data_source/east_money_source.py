# coding: utf-8
import pandas as pd
import akshare as ak
from define.log_config import logger
from data_source import AbstractDataSource


class EastMoneySource(AbstractDataSource):
    def __init__(self):
        super(EastMoneySource, self).__init__()

    def query_stock_meta(self):
        logger.info("Query stock meta via Akshare")
        sse_stocks = ak.stock_info_sh_name_code()
        sse_data = pd.DataFrame({"code": sse_stocks["证券代码"],
                                 "name": sse_stocks["证券简称"],
                                 "market": "China_SSE",
                                 "full_name": sse_stocks["公司全称"],
                                 "uni_key": sse_stocks["证券代码"] + ".SH"})
        szse_stocks = ak.stock_info_sz_name_code()
        szse_data = pd.DataFrame({"code": szse_stocks["A股代码"],
                                  "name": szse_stocks["A股简称"],
                                  "market": "China_SZSE",
                                  "uni_key": szse_stocks["A股代码"] + ".SZ"})
        hk_stock = ak.stock_hk_spot()
        hk_data = pd.DataFrame({"code": hk_stock["symbol"],
                                "name": hk_stock["name"],
                                "market": "HK",
                                "uni_key": hk_stock["symbol"] + ".HK"})
        us_stock = ak.stock_us_spot_em()
        us_data = pd.DataFrame({"code": us_stock["代码"],
                                "name": us_stock["名称"],
                                "market": "US",
                                "uni_key": us_stock["代码"] + ".US"})
        ret_data = pd.concat([sse_data, szse_data, hk_data, us_data], ignore_index=True)
        logger.info(f"Query {ret_data.shape[0]} pieces of data")
        return ret_data


EastMoneySource.register_query("stock_meta", EastMoneySource.query_stock_meta)


if __name__ == "__main__":
    print(ak.stock_info_sh_name_code()[:10])
    print(ak.stock_info_sz_name_code()[:10])

    t1 = ak.stock_hk_spot()[:10]
    print(t1.keys)
    print(t1.dtypes)
    print(t1[:10])

    t2 = ak.stock_us_spot_em()[:10]
    print(t2.keys)
    print(t2.dtypes)
    print(t2[:10])
    exit()
    source = EastMoneySource()
    source.query("query_stock_meta", "haha")
