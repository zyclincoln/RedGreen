# coding: utf-8
from datetime import datetime
import pandas as pd
import akshare as ak
from define.log_config import logger
from data_source import AbstractDataSource, KLevel, parse_unikey, convert_to_number


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

    def query_stock_finance(self, uni_key, level: KLevel):
        code, market = parse_unikey(uni_key)
        if market == "SH":
            indicator = ""
            if level == KLevel.Annually:
                indicator = "按年度"
            raw_data = ak.stock_financial_abstract_ths(symbol=code, indicator="按年度")
            data_frame = pd.DataFrame(
                {"code": code,
                 "market": market.to_string(),
                 "period": raw_data["报告期"],
                 "NI": raw_data["净利润"].apply(convert_to_number),
                 "NI_YOY": raw_data["净利润同比增长率"].apply(convert_to_number),
                 "NON_NI": raw_data["扣非净利润"].apply(convert_to_number),
                 "NON_NI_YOY": raw_data["扣非净利润同比增长率"].apply(convert_to_number),
                 "OI": raw_data["营业总收入"].apply(convert_to_number),
                 "OI_YOY": raw_data["营业总收入同比增长率"].apply(convert_to_number),
                 "EPS": raw_data["基本每股收益"].apply(convert_to_number),
                 "NAVPS": raw_data["每股净资产"].apply(convert_to_number),
                 "OCFPS": raw_data["每股经营现金流"].apply(convert_to_number),
                 "NPM": raw_data["销售净利率"].apply(convert_to_number),
                 "ROE": raw_data["净资产收益率"].apply(convert_to_number),
                 "ROE_DILUTED": raw_data["净资产收益率-摊薄"].apply(convert_to_number),
                 "DR": raw_data["资产负债率"].apply(convert_to_number)
                 }
            )
            return data_frame
        elif market == "SZ":
            pass
        elif market == "HK":
            pass
        else:
            pass

    def query_stock_kdata(self, begin_time: datetime, end_time: datetime, level: KLevel):

        pass


EastMoneySource.register_query("stock_meta", EastMoneySource.query_stock_meta)
EastMoneySource.register_query("stock_finance", EastMoneySource.query_stock_finance)
EastMoneySource.register_query("stock_kdata", EastMoneySource.query_stock_kdata)



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
