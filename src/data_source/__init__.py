# coding: utf-8
from define.log_config import logger
import akshare as ak
import pandas as pd
from define import Market


class AbstractDataSource(object):
    query_func = {}

    @classmethod
    def register_query(cls, query_type, func):
        cls.query_func[query_type] = func

    def query(self, query_type, *args, **kwargs):
        if query_type in self.__class__.query_func:
            return self.__class__.query_func[query_type](self, *args, **kwargs)
        else:
            logger.warning(f"Source: {self.__class__.__name__} do not support query {query_type}")
            return None


def convert_to_number(value):
    if type(value) is float:
        return value
    if type(value) is int:
        return value
    if type(value) is bool:
        return int(value)

    numeral_map = {
        '万亿': 1000000000000,
        '千亿': 100000000000,
        '百亿': 10000000000,
        '十亿': 1000000000,
        '亿':   100000000,
        '千万': 10000000,
        '百万': 1000000,
        '十万': 100000,
        '万':   10000,
        '千':   1000,
        '百':   100,
        '十':   10
    }

    # Try to convert percentage
    if '%' in value:
        return float(value.replace('%', '')) * 0.01

    # Convert bool
    if value == "False" or value == "false":
        return 0
    if value == "True" or value == "true":
        return 1

    # Try to convert number with Chinese
    for key in numeral_map:
        if key in value:
            num = float(value.replace(key, '')) * numeral_map[key]
            return int(num)

    return float(value)


def date_to_year(value):
    if type(value) is str:
        return value.split('-')[0]
    return ""


class DataSource(object):

    @staticmethod
    def query_cn_sse_stocks():
        sse_stocks = ak.stock_info_sh_name_code()
        sse_data = pd.DataFrame({"code": sse_stocks["证券代码"],
                                 "name": sse_stocks["证券简称"],
                                 "market": "China_SSE",
                                 "pindex": sse_stocks["证券代码"] + ".SH"})
        return sse_data

    @staticmethod
    def query_cn_szse_stocks():
        szse_stocks = ak.stock_info_sz_name_code()
        szse_data = pd.DataFrame({"code": szse_stocks["A股代码"],
                                  "name": szse_stocks["A股简称"],
                                  "market": "China_SZSE",
                                  "pindex": szse_stocks["A股代码"] + ".SZ"})
        return szse_data

    @staticmethod
    def query_hk_stocks():
        hk_stock = ak.stock_hk_spot()
        hk_data = pd.DataFrame({"code": hk_stock["symbol"],
                                "name": hk_stock["name"],
                                "market": "HK",
                                "pindex": hk_stock["symbol"] + ".HK"})
        return hk_data

    @staticmethod
    def query_us_stocks():
        us_stock = ak.stock_us_spot_em()
        us_data = pd.DataFrame({"code": us_stock["代码"],
                                "name": us_stock["名称"],
                                "market": "US",
                                "pindex": us_stock["代码"] + ".US"})
        return us_data

    @staticmethod
    def query_all_stocks():
        sse_data = DataSource.query_cn_sse_stocks()
        szse_data = DataSource.query_cn_szse_stocks()
        hk_data = DataSource.query_hk_stocks()
        us_data = DataSource.query_us_stocks()
        return pd.concat([sse_data, szse_data, hk_data, us_data], ignore_index=True)

    @staticmethod
    def query_stock_financial_keyinfo(code: str, market: Market):
        if market == Market.China_SZSE or market == Market.China_SSE:
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
        elif market == Market.HK:
            raw_data = ak.stock_financial_hk_analysis_indicator_em(symbol=code, indicator="年度")
            data_frame = pd.DataFrame(
                {"code": code,
                 "market": market.to_string(),
                 "period": raw_data["REPORT_DATE"].apply(date_to_year),
                 "NI": raw_data["HOLDER_PROFIT"],
                 "NI_YOY": raw_data["HOLDER_PROFIT_YOY"] / 100,
                 "NON_NI": None,
                 "NON_NI_YOY": None,
                 "OI": raw_data["OPERATE_INCOME"],
                 "OI_YOY": raw_data["OPERATE_INCOME_YOY"] / 100,
                 "EPS": raw_data["BASIC_EPS"],
                 "NAVPS": raw_data["BPS"],
                 "OCFPS": raw_data["PER_NETCASH_OPERATE"],
                 "NPM": raw_data["NET_PROFIT_RATIO"] / 100,
                 "ROE": raw_data["ROE_YEARLY"] / 100,
                 "ROE_DILUTED": None,
                 "DR": raw_data["DEBT_ASSET_RATIO"] / 100,
                 }
            )
            return data_frame


if __name__ == "__main__":
    # data = pd.read_csv("test.csv")
    print(DataSource.query_stock_financial_keyinfo(code="00700", market=Market.HK))
    # print(DataSource.query_stock_financial_keyinfo(code="000063", market=Market.China_SSE))
    pass
