# coding: utf-8
from define.log_config import logger
from data_source import AbstractDataSource


class EastMoneySource(AbstractDataSource):
    def __init__(self):
        super(EastMoneySource, self).__init__()

    def query_stock_meta(self, test):
        logger.info(f"test {test}")


EastMoneySource.register_query("query_stock_meta", EastMoneySource.query_stock_meta)


if __name__ == "__main__":
    source = EastMoneySource()
    source.query("query_stock_meta", "haha")
