# coding: utf-8
from src.define import Market, Singleton
from src.storage import Storage


class Stock(object):

    def __init__(self, market: Market, code: str, name: str, pid: str):
        self._market = market
        self._code = code
        self._name = name
        self._pid = pid

    @property
    def market(self):
        return self._market

    @property
    def code(self):
        return self._code

    @property
    def name(self):
        return self._name

    @property
    def pid(self):
        return self._pid

    def roe(self):
        return []


@Singleton
class StockGenerator(object):

    def __init__(self):
        pass

    def create(self, market: Market, code: str):
        return Storage().query_stock(market, code)
