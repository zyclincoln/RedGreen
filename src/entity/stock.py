# coding: utf-8
from src.define import Market, Singleton
from src.storage import Storage


class Stock(object):

    def __init__(self, market: Market, code: str):
        self.market = market
        self.code = code


@Singleton
class StockGenerator(object):

    def __init__(self):
        self.stocks = None

    def create(self, market: Market, code: str):
        if self.stocks is None:
            self.stocks = Storage().get_all_stocks()
        for stock in self.stocks:
            if stock["market"] == Market and stock["code"] == code:
                return Stock(market, code)
        return None
