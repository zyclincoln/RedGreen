# encoding: utf-8
from enum import Enum


class Singleton(object):

    def __init__(self, cls):
        self._cls = cls
        self._instance = {}

    def __call__(self):
        if self._cls not in self._instance:
            self._instance[self._cls] = self._cls()
        return self._instance[self._cls]


class Market(Enum):
    China_SSE = 1,
    China_SZSE = 2,
    China_BSE = 3,
    HK = 4,
    US = 5,

    def to_string(self):
        if self.value == Market.China_SSE.value:
            return "SH"
        elif self.value == Market.China_SZSE.value:
            return "SZ"
        elif self.value == Market.US.value:
            return "US"
        elif self.value == Market.HK.value:
            return "HK"
        elif self.value == Market.China_BSE.value:
            return "China_BSE"
        return ""

    @staticmethod
    def from_string(string: str):
        if string == "China_SSE":
            return Market.China_SSE
        elif string == "China_SZSE":
            return Market.China_SZSE
        elif string == "China_BSE":
            return Market.China_BSE
        elif string == "HK":
            return Market.HK
        elif string == "US":
            return Market.US
        else:
            raise Exception("unknown market type")
