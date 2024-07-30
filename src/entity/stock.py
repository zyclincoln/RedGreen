# coding: utf-8
import os
import datetime
from typing import List

import pandas
from pandas import DataFrame
from typing import Optional
from sqlalchemy import String, create_engine, Engine, DateTime
from sqlalchemy.orm import Mapped, DeclarativeBase, mapped_column, Session
from sqlalchemy.sql.expression import text
from src.define import Market, Singleton
from src.storage import Storage


class Base(DeclarativeBase):
    pass


class TimeSeriesData(DeclarativeBase):
    id: Mapped[int] = mapped_column(primary_key=True)
    uni_key: Mapped[str] = mapped_column(String(16), index=True)
    timestamp: Mapped[datetime.datetime] = mapped_column(DateTime, index=True)


class StockMeta(Base):
    __tablename__ = "stock_meta"
    id: Mapped[int] = mapped_column(primary_key=True)
    market: Mapped[str] = mapped_column(String(10))
    code: Mapped[str] = mapped_column(String(10))
    uni_key: Mapped[str] = mapped_column(String(16), unique=True, index=True)
    name: Mapped[str]
    full_name: Mapped[Optional[str]]


    @classmethod
    def get_data(cls, session: Session, uni_key: List[str]):
        query = session.query(cls)
        if uni_key:
            query = query.filter(cls.uni_key.in_(uni_key))
        return pandas.read_sql(query.statement, query.session.bind)


    @classmethod
    def update_data(cls, db_engine: Engine, data_source, force: False):
        df_data: DataFrame = data_source.query(cls.__tablename__)
        with Session(db_engine) as session:
            ids = df_data['uni_key']
            if force:
                sql_statement = text(f"delete from `{cls.__tablename__}` where uni_key in {tuple(ids)}")
                session.execute(sql_statement)
            else:
                exist_data = cls.get_data(session, uni_key=ids)
                df_data = df_data[~df_data["uni_key"].isin(exist_data["uni_key"])]
            df_data.to_sql(schema=cls.__tablename__, con=session.connection(), index=False, if_exists='append')
            session.commit()


class StockFinance(Base):
    __tablename__ = "stock_finance"


class StockKData(TimeSeriesData):
    __tablename__ = "stock_k_data"
    level: Mapped[str] = mapped_column(String(6))

    low: Mapped[float]
    high: Mapped[float]
    open: Mapped[float]
    close: Mapped[float]

    volume: Mapped[float]
    turnover: Mapped[float]
    change_pct: Mapped[float]
    turnover_rate: Mapped[float]

    @classmethod
    def init_data(cls):



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

    def roe(self, years=5):
        data = Storage().query_stock_basic_financial(market=self._market, code=self._code)
        roe_list = data.sort_values("period", ascending=False)["ROE"].tolist()
        while len(roe_list) < years:
            roe_list.append(0)
        return roe_list

    def eps(self, years=5):
        data = Storage().query_stock_basic_financial(market=self._market, code=self._code)
        eps_list = data.sort_values("period", ascending=False)["EPS"].tolist()
        while len(eps_list) < years:
            eps_list.append(0)
        return eps_list


@Singleton
class StockGenerator(object):

    def __init__(self):
        pass

    def from_data(self, data):
        return Stock(
            market=Market.from_string(data["market"]),
            code=data["code"],
            name=data["name"],
            pid=data["pindex"]
        )

    def create(self, market: Market, code: str):
        return Storage().query_stock(market, code)


if __name__ == "__main__":
    db_path = os.path.join(os.getcwd(), "..", "context", "data", "test.db")
    engine = create_engine(f"sqlite:///{db_path}", echo=True)
    Base.metadata.create_all(engine)
