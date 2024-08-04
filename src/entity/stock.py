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
from src.define.log_config import logger
from src.storage import Storage
from src.data_source import KLevel, Market


class Base(DeclarativeBase):
    pass


class TimeSeriesData(DeclarativeBase):
    id: Mapped[int] = mapped_column(primary_key=True)
    uni_key: Mapped[str] = mapped_column(String(16), index=True)
    level: Mapped[str]
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
    def get_data(cls, session: Session, uni_key: List[str] | None = None, market: Market | str | None = None):
        query = session.query(cls)
        if uni_key is not None and len(uni_key) != 0:
            query = query.filter(cls.uni_key.in_(uni_key))
        if market is not None:
            if isinstance(market, Market):
                market_str = market.to_string()
                query = query.filter(cls.market.is_(market_str))
            else:
                query = query.filter(cls.market.is_(market))
        return pandas.read_sql(query.statement, query.session.bind)

    @classmethod
    def update_data(cls, db_engine: Engine, data_source, force=False):
        df_data: DataFrame = data_source.query(cls.__tablename__)
        with Session(db_engine) as session:
            ids = df_data['uni_key']
            if force:
                sql_statement = text(f"delete from `{cls.__tablename__}` where uni_key in {tuple(ids)}")
                session.execute(sql_statement)
            else:
                exist_data = cls.get_data(session, uni_key=ids)
                df_data = df_data[~df_data["uni_key"].isin(exist_data["uni_key"])]
            df_data.to_sql(name=cls.__tablename__, con=session.connection(), index=False, if_exists='append')
            session.commit()


class StockFinance(TimeSeriesData):
    __tablename__ = "stock_finance"
    net_income: Mapped[float]
    net_income_yoy: Mapped[float]
    operate_income: Mapped[float]
    operation_income_yoy: Mapped[float]
    eps: Mapped[float]
    net_asset_ps: Mapped[float]
    operate_cash_flow_ps: Mapped[float]
    net_profit_margin: Mapped[float]
    roe: Mapped[float]
    debt_asset_ratio: Mapped[float]

    @classmethod
    def get_data(cls, session: Session, uni_key: List[str],
                 begin_time: datetime.datetime, end_time: datetime.datetime, level: KLevel):
        query = session.query(cls)
        query = query.filter(cls.timestamp.between(begin_time, end_time))
        query = query.filter(cls.level.is_(level.to_str()))
        if len(uni_key) != 0:
            query = query.filter(cls.uni_key.in_(uni_key))
        return pandas.read_sql(query.statement, query.session.bind)

    @classmethod
    def update_data(cls, db_engine: Engine, data_source, force=False):
        # TODO: only support HK and China for now
        # Update all code
        with Session(db_engine) as session:
            def fetch_stock_finance(row):
                logger.info(f'{row["name"]}, {row["code"]}, {row["uni_key"]}')
                data = data_source.query("stock_finance", uni_key=row["uni_key"], level=KLevel.Annually)
                data.to_sql(name=cls.__tablename__, con=session.connection(), index=False, if_exists='append')

            sse_stock_meta = StockMeta.get_data(session=session, market=Market.China_SSE)
            sse_stock_meta[0:1].apply(fetch_stock_finance, axis=1)
            session.commit()
        # TODO: update missed code
        # TODO: update missed date
        pass



# class StockFinance(Base):
#     __tablename__ = "stock_finance"
#
#
# class StockKData(TimeSeriesData):
#     __tablename__ = "stock_k_data"
#     level: Mapped[str] = mapped_column(String(6))
#
#     low: Mapped[float]
#     high: Mapped[float]
#     open: Mapped[float]
#     close: Mapped[float]
#
#     volume: Mapped[float]
#     turnover: Mapped[float]
#     change_pct: Mapped[float]
#     turnover_rate: Mapped[float]
#
#     @classmethod
#     def init_data(cls):


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
    db_path = os.path.join(os.getcwd(), "..", "context", "data", "stock_meta.db")
    engine = create_engine(f"sqlite:///{db_path}", echo=True)
    Base.metadata.create_all(engine)

    from data_source.east_money_source import EastMoneySource
    source = EastMoneySource()

    # StockMeta.update_data(engine, source)
    StockFinance.update_data(engine, source)



