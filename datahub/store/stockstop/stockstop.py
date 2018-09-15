import datetime

from pymongo import ASCENDING, DESCENDING
from typing import Dict

from datahub.common.const import EQUITY_INFO
from datahub.fetch.mysql.calendar.tradedates import Calendar
from datahub.handle.stockstop.stockcode import HandlingStockStop
from datahub.store.mongosave import MongoSave


class StoringStopStock(MongoSave):
    def save(self):
        for order_book_id, inner_code in self._hs.get_code_map().items():
            latest_date = self._get_latest_in_db(order_book_id)
            print(order_book_id, latest_date)
            stops = self._hs.get_stop_calendar(inner_code, latest_date)
            self._fill_delisted_stocks(order_book_id, latest_date)
            if len(stops) <= 0:
                continue
            self._db.stk_suspend.insert_many(
                [{
                     EQUITY_INFO.ID: order_book_id,
                     EQUITY_INFO.TRD_DATE: int(stop["stopdate"].strftime("%Y%m%d")),
                     "add-time": datetime.datetime.now()
                 } for stop in stops]
            )

    def __init__(self, config: Dict):
        stop_config = config["stk_stop"]
        super().__init__(stop_config)
        self._create_index()
        self._hs = HandlingStockStop(stop_config)
        self._id_delisted_map = self._hs.get_id_delisted_map()
        self._calendar = Calendar(config["calendar"])

    def _create_index(self):
        self._db.stk_suspend.create_index([("order_book_id", ASCENDING), ("trade_date", ASCENDING)], unique=True)

    def _get_latest_in_db(self, order_book_id: str):
        result = self._db.stk_suspend.find_one(
            filter={EQUITY_INFO.ID: order_book_id},
            sort=[(EQUITY_INFO.TRD_DATE, DESCENDING)]
        )
        if not result:
            return 0
        else:
            return result[EQUITY_INFO.TRD_DATE]

    def _fill_delisted_stocks(self, order_book_id, latest_date):
        delisted_date = self._id_delisted_map.get(order_book_id)
        if not delisted_date:  # it means this stock is listing
            return
        if delisted_date <= latest_date:
            return
        missing_trading_dates = self._calendar.get_trading_dates_in_range(latest_date + 1, delisted_date)
        print(missing_trading_dates, order_book_id)
        self._db.stk_suspend.insert_many(
            [{
                EQUITY_INFO.ID: order_book_id,
                EQUITY_INFO.TRD_DATE: missing_date,
                "add-time": datetime.datetime.now()
            } for missing_date in missing_trading_dates]
        )
