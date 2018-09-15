from datahub.handle.calendar.tradedates import HandlingCalendar
from pymongo import DESCENDING
from ..mongosave import MongoSave
from datahub.common.const import DATEFORMAT


class StoringCalendar(MongoSave):
    def save(self, first=True):
        if first:
            self._db.calendar.drop()
            trading_dates = self._hc.get_trading_dates()
            self._db.calendar.insert_many(trading_dates)
        else:
            latest_date_in_db = self._get_latest_date_in_db()
            latest_trading_dates = self._hc.get_latest_trading_dates(latest_date_in_db)
            self._db.calendar.insert_many(latest_trading_dates)

    def __init__(self, calendar_config):
        super().__init__(calendar_config)
        self.__create_index(self._db)
        self._hc = HandlingCalendar(calendar_config)

    def _get_latest_date_in_db(self):
        dates = list(self._db.calendar.find(
            sort=[(DATEFORMAT.DATE_INT, -1)],
            limit=1,
            projection={"_id": 0, DATEFORMAT.DATE_INT: 1}
        ))
        return dates[0][DATEFORMAT.DATE_INT]


    @staticmethod
    def __create_index(db):
        db.calendar.create_index([(DATEFORMAT.DATE_DATE, DESCENDING)], unique=True)
        db.calendar.create_index([(DATEFORMAT.DATE_INT, DESCENDING)], unique=True)
        db.calendar.create_index([(DATEFORMAT.DATE_STR, DESCENDING)], unique=True)
