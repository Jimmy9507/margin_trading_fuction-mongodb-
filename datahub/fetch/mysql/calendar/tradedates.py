import datetime

from typing import List
import bisect
from pkg_resources import resource_filename
from pandas import read_csv

from datahub.common.const import DATEFORMAT
from ..mysqlconnection import Mysql
import threading


def get_old_trading_dates():
    """
    get trading dates before CSI 300 listing date
    :return: list of dict
    """
    tradedate_file = resource_filename('datahub.fetch.mysql.calendar', "tradedate.csv")
    old_trading_date_df = read_csv(tradedate_file)["TRADEDATE"]
    old_trading_date_list = [int(x) for x in old_trading_date_df]
    tradedate_list = list()
    for date_int in old_trading_date_list:
        date_dict = dict()
        y, m, d = parse_date(date_int)
        date_dict[DATEFORMAT.DATE_INT] = int(date_int)
        dt = datetime.datetime(year=y, month=m, day=d)
        date_dict[DATEFORMAT.DATE_DATE] = dt
        date_dict[DATEFORMAT.DATE_STR] = dt.strftime("%Y-%m-%d")
        tradedate_list.append(date_dict)
    return tradedate_list


def parse_date(date_int):
    r, d = divmod(date_int, 100)
    y, m = divmod(r, 100)
    return y, m, d


class Calendar(Mysql):
    _lock = threading.RLock()

    def __init__(self, calendar_config):
        super().__init__(calendar_config)
        self.__all_trading_dates = None
        self.__expire_at = None

    def get_csi300_trading_dates(self):
        """
        get CSI300 trading dates
        :return: list of dict
        """
        with self._get_connection() as connection:
            sql = "SELECT TRADEDATE as {}, date_format(TRADEDATE, '%Y-%m-%d') as {}," \
                  "convert(date_format(TRADEDATE, '%Y%m%d'), unsigned integer) as {} from indx_mkt WHERE " \
                  "INNER_CODE='106000232' AND (ISVALID = 1) ORDER BY TRADEDATE DESC" \
                .format(DATEFORMAT.DATE_DATE, DATEFORMAT.DATE_STR, DATEFORMAT.DATE_INT)
            with connection.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchall()

    def get_latest_trading_dates(self, trading_date: int) -> List[int]:
        """
        get trading dates which is larger than date_int
        """
        return self.__get_latest_trading_dates(trading_date)

    def get_trading_dates_in_range(self, start_date: int, end_date: int) -> List[int]:
        """
        get trading dates belongs to [start_date, end_date]
        """
        self.__get_trading_dates()
        start_pos = bisect.bisect_left(self.__all_trading_dates, start_date)
        end_pos = bisect.bisect_right(self.__all_trading_dates, end_date)
        return self.__all_trading_dates[start_pos: end_pos]

    def get_latest_trading_date(self) -> int:
        """
        :return: latest trading date
        """
        with self._get_connection() as connection:
            sql = "SELECT MAX(TRADEDATE) as latest_trading_date FROM indx_mkt"
            with connection.cursor() as cursor:
                cursor.execute(sql)
                return str(cursor.fetchone()['latest_trading_date'].strftime("%Y%m%d"))

    def __get_trading_dates(self):
        if self.__all_trading_dates is not None and datetime.datetime.now() < self.__expire_at:
            return self.__all_trading_dates

        with Calendar._lock:
            if self.__all_trading_dates is not None and datetime.datetime.now() < self.__expire_at:
                return self.__all_trading_dates

            self.__all_trading_dates = [r[DATEFORMAT.DATE_INT] for r in self.get_csi300_trading_dates()]
            self.__all_trading_dates.sort()
            self.__expire_at = datetime.datetime.now() + datetime.timedelta(hours=12)

        return self.__all_trading_dates

    def __get_latest_trading_dates(self, trading_date) -> List[int]:
        self.__get_trading_dates()
        start_pos = bisect.bisect_right(self.__all_trading_dates, int(trading_date))
        return self.__all_trading_dates[start_pos:]
