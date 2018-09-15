import unittest

from datahub.fetch.mysql.calendar.tradedates import Calendar, get_old_trading_dates
from datahub.common.const import DATEFORMAT


class TestCalendar(unittest.TestCase):
    def get_calendar(self):
        return Calendar({"source": "mysql://rice:rice@192.168.10.131:3306/pgenius"})

    """
    def test_get_trading_dates(self):
        cl = self.get_calendar()
        dates = cl.get_csi300_trading_dates()[-1]
        print(dates)
        print(type(dates[DATEFORMAT.DATE_INT]))
        print(type(dates[DATEFORMAT.DATE_STR]))
        print(type(dates[DATEFORMAT.DATE_DATE]))

    def test_get_old_trading_dates(self):
        dates = get_old_trading_dates()[-1]
        print(dates)
        print(type(dates[DATEFORMAT.DATE_INT]))
        print(type(dates[DATEFORMAT.DATE_STR]))
        print(type(dates[DATEFORMAT.DATE_DATE]))
    """

    def test_get_latest_trading_dates(self):
        cl = self.get_calendar()
        dates = cl.get_latest_trading_dates(20160627)
        print(dates)

    """
    def test_get_latest_trading_date(self):
        cl = self.get_calendar()
        print(cl.get_latest_trading_date())
    """

if __name__ == '__main__':
    unittest.main()
