from datahub.fetch.mysql.calendar.tradedates import Calendar, get_old_trading_dates


class HandlingCalendar(Calendar):
    def __init__(self, calendar_config):
        super().__init__(calendar_config)

    def get_trading_dates(self):
        new_tradingdates = self.get_csi300_trading_dates()
        old_tradingdates = get_old_trading_dates()
        new_tradingdates.extend(old_tradingdates)
        return new_tradingdates
