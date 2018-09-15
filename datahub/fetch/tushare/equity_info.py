import tushare as ts

from .token import init_token
from datahub.common.const import EQUITY_INFO


class EquityInfo(ts.Equity):
    def __init__(self):
        init_token()
        super().__init__()

    def get_st(self, secID):
        ret_df = self.SecST(secID=secID, beginDate='20050101', endDate='20180101', field='secID,tradeDate,STflg')
        ret_df.rename(
            columns={"secID": EQUITY_INFO.ID, "tradeDate": EQUITY_INFO.TRD_DATE, "STflg": EQUITY_INFO.ST_FLAG},
            inplace=True
        )
        return ret_df

    def get_specified_period_st(self, secID, begin_date, end_date='20180101'):
        ret_df = self.SecST(secID=secID, beginDate=begin_date, endDate=end_date, field='secID,tradeDate,STflg')
        ret_df.rename(
            columns={"secID": EQUITY_INFO.ID, "tradeDate": EQUITY_INFO.TRD_DATE, "STflg": EQUITY_INFO.ST_FLAG},
            inplace=True
        )
        return ret_df
