from typing import Dict

from datahub.common.const import query, stk_stop_calendar, stk_code
from ..mysqlconnection import Mysql


class FetchingStockStop(Mysql):
    def __init__(self, conf: Dict):
        super().__init__(conf)

    def get_stop_calendar(self, inner_code: int, date: int=0):
        with self._get_connection() as connection:
            sql, params = query.fields(
                stk_stop_calendar.stopdate
            ).tables(stk_stop_calendar).where(
                (stk_stop_calendar.inner_code == inner_code) &
                (stk_stop_calendar.stopdate > date)
            ).select()
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchall()

    def _get_innercode_map(self, stock_codes):
        with self._get_connection() as connection:
            sql, params = query.fields(
                stk_code.inner_code, stk_code.stockcode
            ).tables(stk_code).where(
                stk_code.stockcode.in_(tuple(stock_codes))
            ).select()
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchall()
