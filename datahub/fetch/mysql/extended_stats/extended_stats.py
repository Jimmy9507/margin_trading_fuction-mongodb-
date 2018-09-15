from ..mysqlconnection import Mysql


class FetchingExtendedStats(Mysql):
    def __init__(self, config_dict):
        super().__init__(config_dict)

    def _get_extended_stats(self):
        with self._get_connection() as connection:
            sql = 'SELECT stockcode,ex_date,regi_date,enddate,turnover_day,turnover_week,'
            'turnover_month,turnover_six_month,turnover_year,turnover_year_sofar,turnover_sofar'
            with connection.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchall()
