from ..mysqlconnection import Mysql


class FetchingExFactor(Mysql):
    def __init__(self, factor_config_dict):
        super().__init__(factor_config_dict)

    def _get_ex_factor(self):
        with self._get_connection() as connection:
            sql = 'SELECT stockcode, ex_date, regi_date, enddate, ex_factor, cum_factor FROM stk_ex_factor WHERE ' \
                  'isvalid = 1 ORDER BY stockcode, ex_date;'
            with connection.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchall()

