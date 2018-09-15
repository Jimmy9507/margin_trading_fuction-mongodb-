from ..mysqlconnection import Mysql


class FetchingShenwan(Mysql):
    def __init__(self, shenwan_config_dict):
        super().__init__(shenwan_config_dict)

    def get_shenwan_by_date(self, date, index_code):
        with self._get_connection() as connection:
            sql = "SELECT S.SecuCode, S.SecuMarket FROM LC_IndexComponent AS L, SecuMain AS S " \
                  "WHERE L.IndexInnerCode = {} AND L.InDate <= '{}' AND (L.OutDate > '{}' OR L.OutDate is null) " \
                  "AND L.SecuInnerCode = S.InnerCode".format(index_code, str(date), str(date))
            #print(sql)
            with connection.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchall()
