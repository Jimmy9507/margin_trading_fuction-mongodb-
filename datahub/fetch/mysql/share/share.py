from urllib.parse import urlparse
import mysql.connector


class FetchingShare:
    def __init__(self, share_config_dict):
        self.start_date = share_config_dict['start_date']
        result = urlparse(share_config_dict['source'])
        self._host = result.hostname
        self._port = result.port
        self._user = result.username
        self._passwd = result.password
        self._db = result.path[1:]

    def _get_share(self):
        self.connection = mysql.connector.connect(host=self._host, port=self._port, user=self._user,
                                                  password=self._passwd, database=self._db)
        sql = ("SELECT CHANGEDATE, A_STOCKCODE, TOTAL, FL_ASHR, MNG_FL, TOT_NONFL, B_SHR, H_SHR, S_SHR, N_SHR " \
               "FROM stk_shr_stru WHERE A_STOCKCODE IS NOT NULL AND ISVALID = 1 AND CTIME >= '{}' " \
               "ORDER BY CHANGEDATE ASC".format(self.start_date.strftime('%Y-%m-%d %H:%M:%S')))
        cursor = self.connection.cursor(dictionary=True, buffered=False)
        cursor.execute(sql)
        return cursor
