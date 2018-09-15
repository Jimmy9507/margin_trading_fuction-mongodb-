from pymysql import cursors, connect
from urllib.parse import urlparse


class Connection(object):
    def __init__(self):
        self._connect = None

    def __enter__(self):
        return self._connect

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._connect:
            self._connect.close()


class MysqlConnection(Connection):
    def __init__(self, host, port, user, pwd, db):
        super().__init__()
        self.host = host
        self.port = port
        self.user = user
        self.pwd = pwd
        self.db = db

    def __enter__(self):
        self._connect = connect(host=self.host, port=self.port, user=self.user, password=self.pwd, db=self.db,
                                charset="utf8mb4", cursorclass=cursors.DictCursor)
        return super().__enter__()


class Mysql():
    def __init__(self, mysql_config):
        source = mysql_config["source"]
        result = urlparse(source)
        self._host = result.hostname
        self._port = result.port
        self._user = result.username
        self._pwd = result.password
        self._db = result.path[1:]

    def _get_connection(self):
        return MysqlConnection(host=self._host, port=self._port, user=self._user, pwd=self._pwd, db=self._db)
