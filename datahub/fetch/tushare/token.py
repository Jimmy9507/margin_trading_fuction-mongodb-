import tushare as ts

TUSHARE_TOKEN = "223a7793e53d11333ca0edb390632a50712271b78af9938173831fcd4cf5c539"


def init_token():
    ts.set_token(TUSHARE_TOKEN)


def get_token():
    return TUSHARE_TOKEN