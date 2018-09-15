
def is_invalid_stock(orderbook_id):
    return len(orderbook_id) == len("000001.XSHE")


def get_valid_stocks(orderbook_ids):
    return [x for x in orderbook_ids if is_invalid_stock(x)]
