from sqlbuilder.smartsql import T, Result, Q
from sqlbuilder.smartsql.compilers.mysql import compile as mysql_compile


class FENJI(object):
    MU_ID = "fenji_mu_orderbook_id"
    MU_SYMBOL = "fenji_mu_symbol"
    TRACK_INDX_SYMBOL = "track_index_symbol"
    CREATION_DATE = "creation_date"
    EXPIRE_DATE = "expire_date"
    CURR_YIELD = "current_yield"
    NEXT_YIELD = "next_yield"
    INTE_RULE = "interest_rule"
    CONV_DATE = "conversion_date"
    AB_PROP = "a_b_propotion"

    A_ID = "fenji_a_order_book_id"
    A_SYMBOL = "fenji_a_symbol"
    B_ID = "fenji_b_order_book_id"
    B_SYMBOL = "fenji_b_symbol"

    MU_INNER_CODE = "mu_inner_code"
    A_INNER_CODE = "a_inner_code"
    B_INNER_CODE = "b_inner_code"
    EXCHANGE_CODE = "exchange_code"
    RELA_INNER_CODE = "rela_inner_code"

    A_OR_B_ID = "fenji_a_or_b_order_book_id"
    A_OR_B_SYMBOL = "fenji_a_or_b_symbol"
    A_OR_B_INNER_CODE = "fenji_a_or_b_inner_code"

    A_LISTING = "a_listing"
    B_LISTING = "b_listing"
    MU_LISTING = "mu_listing"


class BASE_CONST(object):
    ID = "order_book_id"
    TRD_DATE = "trade_date"


class EQUITY_INFO(BASE_CONST):
    ST_FLAG = "st_flag"


class DATEFORMAT(object):
    DATE_DATE = "trade_date_dt"  # datetime type
    DATE_INT = "trade_date_int"  # yyyyMMdd
    DATE_STR = "trade_date_str"  # "yyyy-MM-dd"


class INDEX_COMP(BASE_CONST):
    COMP = "component_ids"


query = Q(result=Result(compile=mysql_compile))
stk_stop_calendar = T.STK_STP_CALENDAR
stk_code = T.STK_CODE
