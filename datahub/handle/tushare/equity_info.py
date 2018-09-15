from datahub.common.const import EQUITY_INFO
from pandas import to_datetime


def drop_s_stock(st_df):
    st_df.drop(st_df[st_df[EQUITY_INFO.ST_FLAG] == "S"].index, inplace=True)


def convert_str_date_to_datetime(st_df):
    st_df[EQUITY_INFO.TRD_DATE] = to_datetime(st_df[EQUITY_INFO.TRD_DATE])


def remove_duplicate(st_df):
    st_df.drop_duplicates(inplace=True)
