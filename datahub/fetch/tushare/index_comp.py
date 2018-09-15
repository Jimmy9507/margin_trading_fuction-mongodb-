import datetime
from os import path
from math import ceil

from tushare.util.common import Client
from typing import List, Dict
from pandas import read_csv
import tushare as ts

from .token import get_token


class IndexComponentsCache:
    def __init__(self, index_config_dict):
        self._cache = None
        self._expire_at = None
        self._index_config = index_config_dict

    def get_index_comp(self):
        if self._cache and self._expire_at < datetime.datetime.now():
            return self._cache

        self._expire_at = datetime.datetime.now() + datetime.timedelta(minutes=30)
        self._cache = IndexComponents(self._index_config)
        return self._cache


class IndexComponents(ts.Idx):
    def __init__(self, index_config_dict):
        super().__init__(Client(get_token()))
        index_path = index_config_dict['path']
        index_file = index_config_dict['file']
        self.index_filepath = path.join(index_path, index_file)

    def get_all_index(self, num) -> List[Dict]:
        index_df = read_csv(self.index_filepath)
        index_id_listed_date_df = index_df[["OrderBookID", "ListedDate"]]
        id_date_dict_list = index_id_listed_date_df.to_dict(orient="record")

        length = len(id_date_dict_list)
        sliced_list = list()
        sliced_num = ceil(length / num)
        start = 0
        end = sliced_num
        while start < length:
            one_slice = id_date_dict_list[start:end]
            one_dict = dict()
            for element in one_slice:
                orderbook_id = element.get("OrderBookID")
                listed_date = element.get("ListedDate")
                one_dict[orderbook_id] = listed_date
            sliced_list.append(one_dict)
            start += sliced_num
            end += sliced_num
        return sliced_list

    def get_index_components(self, orderbook_id: str, date: str):
        ticker = orderbook_id.split('.')[0]
        comp_df = self.IdxCons(ticker=ticker, intoDate=date, field="consID")
        if comp_df.empty:
            return None
        else:
            return list(set(comp_df["consID"].values))
