from os.path import join

from pandas import read_csv
from typing import Dict

from datahub.fetch.mysql.stockstop.stockstop import FetchingStockStop


class HandlingStockStop(FetchingStockStop):
    def __init__(self, config_dict):
        super().__init__(config_dict)
        self._inst_file_list = config_dict["inst_file"]
        self._inst_path = config_dict["inst_path"]
        self._stock_codes = dict()
        for file in self._inst_file_list:
            df = read_csv(join(self._inst_path, file))
            for order_book_id in set(df.OrderBookID):
                self._stock_codes[order_book_id.split(".")[0]] = order_book_id

    def get_code_map(self) -> Dict:
        result = self._get_innercode_map(self._stock_codes.keys())
        innercode_map = dict()
        for r in result:
            innercode_map[self._stock_codes[r["stockcode"]]] = r["inner_code"]
        return innercode_map

    def get_id_delisted_map(self) -> Dict:
        id_delisted_map = dict()
        for file in self._inst_file_list:
            df = read_csv(join(self._inst_path, file))
            delisted_insts = df[df["DeListedDate"] != "null"]
            for _, row in delisted_insts.iterrows():
                id_delisted_map[row['OrderBookID']] = int(row['DeListedDate'])
        return id_delisted_map
