import datetime

from pandas import read_csv
from pymongo import ASCENDING, DESCENDING

from datahub.common.parse import get_files_path
from datahub.store.update.update import update_record
from ..mongosave import MongoSave
from datahub.fetch.tushare import equity_info as ei
from datahub.common.const import EQUITY_INFO
from datahub.handle.tushare.equity_info import drop_s_stock, convert_str_date_to_datetime, remove_duplicate

MAX_REQ_NUM = 10


class StoringST(MongoSave):
    def save(self, first=True):
        if first:
            self._db.st_stocks.drop()
            all_inst_set = self.__merge_instruments(MAX_REQ_NUM)
            for one_inst in all_inst_set:
                st_df = self.ei.get_st(one_inst)
                self.__save_st_df(st_df)
        else:
            all_inst_set = self.__merge_instruments(1)
            for one_inst in all_inst_set:
                self.save_latest_st(one_inst)

        latest_date = self.get_latest_date()
        update_record('st_stocks', latest_date)

    def __init__(self, st_config_dict):
        super().__init__(st_config_dict)
        self.ei = ei.EquityInfo()
        inst_file_list = st_config_dict["inst_file"]
        inst_path = st_config_dict["inst_path"]
        self.inst_file_path = get_files_path(inst_file_list, inst_path)
        self.__create_index(self._db)

    def __get_all_instruments(self):
        all_inst_set = set()
        for file_path in self.inst_file_path:
            df = read_csv(file_path)
            all_inst_set |= set(df.OrderBookID)
        return all_inst_set

    @staticmethod
    def __create_index(db):
        db.st_stocks.create_index([(EQUITY_INFO.ID, ASCENDING),
                                   (EQUITY_INFO.TRD_DATE, DESCENDING)],
                                  unique=True)
        db.st_stocks.create_index([(EQUITY_INFO.ID, ASCENDING)], unique=False)
        db.st_stocks.create_index([(EQUITY_INFO.TRD_DATE, ASCENDING)], unique=False)

    @staticmethod
    def __validate_st_df(st_df):
        drop_s_stock(st_df)
        remove_duplicate(st_df)
        convert_str_date_to_datetime(st_df)

    def __save_st_df(self, st_df):
        if not st_df.empty:
            self.__validate_st_df(st_df)
            if not st_df.empty:
                self._db.st_stocks.insert_many(st_df.to_dict(orient="record"))

    def __merge_instruments(self, num):
        if num < 1:
            raise ValueError("Num less than 1")
        all_insts = self.__get_all_instruments()
        count = 0
        merged_str = ""
        merged_str_list = list()
        for inst in all_insts:
            count += 1
            if count < num:
                merged_str += inst + ","
                continue
            merged_str += inst
            count = 0
            merged_str_list.append(merged_str)
            merged_str = ""
        if count > 0:
            merged_str_list.append(merged_str)
        return merged_str_list

    def get_latest_st_in_db(self, stock_id):
        return self._db.st_stocks.find_one(
            filter={EQUITY_INFO.ID: stock_id},
            sort=[(EQUITY_INFO.TRD_DATE, DESCENDING)]
        )

    def save_latest_st(self, stock_id):
        latest_in_db = self.get_latest_st_in_db(stock_id)
        if latest_in_db:
            trade_dt = latest_in_db[EQUITY_INFO.TRD_DATE]
            trade_dt += datetime.timedelta(days=1)
            trade_date_str = trade_dt.strftime("%Y%m%d")
            st_df = self.ei.get_specified_period_st(stock_id, trade_date_str)
        else:
            st_df = self.ei.get_st(stock_id)

        self.__save_st_df(st_df)

    def get_latest_date(self):
        result = self._db.st_stocks.find_one(
            sort=[(EQUITY_INFO.TRD_DATE, DESCENDING)],
            projection={EQUITY_INFO.TRD_DATE: 1, "_id": 0}
        )
        return int(result.get(EQUITY_INFO.TRD_DATE).strftime("%Y%m%d"))
