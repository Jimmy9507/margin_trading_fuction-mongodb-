from datahub.handle.share.share import HandlingShare
from pymongo import ASCENDING
from ..mongosave import MongoSave
from pandas import read_csv
from os import path


class StoringShare(MongoSave):
    def save(self):
        result = self._hf.get_share()
        for one_dict in result:
            one_dict = self._merge_stock_code_and_exchange(one_dict, 'A_STOCKCODE')
            one_dict = self.__decimal2int(one_dict)
            one_dict = self.__rename_key(one_dict)
            self._db.share.update_one(
                {"order_book_id": one_dict["order_book_id"],
                 "date": one_dict["date"]},
                {"$set": one_dict},
                upsert=True
            )
            self._hf.connection.close()

    def __init__(self, share_config_dict):
        super().__init__(share_config_dict)
        self.__create_index()
        self._hf = HandlingShare(share_config_dict)
        inst_file_list = share_config_dict["inst_file"]
        inst_path = share_config_dict["inst_path"]
        self.inst_file_path = []
        for inst_file in inst_file_list:
            self.inst_file_path.append(path.join(inst_path, inst_file))
        self.xshe, self.xshg = self._get_instrument_sets()

    def __create_index(self):
        self._db.share.create_index([("order_book_id", ASCENDING), ("date", ASCENDING)], unique=True)
        self._db.share.create_index([("order_book_id", ASCENDING)])
        self._db.share.create_index([("date", ASCENDING)])

    def _get_instrument_sets(self):
        for file_path in self.inst_file_path:
            df = read_csv(file_path)
            if 'XSHE' in file_path:
                xshe = set(df.OrderBookID.apply(lambda x: x[:6]))
            elif 'XSHG' in file_path:
                xshg = set(df.OrderBookID.apply(lambda x: x[:6]))
        return xshe, xshg


    def _merge_stock_code_and_exchange(self, stock_dict, key):
        stock_code = stock_dict[key]
        if stock_code in self.xshe:
            stock_code += '.XSHE'
            stock_dict[key] = stock_code
        else:
            stock_code += '.XSHG'
            stock_dict[key] = stock_code
        return stock_dict

    @staticmethod
    def __rename_key(stock_dict):
        stock_dict['order_book_id'] = stock_dict.pop('A_STOCKCODE')
        stock_dict['total_shares'] = stock_dict.pop('TOTAL')
        stock_dict['date'] = stock_dict.pop('CHANGEDATE')
        stock_dict['a_cir_shares'] = stock_dict.pop('FL_ASHR')
        stock_dict['a_man_cir_shares'] = stock_dict.pop('MNG_FL')
        stock_dict['a_non_cir_shares'] = stock_dict.pop('TOT_NONFL')
        stock_dict['a_total_shares'] = stock_dict['total_shares'] - stock_dict.pop('B_SHR') - stock_dict.pop('H_SHR') - \
                                       stock_dict.pop('S_SHR') - stock_dict.pop('N_SHR')
        stock_dict['a_non_cir_shares'] = stock_dict['a_total_shares'] - stock_dict['a_cir_shares']
        return stock_dict

    @staticmethod
    def __decimal2int(stock_dict):
        total = stock_dict['TOTAL']
        if total is None:
            stock_dict['TOTAL'] = 0
        else:
            stock_dict['TOTAL'] = int(total)
        fl_ashr = stock_dict['FL_ASHR']
        if fl_ashr is None:
            stock_dict['FL_ASHR'] = 0
        else:
            stock_dict['FL_ASHR'] = int(fl_ashr)
        mng_fl = stock_dict['MNG_FL']
        if mng_fl is None:
            stock_dict['MNG_FL'] = None
        else:
            stock_dict['MNG_FL'] = int(mng_fl)
        tot_nonfl = stock_dict['TOT_NONFL']
        if tot_nonfl is None:
            stock_dict['TOT_NONFL'] = 0
        else:
            stock_dict['TOT_NONFL'] = int(tot_nonfl)
        b_shr = stock_dict['B_SHR']
        if b_shr is None:
            stock_dict['B_SHR'] = 0
        else:
            stock_dict['B_SHR'] = int(b_shr)
        h_shr = stock_dict['H_SHR']
        if h_shr is None:
            stock_dict['H_SHR'] = 0
        else:
            stock_dict['H_SHR'] = int(h_shr)
        s_shr = stock_dict['S_SHR']
        if s_shr is None:
            stock_dict['S_SHR'] = 0
        else:
            stock_dict['S_SHR'] = int(s_shr)
        n_shr = stock_dict['N_SHR']
        if n_shr is None:
            stock_dict['N_SHR'] = 0
        else:
            stock_dict['N_SHR'] = int(n_shr)
        return stock_dict