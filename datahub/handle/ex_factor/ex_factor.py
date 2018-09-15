from datahub.fetch.mysql.ex_factor.ex_factor import FetchingExFactor
from pandas import read_csv
from os import path


class HandlingExFactor(FetchingExFactor):
    def __init__(self, factor_config_dict):
        super().__init__(factor_config_dict)
        inst_file_list = factor_config_dict["inst_file"]
        inst_path = factor_config_dict["inst_path"]
        self.inst_file_path = []
        for inst_file in inst_file_list:
            self.inst_file_path.append(path.join(inst_path, inst_file))

    def get_ex_factor(self):
        result = self._get_ex_factor()
        result = self._merge_stock_code_and_exchange(result, 'stockcode')
        result = self.__rename_key(result)
        result = self.__decimal2float(result)
        return result

    def _get_instrument_sets(self):
        for file_path in self.inst_file_path:
            df = read_csv(file_path)
            if 'XSHE' in file_path:
                xshe = set(df.OrderBookID.apply(lambda x: x[:6]))
            elif 'XSHG' in file_path:
                xshg = set(df.OrderBookID.apply(lambda x: x[:6]))
        return xshe, xshg

    def _merge_stock_code_and_exchange(self, stock_dict, key):
        xshe, xshg = self._get_instrument_sets()
        new_list = list()
        for stock in stock_dict:
            stock_code = stock[key]
            if stock_code in xshe:
                stock_code += '.XSHE'
                stock[key] = stock_code
                new_list.append(stock)
            elif stock_code in xshg:
                stock_code += '.XSHG'
                stock[key] = stock_code
                new_list.append(stock)
        return new_list

    @staticmethod
    def __rename_key(stock_dict):
        for stock in stock_dict:
            stock['order_book_id'] = stock.pop('stockcode')
            stock['announcement_date'] = stock.pop('regi_date')
            stock['ex_end_date'] = stock.pop('enddate')
            stock['ex_cum_factor'] = stock.pop('cum_factor')
        return stock_dict

    @staticmethod
    def __decimal2float(stock_dict):
        for stock in stock_dict:
            ex_factor = stock['ex_factor']
            stock['ex_factor'] = float(ex_factor)
            cum_factor = stock['ex_cum_factor']
            stock['ex_cum_factor'] = float(cum_factor)
        return stock_dict