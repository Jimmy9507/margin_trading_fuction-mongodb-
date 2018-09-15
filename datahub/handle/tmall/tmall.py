from ...fetch.tmall.tmall import FetchingTmall
from pandas import read_csv
from os import path
from datetime import timedelta

class HandlingTmall(FetchingTmall):
    def __init__(self, tmall_config):
        super().__init__(tmall_config)
        inst_file_list = tmall_config["inst_file"]
        inst_path = tmall_config["inst_path"]
        self.inst_file_path = []
        for inst_file in inst_file_list:
            self.inst_file_path.append(path.join(inst_path, inst_file))
        start_date = tmall_config['start_date']
        end_date = tmall_config['end_date']
        days = (end_date - start_date).days + 1
        self.date_list = [start_date + timedelta(days=x, hours=16) for x in range(0, days)]
        self.stock_list = self.get_all_code()

    def get_tmall_data(self):
        stock_dict = []
        for date in self.date_list:
            for code in self.stock_list:
                d, m = self.get_tmall_by_date(date, code)
                if d == None:
                    continue
                stock_dict.append({'order_book_id': code, 'date': date,
                                   'day': d, 'month': m})
        return self._merge_stock_code_and_exchange(stock_dict, 'order_book_id')


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
            stock['date'] = stock['date'] - timedelta(hours=16)
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
