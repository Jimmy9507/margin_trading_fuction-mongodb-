import os
import pandas as pd
from dateutil.parser import parse
from pymongo import ASCENDING, DESCENDING
from ..mongosave import MongoSave


class StoringXueqiu(MongoSave):
    def save(self):
        for i in range(int(self.start_date), int(self.end_date)+1):
            for j in ["d", "w", "m"]:
                path = self.source_path + os.sep + str(i) + "_" + str(j) + ".csv"
                if os.path.isfile(path):
                    #print(path)
                    df = pd.read_csv(path)
                    df['frequency'] = j
                    df['date'] = parse(str(i))
                    df.columns = ['order_book_id', 'new_comments', 'new_followers', 'sell_actions',
                     'buy_actions', 'trading_actions', 'total_comments', 'total_followers', 'frequency', 'date']
                    xueqiu_dict = df.to_dict('record')
                    for one_dict in xueqiu_dict:
                        self._db.xueqiu.update_one(
                            {"order_book_id": one_dict["order_book_id"],
                             "date": one_dict["date"],
                             "frequency": one_dict["frequency"]},
                            {"$set": one_dict},
                            upsert=True
                        )

    def __init__(self, xueqiu_config_dict):
        super().__init__(xueqiu_config_dict)
        self.source_path = xueqiu_config_dict['source_path']
        self.start_date = xueqiu_config_dict['start_date']
        self.end_date = xueqiu_config_dict['end_date']
        self.__create_index()

    def __create_index(self):
        self._db.xueqiu.create_index([("date", ASCENDING), ("frequency", ASCENDING),
                                      ("new_comments", DESCENDING)])
        self._db.xueqiu.create_index([("date", ASCENDING), ("frequency", ASCENDING),
                                      ("new_followers", DESCENDING)])
        self._db.xueqiu.create_index([("date", ASCENDING), ("frequency", ASCENDING),
                                      ("total_followers", DESCENDING)])
        self._db.xueqiu.create_index([("date", ASCENDING), ("frequency", ASCENDING),
                                      ("total_comments", DESCENDING)])
        self._db.xueqiu.create_index([("date", ASCENDING), ("frequency", ASCENDING),
                                      ("buy_actions", DESCENDING)])
        self._db.xueqiu.create_index([("date", ASCENDING), ("frequency", ASCENDING),
                                      ("sell_actions", DESCENDING)])
        self._db.xueqiu.create_index([("date", ASCENDING), ("frequency", ASCENDING),
                                      ("trading_actions", DESCENDING)])