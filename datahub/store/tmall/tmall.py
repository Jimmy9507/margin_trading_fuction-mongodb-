from ..mongosave import MongoSave
from pymongo import ASCENDING
from ...handle.tmall.tmall import HandlingTmall

class StoringTmall(MongoSave):
    def save(self):
        stock_dict = self._ht.get_tmall_data()
        for one_dict in stock_dict:
            self._db.tmall.update_one(
                {"order_book_id": one_dict["order_book_id"],
                 "ex_date": one_dict["date"]},
                {"$set": one_dict},
                upsert=True
            )

    def __init__(self, tmall_config):
        super().__init__(tmall_config)
        self._db = self.get_client().yimian
        self.__create_index()
        self._ht = HandlingTmall(tmall_config)

    def __create_index(self):
        self._db.tmall.create_index([("order_book_id", ASCENDING), ('date', ASCENDING)], unique=True)
        self._db.tmall.create_index([("order_book_id", ASCENDING)])
        self._db.tmall.create_index([('date', ASCENDING)])
