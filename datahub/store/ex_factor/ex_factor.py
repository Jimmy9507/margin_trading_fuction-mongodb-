from datahub.handle.ex_factor.ex_factor import HandlingExFactor
from pymongo import ASCENDING
from ..mongosave import MongoSave


class StoringExFactor(MongoSave):
    def save(self):
        factor_dict = self._hf.get_ex_factor()
        for one_dict in factor_dict:
            self._db.ex_factor.update_one(
                {"order_book_id": one_dict["order_book_id"],
                 "ex_date": one_dict["ex_date"]},
                {"$set": one_dict},
                upsert=True
            )

    def __init__(self, factor_config_dict):
        super().__init__(factor_config_dict)
        self.__create_index()
        self._hf = HandlingExFactor(factor_config_dict)

    def __create_index(self):
        self._db.ex_factor.create_index([("order_book_id", ASCENDING), ("ex_date", ASCENDING)], unique=True)
