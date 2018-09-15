from datetime import timedelta
from datahub.handle.shenwan.shenwan import HandlingShenwan
from pymongo import ASCENDING
from ..mongosave import MongoSave


TRADE_MARKET_MAP = {
    83: ".XSHG",
    90: ".XSHE"
}


class StoringShenwan(MongoSave):
    def save(self):
        shenwan_index_dict = self._hs.get_shenwan_dict()
        days = (self.end_date - self.start_date).days + 1
        date_list = [self.start_date + timedelta(days=x) for x in range(0, days)]
        #print(date_list)
        for date in date_list:
            for index in shenwan_index_dict.keys():
                inner_code = shenwan_index_dict[index]["InnerCode"]
                result_dict = self._hs.get_shenwan_by_date(date, inner_code)
                if result_dict is None:
                    continue
                for one_dict in result_dict:
                    d = dict()
                    d["index_code"] = index + ".INDX"
                    d["date"] = date
                    try:
                        d["order_book_id"] = one_dict["SecuCode"] + TRADE_MARKET_MAP[one_dict["SecuMarket"]]
                    except KeyError:
                        #print('KeyError', d, one_dict["SecuMarket"])
                        continue
                    self._db.shenwan.update_one(
                        {"index_code": d["index_code"],
                         "order_book_id": d["order_book_id"],
                         "date": d["date"]},
                        {"$set": d},
                         upsert=True
                    )


    def __init__(self, shenwan_config, cal_config):
        super().__init__(shenwan_config)
        self._db = self.get_client().shenwan
        self.__create_index()
        self._hs = HandlingShenwan(shenwan_config, cal_config)
        self.start_date = shenwan_config["start_date"]
        self.end_date = shenwan_config["end_date"]


    def __create_index(self):
        self._db.shenwan.create_index([("order_book_id", ASCENDING), ("date", ASCENDING)])
        self._db.shenwan.create_index([("index_code", ASCENDING), ("date", ASCENDING)])
