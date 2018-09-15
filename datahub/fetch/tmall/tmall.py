from pymongo import MongoClient
from datetime import timedelta


class FetchingTmall():
    def __init__(self, tmall_config):
        mongo = MongoClient(tmall_config['source'])
        db = mongo.get_default_database()
        self._collection = db.get_collection('deal_stat')

    def get_all_code(self):
        return self._collection.distinct('stock_id')

    def get_tmall_by_date(self, date, stock_id):
        projection = {'_id': 0, 'sales': 1}
        result_d = self._collection.find_one(filter={'stock_id': stock_id, 'date': date},
                                         projection = projection)
        if result_d is None:
            d = None
        else:
            d = result_d['sales']
        if d == None:
            m = None
        else:
            start_date = date - timedelta(days=30)
            result_m = self._collection.find(filter={'stock_id': stock_id,
                                                     'date': {'$gt': start_date, '$lte': date}})
            m = 0
            for i in result_m:
                m += i['sales']
        return d, m
