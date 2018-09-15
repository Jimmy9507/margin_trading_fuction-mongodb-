import unittest

from datahub.handle.tushare.index_comp import get_valid_stocks
from datahub.fetch.tushare.index_comp import IndexComponentsCache, IndexComponents
from datahub.store.tushare.index_comp import saving_all_index_comps
from datahub.store.tushare.index_comp import saving_latest_index_comp
from datahub.store.tushare.index_comp import check_index_comp_integrity
from datahub.store.mongosave import MongoSave

from datahub.fetch.mysql.calendar.tradedates import Calendar


def get_db():
    return MongoSave({"destination": "mongodb://localhost:27017"}).get_db()


def get_calendar():
    return Calendar({"source": "mysql://rice:rice@192.168.10.131:3306/pgenius"})


def get_index_comp_cache():
    return IndexComponentsCache({"path": "/etc/rq/hd/Instruments/latest/china",
                            "file": "INDX_Instruments.csv"})

class TestIndexComponents(unittest.TestCase):

    """
    def test_get_index_comp(self):
        ic = self.get_index_comp_cache()
        print(list(ic.get_index_components("000016.XSHG", "20051231")))
    """

    """
    def test_get_all_index(self):
        a = get_index_comp_cache().get_all_index(10)
        total = 0
        for one in a:
            total += len(one)
            print(one, len(one))
        print(len(a), total)

    def test_get_valid_stocks(self):
        print(get_valid_stocks(
            ["AAA000001.XSHE", '600015.XSHG', '600029.XSHG', '600887.XSHG', '600839.XSHG', '600602.XSHG', '600591.XSHG',
             '600036.XSHG', '600026.XSHG']))
    """

    """
    def test_saving_all_index_comps(self):
        saving_all_index_comps(get_index_comp_cache(), get_calendar(), get_db(), {"000039.XSHG": 20090109}, 1)
    """

    """
    def test_saving_latest_index_comp(self):
        saving_latest_index_comp(get_index_comp_cache(), get_calendar(), get_db(), {"000039.XSHG": 20090109}, 1)

        
    def test_check_index_comp_integrity(self):
        check_index_comp_integrity(get_index_comp_cache(), get_calendar(), get_db(), {"000039.XSHG": 20090109}, 1)
    """
    def testIndexComp(self):
        idxComp = IndexComponents({"path": "/etc/rq/hd/Instruments/latest/china",
                                   "file": "INDX_Instruments.csv"})
        print(idxComp.IdxCons(ticker='000300', intoDate=20160819, field="consID"))

if __name__ == '__main__':
    unittest.main()
