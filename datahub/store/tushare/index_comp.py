from multiprocessing import Process
import sys
import time

from pymongo import ASCENDING, DESCENDING

from datahub.fetch.mysql.calendar.tradedates import Calendar
from datahub.common.const import INDEX_COMP
from ..mongosave import MongoSave
from datahub.fetch.tushare.index_comp import IndexComponentsCache
from datahub.handle.tushare.index_comp import get_valid_stocks

THREAD_NUM = 20


def saving_index_by_one_date(index_comp_cache, db, id, date):
    comp = None
    try:
        comp = index_comp_cache.get_index_comp().get_index_components(id, date=str(date))
    except KeyboardInterrupt:
        print("Keyboard Interrupt")
        return False
    except:
        print("Tushare Unexpected error: ", sys.exc_info()[0])
        return False

    if comp is None:
        return True

    valid_comp = get_valid_stocks(comp)
    try:
        db.index_comp.insert_one(
            {
                INDEX_COMP.ID: id, INDEX_COMP.TRD_DATE: date,
                INDEX_COMP.COMP: valid_comp
            }
        )
    except KeyboardInterrupt:
        print("Keyboard Interrupt")
        return False
    except:
        print("Mongo Unexpected error: ", sys.exc_info()[0])
        return False

    return True


def saving_one_index(index_comp_cache, calendar, db, id, date):
    trading_dates = calendar.get_latest_trading_dates(int(date))

    for trading_date in trading_dates:
        if not saving_index_by_one_date(index_comp_cache, db, id, trading_date):
            return False

    return True


def saving_all_index_comps(index_comp_cache, calendar, db, id_date_dict, thread_id):
    print("saving thread {} starting".format(thread_id))
    for id, date in id_date_dict.items():
        if saving_one_index(index_comp_cache, calendar, db, id, date - 1):
            continue
        print("Fail to save index {} at {}".format(id, date))


def get_latest_idx_comp_in_db(db, index_id):
    return db.index_comp.find_one(
        filter={INDEX_COMP.ID: index_id},
        projection={INDEX_COMP.TRD_DATE: 1, "_id": 0},
        sort=[(INDEX_COMP.TRD_DATE, DESCENDING)]
    )


def saving_latest_index_comp(index_comp_cache, calendar, db, id_date_dict, thread_id):
    print("saving thread {} starting".format(thread_id))
    for id, date in id_date_dict.items():
        latest_recording_date = get_latest_idx_comp_in_db(db, id)
        if latest_recording_date:
            date = latest_recording_date[INDEX_COMP.TRD_DATE]
        else:
            date -= 1
        if saving_one_index(index_comp_cache, calendar, db, id, date):
            continue
        print("Fail to save index {} at {}".format(id, date))


def check_integrity_for_one_index(index_comp_cache, calendar, db, index_id, date):
    trading_dates = calendar.get_latest_trading_dates(int(date))
    print(len(trading_dates))
    for trading_date in trading_dates:
        find_one = db.index_comp.find_one(
            filter={INDEX_COMP.ID: index_id, INDEX_COMP.TRD_DATE: int(trading_date)},
            projection={INDEX_COMP.TRD_DATE: 1, "_id": 0}
        )
        if find_one:
            continue
        # print("Missing index {} at date {}".format(index_id, trading_date))
        saving_index_by_one_date(index_comp_cache, db, index_id, int(trading_date))


def check_index_comp_integrity(index_comp_cache, calendar, db, id_date_dict, thread_id):
    print("saving thread {} starting".format(thread_id))
    for id, date in id_date_dict.items():
        check_integrity_for_one_index(index_comp_cache, calendar, db, id, date - 1)


def create_index(db):
    db.index_comp.create_index(
        [(INDEX_COMP.ID, ASCENDING), (INDEX_COMP.TRD_DATE, DESCENDING)],
        unique=True
    )


def multiprocessing_process_index_comps(func, ic_cache, ic_config, calendar):
    id_date_dict_list = ic_cache.get_index_comp().get_all_index(THREAD_NUM)
    process_id = 1
    processes = []
    for id_date_dict in id_date_dict_list:
        p = Process(
            target=func,
            args=(IndexComponentsCache(ic_config), calendar, MongoSave(ic_config).get_db(),
                  id_date_dict, process_id)
        )
        p.start()
        processes.append(p)
        process_id += 1

    for p in processes:
        print("join")
        p.join()

    create_index(MongoSave(ic_config).get_db())


class StoringIndexComp(MongoSave):
    def save(self, init=False):
        if init:
            self._db.index_comp.drop()
            func = saving_all_index_comps
        else:
            func = saving_latest_index_comp

        multiprocessing_process_index_comps(func, self.ic_cache, self.ic_config, self.__calendar)
        # multiprocessing_process_index_comps(check_index_comp_integrity, self.ic_cache, self.ic_config, self.__calendar)

    def __init__(self, index_config_dict, calendar_config_dict):
        super().__init__(index_config_dict)
        self.__create_index()
        self.__calendar = Calendar(calendar_config_dict)
        self.ic_cache = IndexComponentsCache(index_config_dict)
        self.ic_config = index_config_dict

    def __create_index(self):
        create_index(self.get_db())
