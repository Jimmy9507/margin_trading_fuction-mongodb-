import unittest

from datahub.store.tushare.equity_info import StoringST


class TestST(unittest.TestCase):
    """
    def test_get_st(self):
        st = ei.EquityInfo()
        ret = st.get_st("000037.XSHE,000521.XSHE,000001.XSHE")
        drop_s_stock(ret)
        #print(ret.to_dict(orient="records")) #194
        ret[EQUITY_INFO.TRD_DATE] = to_datetime(ret[EQUITY_INFO.TRD_DATE])
        #ret = ret.dt.strftime("%Y%m%d")
        print(ret)
        print(type(ret.iloc[1][EQUITY_INFO.TRD_DATE]))
        print(ret[EQUITY_INFO.TRD_DATE])
        #print(st.get_st("000521.XSHE"))

    def test_duplicate(self):
        st = ei.EquityInfo()
        t1 = time.time()
        sst = st.get_st("000037.XSHE")
        t2 = time.time()
        sst2 = sst.copy()
        sst2[EQUITY_INFO.ID] = sst2[EQUITY_INFO.ID].apply(lambda x: "000001.XSHE")
        # sst.drop_duplicates(inplace=True)
        t3 = time.time()

        merged_result = [sst, sst2]

        merged = concat(merged_result)
        merged = merged.append(DataFrame([["000001.XSHE", "2016-05-13", "*ST"], ["000001.XSHE", "2016-05-13", "*ST"]],
                                columns=["order_book_id", "trade_date", "st_flag"]))
        print(merged)
        print("=========")
        merged.drop_duplicates(inplace=True)
        print(merged)
    """
    def test(self):
        a = StoringST({"inst_path": "/etc/rq/hd/Instruments/latest/china", "inst_file": ["XSHE_Instruments.csv", "XSHG_Instruments.csv"],
                   "destination": "mongodb://localhost:27017/"})
        print(a.get_latest_date())


if __name__ == '__main__':
    unittest.main()
