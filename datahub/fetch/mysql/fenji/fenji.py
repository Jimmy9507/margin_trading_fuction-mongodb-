from ..mysqlconnection import Mysql
from datahub.common.const import FENJI



TRADE_MARKET_MAP = {
    "1": "XSHE",
    "2": "XSHG"
}

"""
fnd_rela_info missing some fenji_a and fenji_b relationship.
If you want to get fenji_a and fenji_b info, you'd better get fenji_mu firstly, then query its corresponding fenji_a
and fenji_b by name pattern to determine which one is fenji_a, for example, fenji_a generally contains character
A or "优先"， fenji_b generally contains character B or "进取"
"""


class FetchingFenji(Mysql):
    def __init__(self, fenji_config_dict):
        super().__init__(fenji_config_dict)

    def _get_fenji_mu(self):
        with self._get_connection() as connection:
            sql = "select FUND_CODE as %s, FUNDSNAME_2 as %s, ESTAB_DATE as %s, FUND_MATU as %s, " \
                  "INNER_CODE as %s, TRADE_MKT as %s " \
                  "from fnd_gen_info where  INNER_CODE in (" \
                  "select INNER_CODE from fnd_rela_info where RELA_TYPE=14 and ISVALID = 1)" % \
                  (FENJI.MU_ID, FENJI.MU_SYMBOL, FENJI.CREATION_DATE,
                   FENJI.EXPIRE_DATE, FENJI.MU_INNER_CODE, FENJI.EXCHANGE_CODE)
            with connection.cursor() as cursor:
                cursor.execute(sql)
                return self.__get_inner_code_fenji_info_dict(
                    self.__merge_fund_code_and_exchange(cursor.fetchall(), FENJI.MU_ID),
                    FENJI.MU_INNER_CODE
                )

    def _get_fenji_a(self):
        with self._get_connection() as connection:
            sql = "select FUND_CODE as %s, FUNDSNAME_2 as %s, " \
                  "ESTAB_DATE as %s, INNER_CODE as %s, TRADE_MKT as %s " \
                  "from fnd_gen_info " \
                  "where INNER_CODE in (select INNER_CODE " \
                  "from fnd_rela_info where rela_type=16 and isvalid = 1)" % \
                  (FENJI.A_ID, FENJI.A_SYMBOL, FENJI.CREATION_DATE,
                   FENJI.A_INNER_CODE, FENJI.EXCHANGE_CODE)
            with connection.cursor() as cursor:
                cursor.execute(sql)
                return self.__get_inner_code_fenji_info_dict(
                    self.__merge_fund_code_and_exchange(cursor.fetchall(), FENJI.A_ID),
                    FENJI.A_INNER_CODE
                )

    def _get_fenji_b(self):
        with self._get_connection() as connection:
            sql = "select FUND_CODE as %s, FUNDSNAME_2 as %s, " \
                  "ESTAB_DATE as %s, INNER_CODE as %s, TRADE_MKT as %s " \
                  "from fnd_gen_info " \
                  "where INNER_CODE in (select RELA_INNER_CODE " \
                  "from fnd_rela_info where rela_type=16 and isvalid = 1)" % \
                  (FENJI.B_ID, FENJI.B_SYMBOL, FENJI.CREATION_DATE, FENJI.B_INNER_CODE, FENJI.EXCHANGE_CODE)
            with connection.cursor() as cursor:
                cursor.execute(sql)
                return self.__get_inner_code_fenji_info_dict(
                    self.__merge_fund_code_and_exchange(cursor.fetchall(), FENJI.B_ID),
                    FENJI.B_INNER_CODE
                )

    def _get_struct_info(self):
        with self._get_connection() as connection:
            sql = "select INNER_CODE as %s, TRACK_INDEX as %s, " \
                  "CUR_YLD as %s, " \
                  "NEXT_YLD as %s, " \
                  "INTR_RULE as %s, CONV_DATE as %s, " \
                  "AB_SHR_PROP as %s from STRU_FUND_INFO where isvalid = 1 " % \
                  (FENJI.MU_INNER_CODE, FENJI.TRACK_INDX_SYMBOL, FENJI.CURR_YIELD, FENJI.NEXT_YIELD,
                   FENJI.INTE_RULE, FENJI.CONV_DATE, FENJI.AB_PROP)
            with connection.cursor() as cursor:
                cursor.execute(sql)
                results = cursor.fetchall()
                inner_code_struct_info_dict = {}
                for each in results:
                    inner_code_struct_info_dict[each[FENJI.MU_INNER_CODE]] = each
                return inner_code_struct_info_dict

    def _get_mu_inner_code_track_index(self):
        with self._get_connection() as connection:
            sql = "select struct.inner_code as %s, indx_sname as %s " \
                  "from indx_gen_info idx, STRU_FUND_INFO struct " \
                  "where idx.inner_code = struct.track_index and struct.isvalid = 1" % \
                  (FENJI.MU_INNER_CODE, FENJI.TRACK_INDX_SYMBOL)
            with connection.cursor() as cursor:
                cursor.execute(sql)
                results = cursor.fetchall()
                mu_inner_code_track_index_dict = {}
                for each in results:
                    mu_inner_code_track_index_dict[each[FENJI.MU_INNER_CODE]] = each[FENJI.TRACK_INDX_SYMBOL]
                return mu_inner_code_track_index_dict

    def _get_fenji_mu_ab_rela(self):
        with self._get_connection() as connection:
            sql = "select INNER_CODE as %s, RELA_INNER_CODE as %s " \
                  "from fnd_rela_info " \
                  "where rela_type=14 and isvalid = 1 " \
                  "and INNER_CODE is not null and RELA_INNER_CODE is not null" % \
                  (FENJI.MU_INNER_CODE, FENJI.RELA_INNER_CODE)
            with connection.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchall()

    def _get_fenji_a_b_inner_code_dict(self):
        with self._get_connection() as connection:
            sql = "select INNER_CODE as %s, RELA_INNER_CODE as %s " \
                  "from fnd_rela_info where rela_type=16 and isvalid = 1 " \
                  "and INNER_CODE is not null and RELA_INNER_CODE is not null" % \
                  (FENJI.A_INNER_CODE, FENJI.B_INNER_CODE)
            with connection.cursor() as cursor:
                cursor.execute(sql)
                results = cursor.fetchall()
                a_b_inner_code_dict = {}
                for each in results:
                    a_inner_code = each[FENJI.A_INNER_CODE]
                    b_inner_code = each[FENJI.B_INNER_CODE]
                    if a_inner_code and b_inner_code:
                        a_b_inner_code_dict[a_inner_code] = b_inner_code
                return a_b_inner_code_dict

    def _get_fenji_a_info(self, inner_code):
        with self._get_connection() as connection:
            sql = "select FUND_CODE as %s, FUNDSNAME_2 as %s, " \
                  "ESTAB_DATE as %s, INNER_CODE as %s, TRADE_MKT as %s " \
                  "from fnd_gen_info " \
                  "where INNER_CODE = %s" % \
                  (FENJI.A_ID, FENJI.A_SYMBOL, FENJI.CREATION_DATE,
                   FENJI.A_INNER_CODE, FENJI.EXCHANGE_CODE, inner_code)
            with connection.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchone()

    def _get_fenji_b_info(self, inner_code):
        with self._get_connection() as connection:
            sql = "select FUND_CODE as %s, FUNDSNAME_2 as %s, " \
                  "ESTAB_DATE as %s, INNER_CODE as %s, TRADE_MKT as %s " \
                  "from fnd_gen_info " \
                  "where INNER_CODE = %s" % \
                  (FENJI.B_ID, FENJI.B_SYMBOL, FENJI.CREATION_DATE,
                   FENJI.B_INNER_CODE, FENJI.EXCHANGE_CODE, inner_code)
            with connection.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchone()

    def _get_fenji_info(self, inner_code):
        with self._get_connection() as connection:
            sql = "select FUND_CODE as %s, FUNDSNAME_2 as %s, " \
                  "ESTAB_DATE as %s, INNER_CODE as %s, TRADE_MKT as %s " \
                  "from fnd_gen_info " \
                  "where INNER_CODE = %s" % \
                  (FENJI.A_OR_B_ID, FENJI.A_OR_B_SYMBOL, FENJI.CREATION_DATE,
                   FENJI.A_OR_B_INNER_CODE, FENJI.EXCHANGE_CODE, inner_code)
            with connection.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchone()

    @staticmethod
    def __merge_fund_code_and_exchange(fund_info_dict, key):
        for fund_info in fund_info_dict:
            exchange_code = fund_info[FENJI.EXCHANGE_CODE]
            if exchange_code:
                fund_code = fund_info[key]
                fund_code += "." + TRADE_MARKET_MAP[str(exchange_code)]
                fund_info[key] = fund_code
        return fund_info_dict

    @staticmethod
    def __get_inner_code_fenji_info_dict(fund_info_list, inner_code_name):
        inner_code_fund_info_dict = {}
        for fund_info in fund_info_list:
            fund_inner_code = fund_info[inner_code_name]
            inner_code_fund_info_dict[fund_inner_code] = fund_info
        return inner_code_fund_info_dict
