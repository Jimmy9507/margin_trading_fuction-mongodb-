from pandas import read_csv

from datahub.common.parse import get_files_path
from datahub.common.const import FENJI
from datahub.fetch.mysql.fenji.fenji import FetchingFenji


class HandlingFenji(FetchingFenji):
    def __init__(self, fenji_config_dict):
        super().__init__(fenji_config_dict)
        fnd_file = fenji_config_dict["inst_file"]
        fnd_path = fenji_config_dict["inst_path"]
        self.fnd_file_path = get_files_path(fnd_file, fnd_path)

    def __get_all_listing_funds(self):
        all_listing_funds = set()
        for file_path in self.fnd_file_path:
            df = read_csv(file_path)
            all_listing_funds |= set(df.OrderBookID)

        return all_listing_funds

    def get_fenji_dict(self):
        all_listing_funds = self.__get_all_listing_funds()
        ret = self.__merge_fenji_mu_and_struct_info()
        for one_dict in ret:
            mu_orderbook_id = one_dict[FENJI.MU_ID]
            one_dict[FENJI.MU_LISTING] = mu_orderbook_id in all_listing_funds

            a_orderbook_id = one_dict[FENJI.A_ID]
            one_dict[FENJI.A_LISTING] = a_orderbook_id in all_listing_funds

            b_orderbook_id = one_dict[FENJI.B_ID]
            one_dict[FENJI.B_LISTING] = b_orderbook_id in all_listing_funds
        return ret

    def __merge_fenji_mu_and_fenji_ab(self):
        mu_a_b_inner_code_dict = self.__fenji_mu_a_b_inner_code_dict()
        fenji_mu_info = self._get_fenji_mu()
        fenji_a_info = self._get_fenji_a()
        fenji_b_info = self._get_fenji_b()
        merged_fenji_dict = {}
        for mu_inner_code, a_b_dict in mu_a_b_inner_code_dict.items():
            new_mu_a_b_info_dict = {}
            mu_info = fenji_mu_info.get(mu_inner_code)
            for column_name in self._mu_fund_info_properties:
                if mu_info[column_name]:
                    new_mu_a_b_info_dict[column_name] = mu_info[column_name]
            a_info = fenji_a_info.get(a_b_dict[FENJI.A_INNER_CODE])
            if not a_info:
                a_info = self._get_fenji_a_info(a_b_dict[FENJI.A_INNER_CODE])
            for column_name in self._a_fund_info_properties:
                if a_info[column_name]:
                    new_mu_a_b_info_dict[column_name] = a_info[column_name]
            b_info = fenji_b_info.get(a_b_dict[FENJI.B_INNER_CODE])
            if not b_info:
                b_info = self._get_fenji_b_info(a_b_dict[FENJI.B_INNER_CODE])
            for column_name in self._b_fund_info_properties:
                if b_info[column_name]:
                    new_mu_a_b_info_dict[column_name] = b_info[column_name]
            merged_fenji_dict[mu_inner_code] = new_mu_a_b_info_dict
        return merged_fenji_dict

    def __merge_fenji_mu_and_track_index(self):
        mu_inner_code_track_index_dict = self._get_mu_inner_code_track_index()
        merged_fenji = self.__merge_fenji_mu_and_fenji_ab()
        merged_fenji_dict = {}
        for mu_inner_code, merged_dict in merged_fenji.items():
            track_index = mu_inner_code_track_index_dict.get(mu_inner_code)
            if track_index:
                merged_dict[FENJI.TRACK_INDX_SYMBOL] = track_index
            merged_fenji_dict[mu_inner_code] = merged_dict
        return merged_fenji_dict

    def __merge_fenji_mu_and_struct_info(self):
        mu_inner_code_struct_info_dict = self._get_struct_info()
        merged_fenji = self.__merge_fenji_mu_and_track_index()
        merged_fenji_dict_list = []
        for mu_inner_code, merged_dict in merged_fenji.items():
            struct_info = mu_inner_code_struct_info_dict.get(mu_inner_code)
            """
            if not struct_info:
                continue
            """
            for column_name in self._mu_struct_info_properties:
                if struct_info and struct_info[column_name]:
                    merged_dict[column_name] = struct_info[column_name]
            merged_fenji_dict_list.append(merged_dict)
        return merged_fenji_dict_list

    def __fenji_mu_a_b_inner_code_dict(self):
        a_b_inner_code_dict = self._get_fenji_a_b_inner_code_dict()

        mu_ab_inner_code_dict_list = self._get_fenji_mu_ab_rela()
        mu_a_b_inner_code_dict = {}
        need_to_remove_mu = []
        need_to_double_check = []
        for each in mu_ab_inner_code_dict_list:
            mu_inner_code = each[FENJI.MU_INNER_CODE]
            mu_ab_dict = mu_a_b_inner_code_dict.get(mu_inner_code)
            rela_inner_code = each[FENJI.RELA_INNER_CODE]
            if not rela_inner_code:
                need_to_remove_mu.append(mu_inner_code)
                continue

            if mu_ab_dict:
                if rela_inner_code in a_b_inner_code_dict:
                    mu_ab_dict[FENJI.A_INNER_CODE] = rela_inner_code
                else:
                    mu_ab_dict[FENJI.B_INNER_CODE] = rela_inner_code
                if not mu_ab_dict.get(FENJI.A_INNER_CODE) or not mu_ab_dict.get(FENJI.B_INNER_CODE):
                    need_to_double_check.append(mu_inner_code)
            else:
                new_mu_a_b_dict = {}
                if rela_inner_code in a_b_inner_code_dict:
                    new_mu_a_b_dict[FENJI.A_INNER_CODE] = rela_inner_code
                else:
                    new_mu_a_b_dict[FENJI.B_INNER_CODE] = rela_inner_code
                mu_a_b_inner_code_dict[mu_inner_code] = new_mu_a_b_dict

        for check_one in need_to_double_check:

            mu_ab_dict = mu_a_b_inner_code_dict.get(check_one)
            double_check_done = 2
            for each in mu_ab_inner_code_dict_list:
                if each[FENJI.MU_INNER_CODE] == check_one:
                    a_or_b = each[FENJI.RELA_INNER_CODE]
                    fund_info = self._get_fenji_info(a_or_b)
                    fund_symbol = fund_info[FENJI.A_OR_B_SYMBOL]
                    if any([x in fund_symbol for x in ("进取", "B")]):
                        mu_ab_dict[FENJI.B_INNER_CODE] = a_or_b
                        double_check_done -= 1
                    elif any([x in fund_symbol for x in ("优先", "A")]):
                        mu_ab_dict[FENJI.A_INNER_CODE] = a_or_b
                        double_check_done -= 1
                    else:
                        print("No matched fenji_ab for fenji_mu {}".format(check_one))
                        need_to_remove_mu.append(check_one)

                if double_check_done == 0:
                    break

            if double_check_done != 0:
                print("Failed to merge mu and ab for mu id %s" % check_one)

        for remove_mu in need_to_remove_mu:
            print("Removed fenji_mu {}".format(remove_mu))
            mu_a_b_inner_code_dict.pop(remove_mu)

        return mu_a_b_inner_code_dict

    _mu_fund_info_properties = frozenset((
        FENJI.MU_ID,
        FENJI.MU_SYMBOL,
        FENJI.CREATION_DATE,
        FENJI.EXPIRE_DATE,
    ))

    _mu_struct_info_properties = frozenset((
        FENJI.CURR_YIELD,
        FENJI.NEXT_YIELD,
        FENJI.INTE_RULE,
        FENJI.CONV_DATE,
        FENJI.AB_PROP,
    ))

    _a_fund_info_properties = frozenset((
        FENJI.A_ID,
        FENJI.A_SYMBOL,
    ))

    _b_fund_info_properties = frozenset((
        FENJI.B_ID,
        FENJI.B_SYMBOL,
    ))

    _track_index_properties = frozenset((
        FENJI.TRACK_INDX_SYMBOL
    ))


