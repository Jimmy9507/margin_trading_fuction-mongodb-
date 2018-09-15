import re
from decimal import Decimal

from pymongo import ASCENDING

from datahub.common.const import FENJI
from datahub.common.codec import encode_decimal
from datahub.handle.fenji.fenji import HandlingFenji
from ..mongosave import MongoSave

REG_REMOVE_ZEROS = re.compile(r"(\d)\.(\d*?[^0]?)(0+!?)%$")

REG_REMOVE_WHITESPACE = re.compile(r"\s+")


class StoringFenji(MongoSave):
    def save(self):
        fenji_dict = self._hf.get_fenji_dict()
        for one_dict in fenji_dict:
            validated_dict = self.__validate_fenji_dict(one_dict)
            self._db.fenji.update_one(
                {FENJI.MU_ID: validated_dict[FENJI.MU_ID]},
                {"$set": validated_dict},
                upsert=True
            )

    def __init__(self, fenji_config_dict):
        super().__init__(fenji_config_dict)
        self.__create_index()
        self._hf = HandlingFenji(fenji_config_dict)

    def __create_index(self):
        self._db.fenji.create_index([(FENJI.MU_ID, ASCENDING)], unique=True)

    @staticmethod
    def __validate_fenji_dict(fenji_dict):
        for key, value in fenji_dict.items():
            if isinstance(value, Decimal):
                fenji_dict[key] = encode_decimal(value)

        inte_rule = fenji_dict.get(FENJI.INTE_RULE)
        if inte_rule:
            # remove zeros before character %
            inte_rule = REG_REMOVE_ZEROS.sub(StoringFenji.zeros_repl, inte_rule)

            # remove white space character in inte_rule
            re.sub(r"\s+", "", inte_rule)
            inte_rule = REG_REMOVE_WHITESPACE.sub("", inte_rule)

            if '╳' in inte_rule:
                inte_rule = inte_rule.replace('╳', '*')
            elif '×' in inte_rule:
                inte_rule = inte_rule.replace('×', '*')
            elif 'x' in inte_rule:
                inte_rule = inte_rule.replace('x', '*')

            fenji_dict[FENJI.INTE_RULE] = inte_rule

        return fenji_dict

    @staticmethod
    def zeros_repl(matchobj):
        ret = matchobj.group(1)
        internal_digits = matchobj.group(2)
        if len(internal_digits) > 0:
            ret += "." + internal_digits
        return ret + "%"
