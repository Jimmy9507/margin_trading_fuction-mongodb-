from datahub.fetch.mysql.shenwan.shenwan import FetchingShenwan
import os
from bson import json_util
import json


class HandlingShenwan(FetchingShenwan):
    def __init__(self, shenwan_config_dict, cal_config):
        super().__init__(shenwan_config_dict)
        self.cal_config = cal_config
        file = shenwan_config_dict['file']
        path = shenwan_config_dict['path']
        self.file_path = path + os.sep + file

    def get_shenwan_dict(self):
        with open(self.file_path) as infile:
            data = json.load(infile, object_hook=json_util.object_hook)
        return data
