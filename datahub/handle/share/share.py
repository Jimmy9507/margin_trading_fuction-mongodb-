from datahub.fetch.mysql.share.share import FetchingShare


class HandlingShare(FetchingShare):
    def __init__(self, share_config_dict):
        super().__init__(share_config_dict)


    def get_share(self):
        return self._get_share()
