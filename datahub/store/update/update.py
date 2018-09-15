from pymongo import MongoClient

_update = None


def init_update(config):
    dest = config['destination']
    mongo = MongoClient(dest)
    global _update
    _update = mongo.datahub.update


def update_record(collection_name: str, date: int):
    global _update
    if not _update:
        raise RuntimeError("Update config does not be set.")
    _update.update_one(
        filter={"name": collection_name},
        update={"$set": {"latest-date": date, "name": collection_name}},
        upsert=True
    )
