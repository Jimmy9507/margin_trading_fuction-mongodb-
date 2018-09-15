from json import load


def get_config(path):
    with open(path, mode="rt", encoding="utf8") as f:
        config_dict = load(f)
    return config_dict
