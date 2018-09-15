from os import path

from typing import List


def get_files_path(files: List[str], file_path: str):
    ret = []
    for file in files:
        ret.append(path.join(file_path, file))

    return ret
