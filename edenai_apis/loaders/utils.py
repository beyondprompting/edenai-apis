import json
import ntpath
from typing import Dict

from edenai_apis.utils.exception import ProviderException


def load_json(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError as excp:
        print(path)
        raise Exception(f"file {ntpath.basename(path)} was not found")
    return data


def check_messsing_keys(owr_dict: Dict, own_dict: Dict):
    different_keys = owr_dict.keys() - own_dict.keys()
    if len(different_keys) > 0:
        raise ProviderException(f"Setting keys missing: {', '.join(different_keys)}")
    return True
