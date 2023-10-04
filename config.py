from typing import Union,List, Dict
from enums import SyncMode
from platformdirs import user_config_path
from pathlib import Path
import json

class RobinHoodConfiguration:
    source_path: Union[str|None]=None
    destination_path: Union[str|None]=None
    exclusion_filters:Union[List[str] | None] = None
    deep_comparisons:bool = False
    exclude_hidden_files:bool = False
    sync_mode:SyncMode = SyncMode.UPDATE

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(RobinHoodConfiguration, cls).__new__(cls)

        return cls.instance


def get_config_file() -> Path:
    return user_config_path("robinhood").joinpath("profiles.json")
def config_file_exists() -> bool:
    config_file = get_config_file()

    if config_file.parent.exists() and config_file.exists():
        return True

    return False



def get_all_profiles() -> Dict[str,RobinHoodConfiguration]:
    profiles = {}

    if (not config_file_exists()):
        return profiles

    with open(get_config_file(),"r") as h:
        profiles = json.load(h)
        #TODO: add decoding stuff

    return profiles

