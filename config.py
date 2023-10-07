from typing import Union,List, Dict, Any
from enums import SyncMode
from platformdirs import user_config_path
from pathlib import Path
from dataclasses import dataclass
import json
import os

def get_config_file() -> Path:
    return user_config_path("robinhood").joinpath("profiles.json")
def config_file_exists() -> bool:
    config_file = get_config_file()

    if config_file.parent.exists() and config_file.exists():
        return True

    return False

class RobinHoodProfileEncoder(json.JSONEncoder):
    def default(this,o:Any)->dict:
        if (isinstance(o,RobinHoodProfile)):
            return o.to_json()

        return json.JSONEncoder.default(o)

@dataclass
class RobinHoodProfile:
    name: Union[str | None] = None
    source_path: Union[str|None]=None
    destination_path: Union[str|None]=None
    exclusion_filters:Union[List[str] | None] = None
    deep_comparisons:bool = False
    exclude_hidden_files:bool = False
    sync_mode:SyncMode = SyncMode.UPDATE

    def __str__(this)->str:
        return f"Source Path ....: {this.source_path}\n"\
               f"Destination Path: {this.destination_path}\n"\
               f"Deep search ....: {this.deep_comparisons}\n"\
               f"Filters ........: {this.exclusion_filters}\n"\
               f"Exclude hidden .: {this.exclude_hidden_files}"

    def to_json(this):
        return {
            "source_path": this.source_path,
            "destination_path": this.destination_path,
            "exclusion_filters": this.exclusion_filters,
            "deep_comparisons": this.deep_comparisons,
            "exclude_hidden_files": this.exclude_hidden_files
        }



class RobinHoodConfiguration:
    def __new__(cls, profile_name:Union[str|None]=None):
        if not hasattr(cls, 'instance'):
            cls.instance = super(RobinHoodConfiguration, cls).__new__(cls)

        return cls.instance

    def __init__(this, profile_name:Union[str|None]=None):
        this._cfg:Dict[str,RobinHoodProfile] = {}
        this._current_profile:Union[RobinHoodProfile|None] = None

        this.read_config_file()

        if profile_name is None:
            this._current_profile = RobinHoodProfile()
        else:
            this._current_profile = this[profile_name]

    def __getitem__(this, name:str) -> RobinHoodProfile:
        return this.get_profile(name)

    def __setitem__(this, name:str, profile:RobinHoodProfile) -> None:
        this.edit_profile(name, profile)

    def __delitem__(this, name:str) -> None:
        this.delete_profile(name)

    def add_profile(this, name:str, profile: RobinHoodProfile) -> None:
        if name in this._cfg:
            raise ValueError(f"Profile '{name}' already exists")

        this.edit_profile(name, profile)

    def edit_profile(this, name:str, profile: RobinHoodProfile) -> None:
        if (isinstance(profile,RobinHoodProfile)):
            this._cfg[name] = profile
            this.flush()
        else:
            raise TypeError(f"The given profile is not a (sub)type of RobinHoodProfile")

    def get_profile(this, name:str) -> RobinHoodProfile:
        return this._cfg[name]

    def delete_profile(this, name:str) -> None:
        del this._cfg[name]
        this.flush()

    @property
    def current_profile(this) -> RobinHoodProfile:
        return this._current_profile

    @current_profile.setter
    def current_profile(this,profile:RobinHoodProfile)->None:
        this._current_profile = profile

    def flush(this):
        file_dict={'version':"0.1","profiles":{}}

        for name,profile in this._cfg.items():
            file_dict['profiles'][name] = profile

        with open(get_config_file(),'w') as h:
            json.dump(file_dict,h,cls=RobinHoodProfileEncoder)


    def read_config_file(this):
        config_file = get_config_file()

        if (not config_file.exists()):
            os.makedirs(config_file.parent,exist_ok=True)
            return


        with open(config_file) as h:
            dict = json.load(h)

        if (len(dict)==0): return

        match dict['version']:
            case "0.1":
                if "profiles" in dict:
                    for name, settings in dict['profiles'].items():
                        this.add_profile(name,RobinHoodProfile(name=name,**settings))
            case _:
                raise ValueError(f"Unsupported version {dict['version']}")



    def get_profiles(this) -> Dict[str,RobinHoodProfile]:
        return {k:v for k,v in this._cfg.items()}