# Copyright (c) 2023 Valerio AFK <afk.broadcast@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from __future__ import annotations
from typing import Union,List, Dict, Any
from enums import SyncMode
from platformdirs import user_config_path, user_cache_dir
from pathlib import Path
from dataclasses import dataclass
from hashlib import md5
import json
import os

MNEMONIC_PROGRAM_NAME:str = "RobinHood"
PROGRAM_NAME:str = MNEMONIC_PROGRAM_NAME.lower()

def get_config_file() -> Path:
    """Generates the fullpath of the profile.json configuration file"""
    return user_config_path(PROGRAM_NAME).joinpath("profiles.json")

def get_cache_file(obj:str) -> Path:
    """
    Generates a cache filename for any object by making an md5 digest from the value returned by __hash__
    It doesn't create the file. Just a path to it

    :param obj: The object to get a suitable filename for its cache
    :return: A Path pointing to the generated filename
    """

    digest = md5(str(obj).encode("ascii")).hexdigest()
    return os.path.join(user_cache_dir(PROGRAM_NAME),f"cache_{digest}.json")

def config_file_exists() -> bool:
    """
    Check if the configuration file exists
    :return: TRUE if exists, FALSE otherwise
    """
    config_file = get_config_file()

    if config_file.parent.exists() and config_file.exists():
        return True

    return False

class RobinHoodProfileEncoder(json.JSONEncoder):
    """
    A JSONEncoder for Profile Classes
    """
    def default(this,o:Any)->dict:
        if (isinstance(o,RobinHoodProfile)):
            return o.to_json()

        return json.JSONEncoder.default(o)

@dataclass
class RobinHoodProfile:
    """
    Contains information about user profiles/preferences
    """
    name: Union[str | None] = None
    source_path: Union[str|None]=None
    destination_path: Union[str|None]=None
    exclusion_filters:Union[List[str] | None] = None
    deep_comparisons:bool = False
    exclude_hidden_files:bool = False
    on_completion:str = "NOTHING"
    sync_mode:SyncMode = SyncMode.UPDATE

    def __str__(this)->str:
        return f"Source Path ....: {this.source_path}\n"\
               f"Destination Path: {this.destination_path}\n"\
               f"Deep search ....: {this.deep_comparisons}\n"\
               f"Filters ........: {this.exclusion_filters}\n"\
               f"Exclude hidden .: {this.exclude_hidden_files}\n"\
               f"On completion ..: {this.on_completion}"

    def to_json(this):
        return {
            "source_path": this.source_path,
            "destination_path": this.destination_path,
            "exclusion_filters": this.exclusion_filters,
            "deep_comparisons": this.deep_comparisons,
            "exclude_hidden_files": this.exclude_hidden_files,
            "on_completion": this.on_completion
        }

    def clear_cache(this):

        paths = [this.source_path, this.destination_path]

        for p in paths:
            if p is not None:
                try:
                    f = get_cache_file(p)
                    os.unlink(f)
                except FileNotFoundError:
                    ...



class RobinHoodConfiguration:
    """
    Singleton class representing the configuration file of the program
    """
    def __new__(cls, profile_name:Union[str|None]=None):
        """
        This overridden __new__ method makes the singleton design pattern in Python
        :param profile_name: In case the singleton needs to be instatianted (when the program starts),
                             it can load the specified profile. This parameter is provided to the __init__ method
        """
        if not hasattr(cls, 'instance'):
            cls.instance = super(RobinHoodConfiguration, cls).__new__(cls)

        return cls.instance

    def __init__(this, profile_name:Union[str|None]=None):
        """
        Initialise the configuration taken from the specific profile (if specified)
        :param profile_name: The name of the profile to load. If None, the object will represent a new/empty profile
        """

        # Dictionary of profiles
        this._cfg:Dict[str,RobinHoodProfile] = {}

        # Current profile
        this._current_profile:Union[RobinHoodProfile|None] = None

        # Reads the JSON config file
        this.read_config_file()

        # If a profile is not specified
        if profile_name is None:
            # it makes an empty profile
            this._current_profile = RobinHoodProfile()
        else:
            # else it assign the specified profile to the current_profile
            this._current_profile = this[profile_name]

    # getitem/setitem allows to get other profile in a dictionary-style fashion
    def __getitem__(this, name:str) -> RobinHoodProfile:
        return this.get_profile(name)

    def __setitem__(this, name:str, profile:RobinHoodProfile) -> None:
        this.edit_profile(name, profile)

    # Similarly, dictionary style profile deletion
    def __delitem__(this, name:str) -> None:
        this.delete_profile(name)

    def add_profile(this, name:str, profile: RobinHoodProfile) -> None:
        """
        Adds a new profile to the configuration file.
        If it already exists, it raises an expection
        :param name: Name of the profile
        :param profile: Profile to add
        """
        if name in this._cfg:
            raise ValueError(f"Profile '{name}' already exists")

        this.edit_profile(name, profile)

    def edit_profile(this, name:str, profile: RobinHoodProfile) -> None:
        """
        Applies changes to an existing profile
        :param name: The profile to be edited
        :param profile: The edited profile
        :return:
        """
        if (isinstance(profile,RobinHoodProfile)):
            this._cfg[name] = profile
            this.flush()
        else:
            raise TypeError(f"The given profile is not a (sub)type of RobinHoodProfile")

    def get_profile(this, name:str) -> RobinHoodProfile:
        """
        Same thing as __getitem__

        :param name: The profile to be returned
        :return: The specified profile
        """
        return this._cfg[name]

    def delete_profile(this, name:str) -> None:
        """
        Same thing as __delitem__

        :param name: Name of the profile to delete
        """

        del this._cfg[name]
        this.flush()

    @property
    def current_profile(this) -> RobinHoodProfile:
        """
        Returns the current profile
        :return: The current  profile
        """
        return this._current_profile

    @current_profile.setter
    def current_profile(this,profile:RobinHoodProfile)->None:
        """
        Sets the current profile with another one

        :param profile: The profile to be set as the new current profile
        """
        this._current_profile = profile

    def flush(this) -> None:
        """
        Saves the configuration profiles and other metadata to disk
        """
        file_dict={'version':"0.1","profiles":{}}

        for name,profile in this._cfg.items():
            file_dict['profiles'][name] = profile

        # Let's make a new config file.
        # If it already exists, it'll be simply overwritten
        with open(get_config_file(),'w') as h:
            json.dump(file_dict,h,cls=RobinHoodProfileEncoder)


    def read_config_file(this) -> None:
        """
        Loads the configuration file from disk
        """

        # Gets the configuration fullpath
        config_file = get_config_file()

        # If the config file doesn't exist
        # Containing folder(s) will be created
        if (not config_file.exists()):
            os.makedirs(config_file.parent,exist_ok=True)
            return

        # Reads the config file and decode the json
        with open(config_file,"r") as h:
            dict = json.load(h)

        # if the file is empty or contains no information, let's return
        if (len(dict)==0): return

        # Manages specific cases in case of config files generated from previous versions
        match dict['version']:
            case "0.1":
                # if the key profiles exists
                if "profiles" in dict:
                    # for each profile, I create a new RobinHoodProfile object with the parameters contained within
                    # the given dictionary
                    for name, settings in dict['profiles'].items():
                        this.add_profile(name,RobinHoodProfile(name=name,**settings))
            case _:
                raise ValueError(f"Unsupported version {dict['version']}")



    def get_profiles(this) -> Dict[str,RobinHoodProfile]:
        """
        Returns all the profiles

        :return: A Dictionary containing all the profiles. The key is given by the name of the profile, the value
                 is a RobinHoodProfile object
        """
        return {k:v for k,v in this._cfg.items()}