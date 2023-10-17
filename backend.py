#!python

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
import os.path
from typing import Union, List, Iterable, Callable, Dict, Tuple, Any
from dataclasses import dataclass
from abc import ABC, abstractmethod
from enums import SyncMode, SyncStatus, ActionType, ActionDirection
from filesystem import FileType, FileSystemObject, FileSystem, fs_auto_determine, mkdir, convert_to_bytes
from filesystem import AbstractPath
from file_filters import UnixPatternExpasionFilter, RemoveHiddenFileFilter, FilterSet, FileFilter
from datetime import datetime
from rclone_python.rclone import copy, delete
from config import RobinHoodProfile
from platformdirs import site_cache_path
from fnmatch import fnmatch
import subprocess
import re
import rclone_python

# List of launched subprocesses that is populated by the Popen wrapper function (see below)
_POPEN = []


#######################################################################################################################
# The code within this comment bracket has been taken from rclone-python library and improved to make it better
# for this project. This code was released under MIT Licence and I am stating its licence agreement below:
#
# MIT License
#
# Copyright (c) 2022 Johannes Gundlach
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
def improved_extract_rclone_progress(buffer: str) -> Tuple[bool, Union[Dict[str, Any], None]]:
    # matcher that checks if the progress update block is completely buffered yet (defines start and stop)
    # it gets the sent bits, total bits, progress, transfer-speed and eta
    reg_transferred = re.findall(
        r"Transferred:\s+(\d+(.\d+)? \w+) \/ (\d+.\d+ \w+), (\d{1,3})%, (\d+(.\d+)? \w+\/\w+), ETA (\S+)",
        # fixed pattern
        buffer,
    )

    def _extract_value_unit(pattern: str) -> Tuple[float, str]:
        try:
            a, b = pattern.strip().split(" ")
        except ValueError:
            a = pattern.strip()
            b = "B/s"

        a = float(a)
        return a, b

    if reg_transferred:  # transferred block is completely buffered
        # get the progress of the individual files
        # matcher gets the currently transferring files and their individual progress
        # returns list of tuples: (name, progress, file_size, unit)
        prog_transferring = []
        prog_regex = re.findall(
            r"\* +(.+):[ ]+(\d{1,3})% \/(\d+(.\d+)?)([a-zA-Z]+),", buffer
        )
        for item in prog_regex:
            prog_transferring.append(
                (
                    item[0],
                    int(item[1]),
                    float(item[2]),
                    # the suffix B of the unit is missing for subprocesses
                    item[4] + "B",
                )
            )

        out = {"prog_transferring": prog_transferring}
        sent_bits, _, total_bits, progress, transfer_speed_str, _, eta = reg_transferred[0]
        out["progress"] = float(progress.strip())

        out["total_bits"], out["unit_total"] = _extract_value_unit(total_bits)
        out["sent_bits"], out["unit_sent"] = _extract_value_unit(sent_bits)
        out["transfer_speed"], out["transfer_speed_unit"] = _extract_value_unit(transfer_speed_str)

        out["eta"] = eta

        return True, out

    else:
        return False, None


#######################################################################################################################
# End of rclone-python modified code
#######################################################################################################################

def _wrap_Popen(fun: Callable) -> Callable:
    '''
    This function wraps the function Popen such that each launched subprocess is stored in a list.
    This is necessary to effectively implement a "stop" function to kill any pending jobs performed by rclone
    :param fun: Any function (in our case, it will always be Popen)
    :return: A function that invokes Popen and stores the details of the launched subprocess
    '''

    def _invoke(*args, **kwargs):
        process = fun(*args, **kwargs)
        _POPEN.append(process)
        return process

    return _invoke


# Popne is replaced with the wrapper above
subprocess.Popen = _wrap_Popen(subprocess.Popen)

# The extract_rclone_progress within rclone_python is replaced with my version
rclone_python.rclone.utils.extract_rclone_progress = improved_extract_rclone_progress

@dataclass(frozen=True)
class ActionProgress:
    filename:str = ""
    progress: float = 0
    transfer_speed:str= ""
    timestamp:datetime = datetime.now()

@dataclass(frozen=True)
class SyncProgress():
    prog_transferring:[List[Any]|None] = None
    progress: float = 0
    total_bits: float = 0
    sent_bits: float = 0
    unit_sent: str = ""
    unit_total: str = ""
    transfer_speed: float = 0
    transfer_speed_unit: str = ""
    eta: str = ""
    timestamp: datetime = datetime.now()

    @property
    def bytes_transferred(this):
        return convert_to_bytes(this.sent_bits, this.unit_sent)

    @property
    def bytes_total(this):
        return convert_to_bytes(this.total_bits, this.unit_total)



class SyncEvent:

    def __init__(this, value: Any = None, *, processed: Union[int | None] = None, total: Union[int | None] = None):
        this.value = value
        this.processed = processed
        this.total = total


class SyncComparisonEvent(SyncEvent):

    def __init__(this, src_path=None, dest_path=None):
        super().__init__((src_path, dest_path))

    @property
    def source_path(this):
        return this.value[0]

    @property
    def destination_path(this):
        return this.value[1]


def _get_trigger_fn(eventhandler: Union[SyncEvent | None] = None) -> Callable[[str, SyncEvent], None]:
    def _trigger(mtd: str, e: SyncEvent) -> None:
        if isinstance(eventhandler, RobinHoodBackend):
            try:
                fn = getattr(eventhandler, mtd)
                fn(e)
            except AttributeError:
                ...  # event does not exist

    return _trigger


class AbstractSyncAction(ABC):

    def __init__(this,
                 a: FileSystemObject,
                 b: FileSystemObject,
                 direction: Union[ActionDirection | None] = None):

        this.a = a
        this.b = b
        this.direction = direction
        this._status = SyncStatus.NOT_STARTED

    @property
    def status(this) -> SyncStatus:
        return this._status

    @status.setter
    def status(this, value:SyncStatus) -> None:
        this._status = value

    @abstractmethod
    def apply_action(this, show_progress=False, eventhandler: [SyncEvent | None] = None) -> None:
        ...


class SyncAction(AbstractSyncAction):
    def __init__(this,
                 a: FileSystemObject,
                 b: FileSystemObject,
                 action_type: ActionType = ActionType.NOTHING,
                 direction: Union[ActionDirection | None] = None,
                 timeout=60
                 ):

        super().__init__(a=a,b=b,direction=direction)
        this.type = action_type
        this._update = None
        this._timeout = timeout



    def __action_type_str(this) -> str:
        match (this.type):
            case ActionType.NOTHING:
                return '-'
            # case ActionType.MKDIR:
            #     return 'D'
            case ActionType.COPY:
                return '*'
            case ActionType.UPDATE:
                return '+'
            case ActionType.DELETE:
                return 'x'

    def __str__(this) -> str:
        action_type = this.__action_type_str()

        if (this.type != ActionType.NOTHING):
            action_type = str(this.direction) + action_type

        return f"{this.a} {action_type} {this.b}"

    def __repr__(this) -> str:
        return str(this)

    @property
    def get_one_path(this) -> FileSystemObject:
        return this.a if this.b is None else this.b

    def apply_action(this, show_progress=False, eventhandler: [SyncEvent | None] = None) -> None:
        _trigger = _get_trigger_fn(eventhandler)

        def _update_internal_status(d: Dict):
            update = _parse_rclone_progress([this],this.direction,d)
            _trigger("on_synching", SyncEvent(update))

        match this.type:
            # case ActionType.MKDIR:
            #     mkdir(this.get_one_path)
            case ActionType.DELETE:
                try:
                    p = this.b if this.direction == ActionDirection.SRC2DST else this.a
                    delete(p.absolute_path)
                except Exception:
                    this._status = SyncStatus.FAILED
            case ActionType.UPDATE | ActionType.COPY:
                x = this.a.absolute_path
                y = this.b.containing_directory

                if (this.direction == ActionDirection.DST2SRC):
                    x = this.b.absolute_path
                    y = this.a.containing_directory

                this._status = SyncStatus.IN_PROGRESS
                copy(x, y, show_progress=show_progress, listener=_update_internal_status, args=['--use-mmap', '--no-traverse'])

        this._check_success()

        _trigger("on_synching", SyncEvent(this))

    def _check_success(this) -> None:

        if this.type == ActionType.NOTHING:
            success = True
        else:
            x = this.a
            y = this.b

            x.update_information()
            y.update_information()

            if (this.direction == ActionDirection.DST2SRC):
                x, y = y, x

            match this.type:
                # case ActionType.MKDIR:
                #     success = y.exists
                case ActionType.UPDATE | ActionType.COPY:
                    success = y.exists and (x.size == y.size)
                case ActionType.DELETE:
                    success = not y.exists
                case _:
                    success = True

        this._status = SyncStatus.SUCCESS if success else SyncStatus.FAILED

    def get_update(this) -> Union[SyncProgress | None]:
        return this._update

    @property
    def update(this):
        return this.get_update()

    @update.setter
    def update(this, value):
        this._update = value


class SynchingManager():
    """
    This class manages the application of each action between source and destination directories
    """

    def __init__(this, source:FileSystem, destination:FileSystem):
        this.source = source
        this.destination = destination

        this._changes = []

    def __iter__(this) -> Iterable:
        return iter(this._changes)
    def __len__(this) -> int:
        return len(this._changes)

    def __getitem__(this, item:int) -> AbstractSyncAction:
        return this._changes[item]

    def index_of(this, action:AbstractSyncAction) -> int:
        """
        Returns the position of the provided action within the manager

        :param action: the action to retrieve its position
        """

        return this._changes.index(action)

    def clear(this) -> None:
        this._changes = []

    def remove_action(this, action:AbstractSyncAction) -> None:
        this._changes.remove(action)

    def add_action(this, action: AbstractSyncAction) -> None:
        src_path = this.source.root
        dst_path = this.destination.root

        # Check if the paths in the provided action are rooted properly in both source and dest directories
        if not AbstractPath.is_root_of(action.a.absolute_path,src_path): #this._root_source.is_under_root(action.a.absolute_path):
            raise ValueError(f"The file '{action.a.relative_path} 'is not in '{src_path}'")

        if not AbstractPath.is_root_of(action.b.absolute_path,dst_path): #this._root_destination.is_under_root(action.b.absolute_path):
            raise ValueError(f"The file '{action.b.relative_path} 'is not in '{dst_path}'")

        this._changes.append(action)


    def sort(this,**kwargs) -> None:
        this._changes.sort(**kwargs)

    def apply_changes(this, show_progress:bool=False, eventhandler: [SyncEvent | None] = None) -> None:
        for x in this._changes:
            if x.status != SyncStatus.SUCCESS:
                x.apply_action(show_progress, eventhandler)
                this.flush_action(x)

        this._flush_cache()


    def _flush_cache(this) -> None:
        """
        Flushes the file system cache into the disk (JSON file) after changes have been applied
        """
        this.source.flush_file_object_cache()
        this.destination.flush_file_object_cache()

    def flush_action(this, action: SyncAction) -> None:
        """
        Flush action changes (if successful) within the directory trees.
        For example, if an action creates a file in the destination, it needs to be updated with such information

        :param action: Action to flush in the file system cache
        """

        if action.status != SyncStatus.SUCCESS:
            return

        match action.type:
            case ActionType.COPY | ActionType.UPDATE:
                side = this.destination if action.direction == ActionDirection.SRC2DST else this.source
                fso = action.b if action.direction == ActionDirection.SRC2DST else action.a

                side.set_file(fso.fullpath, fso)
            case ActionType.DELETE:
                side = this.destination if action.direction == ActionDirection.SRC2DST else this.source
                fso = action.b if action.direction == ActionDirection.SRC2DST else action.a

                side.set_file(fso.fullpath, None)



class BulkCopySynchingManager(SynchingManager):

    def __init__(this,
                 source:FileSystem,
                 destination:FileSystem,
                 direction: ActionDirection):

        super().__init__(source, destination)

        this._direction = direction
        this._actions_in_progress = []

    def add_action(this, action: SyncAction) -> None:

        if action.type not in [ActionType.COPY, ActionType.UPDATE]:
            raise ValueError("The provided action is not copying or updating a file")

        if (action.direction != this._direction):
            raise ValueError("The provided action is towards a different synching direction")

        super().add_action(action)

    def apply_changes(this, show_progress:bool=False, eventhandler: [SyncEvent | None] = None) -> None:
        '''
        Applies bulk copy/update actions to destionation directory
        :param show_progress: A boolean representing whether to show the progress bar or not (useful for batch processes)
        :param eventhandler: A class extending RobinHoodBackend (where events will be passed to)
        '''

        # Gets the function that facilitate the triggering of events in the eventhandler (if provided)
        _trigger = _get_trigger_fn(eventhandler)

        def _update_internal_status(d:Dict) -> None:
            '''
            This internal function is used as callback function for the rclone_python copy function
            The updates coming from there are formatted and passed to the right SyncAction object
            :param d: Dictionary of updates as provided by rclone
            '''


            # Creates an object to format the dictionary provided by rclone_python with the current transfer update
            sync_update = _parse_rclone_progress(this._changes, this._direction,d)

            for current_action in sync_update.prog_transferring:
                # Let's check if this action is a new one (this means that we are either at the very
                # beginning or an action finished (either successfully or not)

                if current_action not in this._actions_in_progress:
                    # As this is an action that just started, its status is updated
                    current_action.status = SyncStatus.IN_PROGRESS
                    # And gets inside the club of actions in progress
                    this._actions_in_progress.append(current_action)


            for x in this._actions_in_progress:
                # if some actions have a time before the current_time value, it means that it doesn't
                # appear in the stdout of rclone, ie it's done (no matter if it's successful or not)
                if (x.update.timestamp<sync_update.timestamp):
                    x._check_success()
                    # Notify that this action is concluded
                    _trigger("on_synching",SyncEvent(x))

                    #Flush changes into file system
                    this.flush_action(x)

            # Filter out all the terminated actions
            this._actions_in_progress = [x for x in this._actions_in_progress if x.status == SyncStatus.IN_PROGRESS]

            # Notify that these actions are still in progress and send them as a list
            _trigger("on_synching",SyncEvent(sync_update))



        tmp_dir = site_cache_path()
        tmp_fname = f"rh_sync_tmp_{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.txt"

        path = os.path.join(tmp_dir, tmp_fname)


        with open(path, "w") as handle:
            for x in this:
                fso = x.a if this._direction == ActionDirection.SRC2DST else x.b
                handle.write(f"{fso.relative_path}\n")


        a = this.source.root
        b = this.destination.root

        if this._direction == ActionDirection.DST2SRC:
            a,b=b,a

        copy(a,
             b,
             show_progress=show_progress,
             listener=_update_internal_status,
             args=['--files-from', path,'--no-check-dest', '--no-traverse'])

        # Better double-checking again when it's done if everything has been copied successfully
        for itm in this:
            if itm.status in [SyncStatus.IN_PROGRESS, SyncStatus.NOT_STARTED]:
                itm._check_success()
                _trigger("on_synching", SyncEvent(itm))
                this.flush_action(itm)

        this._flush_cache()

class RobinHoodBackend(ABC):
    '''This class manages the communication between the backend and frontend
    It contains a series of events that are triggered when they occur
    before/during/after comparing/synching two directories
    '''
    @abstractmethod
    def before_comparing(this, event: SyncEvent) -> None:
        ...

    @abstractmethod
    def on_comparing(this, event: SyncEvent) -> None:
        ...

    @abstractmethod
    def after_comparing(this, event: SyncEvent) -> None:
        ...

    @abstractmethod
    def before_synching(this, event: SyncEvent) -> None:
        ...

    @abstractmethod
    def on_synching(this, event:SyncEvent) -> None:
        ...

    @abstractmethod
    def after_synching(this, event: SyncEvent) -> None:
        ...

def find_dedupe(path: Union[str | FileSystem],
                        eventhandler: [RobinHoodBackend | None] = None
                        ) -> Iterable[SyncAction]:

    """
    Finds deduplicates files by matching hashes. This function is done without using `rclone dedupe` that is extremely
    slow. It matches the checksum of files only when their sizes is the same.

    :param path: Root path where to find deduplicate files
    :param eventhandler:
    :return:
    """

    _trigger = _get_trigger_fn(eventhandler)
    _trigger("before_comparing", SyncEvent(path))

    if (type(path) == str):
        fs = fs_auto_determine(path, True)
        fs.cached = True

    fs.load()

    actions = []
    size_organiser = {}

    for fso in fs.walk():
        if (fso.type == FileType.REGULAR) and ((size:=fso.size) > 0):
            l = size_organiser.setdefault(size,[])
            l.append(fso)

    size_organiser = {size:sorted(fsos,key=lambda x : x.mtime.timestamp(),reverse=True) for size,fsos in size_organiser.items() if len(fsos)>1}

    for i,fs_objs in enumerate(size_organiser.values()):
        a = fs_objs[0]

        _trigger("on_comparing", SyncEvent(a.relative_path, processed=i + 1, total=len(size_organiser)))

        for j in range(1,len(fs_objs)):
            b = fs_objs[j]

            if a.checksum == b.checksum:
                actions.append(SyncAction(a, b, ActionType.DELETE, ActionDirection.SRC2DST))

    _trigger("after_comparing", SyncEvent(actions))

    return actions

# def find_dedupe_rclone(path: Union[str | FileSystem],
#                 eventhandler: [RobinHoodBackend | None] = None
#                 ) -> Iterable[SyncAction]:
#     _trigger = _get_trigger_fn(eventhandler)
#
#     _trigger("before_comparing", SyncEvent(path))
#
#     if (type(path) == str):
#         fs = fs_auto_determine(path, True)
#         fs.cached = True
#
#     fs.load()
#
#     cmdline_args = ['rclone', 'dedupe', fs.root, '--dedupe-mode', 'list']
#
#     if (isinstance(fs, LocalFileSystem)):
#         cmdline_args.append("--by-hash")
#
#     report = subprocess.run(cmdline_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#
#     actions = []
#
#     if (report.returncode == 0):
#         stdout = report.stdout.decode().splitlines()
#
#         dedupes = {}
#
#         i = 0
#
#         while (i < len(stdout)):
#             line = stdout[i]
#
#             match = re.match(r"([0-9a-fA-F]+): ([\d]+) duplicates", line)
#
#             if match is not None:
#                 hash = match[1]
#                 n = int(match[2])
#
#                 files = []
#
#                 for j in range(1, n + 1):
#                     tokens = stdout[i + j].split(",")
#                     files.append(tokens[-1].strip())
#
#                 i += n
#                 dedupes[hash] = files[::-1]  # for some reason, what it seems to be the original file is the last
#
#             i += 1
#
#         for hashes, files in dedupes.items():
#
#             orig = fs.new_path(files[0])
#
#             for dup in files[1:]:
#                 duplicate_filepath = fs.new_path(dup)
#                 a = fs.get_file(orig)
#                 b = fs.get_file(duplicate_filepath)
#
#                 actions.append(SyncAction(a, b, ActionType.DELETE, ActionDirection.SRC2DST))
#
#         _trigger("after_comparing", SyncEvent(actions))
#
#     return actions


def compare_tree(src: Union[str | FileSystem],
                 dest: Union[str | FileSystem],
                 mode: SyncMode.UPDATE,
                 profile: RobinHoodProfile,
                 eventhandler: [RobinHoodBackend | None] = None
                 ) -> SynchingManager:
    """
    Compare two directories and return differences according to the provided synching modality
    :param src: Source directory
    :param dest: Destination directory
    :param mode: One of the following: Update, Mirror, Sync, Dedupe (see SyncMode Enum)
    :param profile: A specific user profile
    :param eventhandler: A class of the type RobinHoodBackend that listens to all the events made occurring the tree comparison
    :return:
    """
    _trigger = _get_trigger_fn(eventhandler)

    _trigger("before_comparing", SyncEvent(src))

    # if the provided source path is a dir, then the function fs_auto_determine attempts to determine if it's local or remote

    if (type(src) == str):
        src = fs_auto_determine(src, True)
        src.cached = True

    # Same as above, but with the destination path
    if (type(dest) == str):
        dest = fs_auto_determine(dest, True)
        dest.cached = True

    # Loads both directories caches

    src.load()
    dest.load()

    # Asks rclone to compute the differences betweeen those two directories
    # TODO: implement deep search using rclone flags
    report = subprocess.run(['rclone', 'check', src.root, dest.root, '--combined', '-'], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)

    # Parse the result obtained  from rclone
    sync_changes = SynchingManager(src , dest)

    files = report.stdout.decode().splitlines()

    # for each line in the stdout
    for i, line in enumerate(files):
        # for more information regarding the output of rclone check, please refer to https://rclone.org/commands/rclone_check/
        action, path = line.split(" ", maxsplit=1)

        # Triggers the event that `path` is currently under examination
        _trigger("on_comparing", SyncEvent(path, processed=i + 1, total=len(files)))

        # Converts the path (string) into an *AbstractPath
        src_path = src.new_path(path)
        dest_path = dest.new_path(path)

        # Attempts to get a corresponding FileSystemObject from the provided path

        source_object = None
        dest_object = None

        # The try-expect blocks are necessary because the file might not exist in one of the two sides

        try:
            source_object = src.get_file(src_path)
        except FileNotFoundError:
            ...

        try:
            dest_object = dest.get_file(dest_path)
        except FileNotFoundError:
            ...

        past_source_object = src.get_previous_version(src_path)
        past_dest_object = dest.get_previous_version(dest_path)

        # By default, it's assumed that the standard way to transfer files is from source -> destination
        # Specific cases are treated below
        direction = ActionDirection.SRC2DST

        match action:
            # + means file exists in source, not in destination
            case "+":
                dest_object = FileSystemObject(fullpath=dest_path,
                                               type=source_object.type,
                                               size=None,
                                               mtime=None,
                                               exists=False,
                                               hidden=source_object.hidden)

                # if the file has been found in source but not in dest, the following cases apply:
                # 1- Never existed (has been added in source)
                # 2- Existed (ie has been deleted from dest)
                # 3- Existed (ie moved to a different location preserving its name)

                # Case 1
                if (past_dest_object is None) or (not past_dest_object.exists):
                    action = ActionType.COPY #if source_object.type == FileType.REGULAR else ActionType.MKDIR
                # Other cases
                else:
                    # The presence of a past_dest_object doesn't necessary mean it actually existed. Let's doublecheck it
                    if past_dest_object.exists:
                        # Case 2: if paths coincides, it's been deleted
                        if source_object.relative_path == past_dest_object.relative_path:
                            action = ActionType.DELETE
                            direction = ActionDirection.DST2SRC
                        else:
                            # Case 3: it's been moved
                            NotImplementedError("Not yet mate!")

            # - means file exists in destination, not in source
            case "-":
                source_object = FileSystemObject(fullpath=src_path,
                                                 type=dest_object.type,
                                                 size=None,
                                                 mtime=None,
                                                 exists=False,
                                                 hidden=dest_object.hidden)

                # Case 1
                if (past_source_object is None) or (not past_source_object.exists):
                    direction = ActionDirection.DST2SRC
                    action = ActionType.COPY #if dest_object.type == FileType.REGULAR else ActionType.MKDIR
                # Other cases
                else:
                    # The presence of a past_dest_object doesn't necessary mean it actually existed. Let's doublecheck it
                    if past_source_object.exists:
                        # Case 2: if paths coincides, it's been deleted
                        if dest_object.relative_path == past_source_object.relative_path:
                            action = ActionType.DELETE
                        else:
                            # Case 3: it's been moved
                            NotImplementedError("Not yet mate!")

            # * means file exists in both side but somehow differs - more needs to be done to determine what to copy where
            #TODO: detects deleted/moved files from cache
            case "*":
                if source_object.mtime.timestamp() >= dest_object.mtime.timestamp():
                    action = ActionType.UPDATE
                    direction = ActionDirection.SRC2DST
                else:
                    action = ActionType.UPDATE
                    direction = ActionDirection.DST2SRC
            # ! means there's been an error - maybe to leave this
            case "!":
                action = ActionType.UNKNOWN
            case _:
                action = ActionType.NOTHING

        sync_changes.add_action(SyncAction(source_object, dest_object, action, direction))

    # Flush file info to cache
    src.flush_file_object_cache()
    dest.flush_file_object_cache()

    # Filter results utilising user-defined filters
    filter_results(sync_changes, profile)

    # Fix actions according to sync mode
    match mode:
        case SyncMode.UPDATE:
            results_for_update(sync_changes)
        case SyncMode.MIRROR:
            results_for_mirror(sync_changes)

    _trigger("after_comparing", SyncEvent(sync_changes))

    return sync_changes


def apply_changes(changes: SynchingManager,
                  eventhandler: [RobinHoodBackend | None] = None,
                  show_progress:bool=False
                  ) -> None:
    '''
    Applies all changes to the two (local/remote) drives
    :param changes:  An iterable (eg a list) of objects of any subtype of SyncAction
    :param eventhandler: A class extending RobinHoodBackend (where events will be passed to)
    :param show_progress: A boolean representing whether to show the progress bar or not (useful for batch processes)
    '''

    # Gets the function that facilitate the triggering of events in the eventhandler (if provided)
    _trigger = _get_trigger_fn(eventhandler)

    # Triggers the before_synching event
    _trigger("before_synching", SyncEvent())

    change_list = [changes]

    # If both local and remote paths are provided, action grouping is possible
    if (changes.source is not None) and (changes.destination is not None):
        # Source to destination and Destination to source are managed separately
        # src_path = changes.source.new_path(changes.source.root)
        # dst_path = changes.destination.new_path(changes.destination.root)

        bulky_copy_src2dst = BulkCopySynchingManager(changes.source,
                                                     changes.destination,
                                                     direction=ActionDirection.SRC2DST)
        bulky_copy_dst2src = BulkCopySynchingManager(changes.source,
                                                     changes.destination,
                                                     direction=ActionDirection.DST2SRC)

        # A list containing all the remaining actions that couldn't be bulked
        other_actions = SynchingManager(changes.source, changes.destination)

        # Check each action if it could be put inside one of the two copy bulks
        for itm in changes:
            if (itm.type != ActionType.NOTHING) and (itm.status != SyncStatus.SUCCESS):

                # Check the action direction
                match itm.direction:
                    case ActionDirection.SRC2DST:
                        try:
                            # the add_action method raises an exception if the provided action is not bulkable
                            bulky_copy_src2dst.add_action(itm)
                        except ValueError:
                            # in this case, it's another type of action that will be treated individually
                            other_actions.add_action(itm)
                    case ActionDirection.DST2SRC:
                        # whatever happens here is the same as before, but in the opposite direction
                        try:
                            bulky_copy_dst2src.add_action(itm)
                        except ValueError:
                            other_actions.add_action(itm)

        # The changes list/iterable is updated with a list containing the two bulks and the other changes to apply
        change_list = [bulky_copy_src2dst, bulky_copy_dst2src, other_actions ]

    # here is where the real magic happens: all actions are applied
    for r in change_list:
        #if not isinstance(r,SyncAction) or r.action_type != ActionType.NOTHING:
        r.apply_changes(show_progress=show_progress, eventhandler=eventhandler)

    _trigger("after_synching", SyncEvent())


def filter_results(changes: SynchingManager, profile: RobinHoodProfile):
    exclusion_filters = profile.exclusion_filters
    filters: List[FileFilter] = []

    if exclusion_filters is not None:
        filters = [UnixPatternExpasionFilter(pattern) for pattern in profile.exclusion_filters]

    if profile.exclude_hidden_files:
        filters.append(RemoveHiddenFileFilter())

    if (len(filters) > 0):
        filter_set = FilterSet(*filters)

        actions_to_remove = []

        for x in changes:
            if filter_set.filter(x.a) or filter_set.filter(x.b):
                actions_to_remove.append(x)

        for x in actions_to_remove:
            changes.remove_action(x)


def results_for_update(results: SynchingManager) -> None:
    for action in results:
        src = action.a
        dest = action.b

        if action.direction == ActionDirection.DST2SRC:
            action.type = ActionType.NOTHING
            action.direction = None
        elif (src is not None) and (dest is not None):
            if (action.type == ActionType.NOTHING):
                if (src.size != dest.size):
                    src_mtime = src.mtime
                    dest_mtime = dest.mtime
                    if src_mtime.timestamp() > dest_mtime.timestamp():
                        action.direction = ActionDirection.SRC2DST
                        action.type = ActionType.UPDATE
                        #
                        # match src.type:
                        #     case FileType.REGULAR:
                        #
                        #     case FileType.DIR:
                        #         action.action_type = ActionType.MKDIR
                    else:
                        action.type = ActionType.UNKNOWN


def results_for_mirror(results: SynchingManager) -> None:
    for action in results:
        src = action.a
        dest = action.b

        if (action.direction == ActionDirection.DST2SRC) or ((src is not None) and (dest is not None)):
            if (src.size != dest.size):
                action.direction = ActionDirection.SRC2DST

                if (not src.exists):
                    action.type = ActionType.DELETE
                else:
                    action.type = ActionType.UPDATE
                    # match src.type:
                    #     case FileType.REGULAR:
                    #
                    #     case FileType.DIR:
                    #         action.action_type = ActionType.MKDIR

def _parse_rclone_progress(actions:List[SyncAction], sync_direction: ActionDirection, output:Dict) -> SyncProgress:
    '''
    Creates a suitable SyncProgress and Action progress objects from the dictionary generated by rclone_python
    :param actions: List of all actions treated during synchronisation
    :param sync_direction: either source-to-destination or destination-to-source
    :param output: rclone_python output coming in the form of a dictionary
    :return: A SyncProgress object with all the information nicely formatted
    '''

    # sometimes rclone puts ellipsis for long paths - either of these conditions is fine
    match_path = lambda t,a: a.endswith(t) or fnmatch(a,t.replace("\u2026","*"))

    current_time = datetime.now()

    individual_tasks = []

    for task in output['prog_transferring']:
        current_action = None

        # The aim of this for-loop is to match the task in rclone with the internal represation of that task
        # stored in a SyncAction within the list of actions
        for x in actions:
            # the source path depends on the direction of the action.
            # Whether to compare with x.a and x.b depends on where we are transfering from
            if (sync_direction == ActionDirection.SRC2DST) and match_path(task[0],x.a.relative_path): #(x.a.relative_path.endswith(task[0])):
                current_action = x
                break
            if (sync_direction == ActionDirection.DST2SRC) and match_path(task[0],x.b.relative_path): #(x.b.relative_path.endswith(task[0])):
                current_action = x
                break

        # if the action could not be found, something off is happening. All actions performed by rclone should
        # be the ones the user approved. Alternatively, there's a bug in the way paths are matched above.
        if current_action is not None:
            local_progress = ActionProgress(filename=task[0],
                                            progress=task[1],
                                            transfer_speed=task[2],
                                            timestamp=current_time)

            current_action.update = local_progress

            individual_tasks.append(current_action)

    output['prog_transferring'] = individual_tasks


    return SyncProgress(timestamp=current_time,**output)

def kill_all_subprocesses():
    global _POPEN

    for proc in _POPEN:
        if proc.returncode is None:
            proc.terminate()
            try:
                proc.communicate()
            except ValueError:
                # We could be at the end of the file, ie the pipe has been already all read
                # this should be enough to read the exit code of the process and terminate it
                ...

    _POPEN = [p for p in _POPEN if p.returncode is not None]