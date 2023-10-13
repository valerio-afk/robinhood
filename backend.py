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

import os.path
from typing import Union, List, Iterable, Callable, Dict, Tuple, Any
from dataclasses import dataclass
from abc import ABC, abstractmethod
from enums import SyncMode, SyncStatus, ActionType, ActionDirection
from filesystem import FileType, FileSystemObject, FileSystem, fs_auto_determine, mkdir, convert_to_bytes, \
    LocalFileSystem
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

    def __init__(this, direction: Union[ActionDirection | None] = None):
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

        super().__init__(direction)
        this.a = a
        this.b = b
        this.action_type = action_type
        this._update = None
        this._timeout = timeout



    def __action_type_str(this) -> str:
        match (this.action_type):
            case ActionType.NOTHING:
                return '-'
            case ActionType.MKDIR:
                return 'D'
            case ActionType.COPY:
                return '*'
            case ActionType.UPDATE:
                return '+'
            case ActionType.DELETE:
                return 'x'

    def __str__(this) -> str:
        action_type = this.__action_type_str()

        if (this.action_type != ActionType.NOTHING):
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

        match this.action_type:
            case ActionType.MKDIR:
                mkdir(this.get_one_path)
            case ActionType.DELETE:
                try:
                    p = this.b if this.direction.SRC2DST else this.a
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

        if this.action_type == ActionType.NOTHING:
            success = True
        else:
            x = this.a
            y = this.b

            x.update_information()
            y.update_information()

            if (this.direction == ActionDirection.DST2SRC):
                x, y = y, x

            match this.action_type:
                case ActionType.MKDIR:
                    success = y.exists
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


class BulkCopyAction(AbstractSyncAction):

    def __init__(this,
                 root_source: AbstractPath,
                 root_dst: AbstractPath,
                 direction: ActionDirection):

        super().__init__(direction)

        this._actions: List[SyncAction] = []
        this._root_source = root_source
        this._root_destination = root_dst
        this._actions_in_progress = []

    def add_action(this, action: SyncAction) -> None:
        if not this._root_source.is_under_root(action.a.absolute_path):
            raise ValueError(f"The file '{action.a.relative_path} 'is not in '{this._root_source}'")

        if not this._root_destination.is_under_root(action.b.absolute_path):
            raise ValueError(f"The file '{action.b.relative_path} 'is not in '{this._root_destination}'")

        if action.action_type not in [ActionType.COPY, ActionType.UPDATE]:
            raise ValueError("The provided action is not copying or updating a file")

        if (action.direction != this.direction):
            raise ValueError("The provided action is towards a different synching direction")

        this._actions.append(action)

    def apply_action(this, show_progress:bool=False, eventhandler: [SyncEvent | None] = None) -> None:
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
            sync_update = _parse_rclone_progress(this._actions, this.direction,d)

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

            # Filter out all the terminated actions
            this._actions_in_progress = [x for x in this._actions_in_progress if x.status == SyncStatus.IN_PROGRESS]

            # Notify that these actions are still in progress and send them as a list
            _trigger("on_synching",SyncEvent(sync_update))



        tmp_dir = site_cache_path()
        tmp_fname = f"rh_sync_tmp_{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.txt"

        path = os.path.join(tmp_dir, tmp_fname)


        with open(path, "w") as handle:
            for x in this._actions:
                fso = x.a if this.direction == ActionDirection.SRC2DST else x.b
                handle.write(f"{fso.relative_path}\n")

        a = this._root_source.absolute_path
        b = this._root_destination.absolute_path

        if this.direction == ActionDirection.DST2SRC:
            a,b=b,a

        copy(a,
             b,
             show_progress=show_progress,
             listener=_update_internal_status,
             args=['--files-from', path,'--no-check-dest', '--no-traverse'])

        # Better double checking again when it's done if everything has been copied successfully
        for itm in this._actions:
            if itm.status in [SyncStatus.IN_PROGRESS, SyncStatus.NOT_STARTED]:
                itm._check_success()
                _trigger("on_synching", SyncEvent(itm))


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

def find_dedupe_managed(path: Union[str | FileSystem],
                        eventhandler: [RobinHoodBackend | None] = None
                        ) -> Iterable[SyncAction]:

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

    size_organiser = {size:sorted(fsos) for size,fsos in size_organiser.items() if len(fsos)>1}

    for i,fs_objs in enumerate(size_organiser.values()):
        a = fs_objs[0]

        _trigger("on_comparing", SyncEvent(a.relative_path, processed=i + 1, total=len(size_organiser)))

        for j in range(1,len(fs_objs)):
            b = fs_objs[j]

            if a.checksum == b.checksum:
                actions.append(SyncAction(a, b, ActionType.DELETE, ActionDirection.SRC2DST))

    _trigger("after_comparing", SyncEvent(actions))

    return actions

def find_dedupe(path: Union[str | FileSystem],
                eventhandler: [RobinHoodBackend | None] = None
                ) -> Iterable[SyncAction]:
    _trigger = _get_trigger_fn(eventhandler)

    _trigger("before_comparing", SyncEvent(path))

    if (type(path) == str):
        fs = fs_auto_determine(path, True)
        fs.cached = True

    fs.load()

    cmdline_args = ['rclone', 'dedupe', fs.root, '--dedupe-mode', 'list']

    if (isinstance(fs, LocalFileSystem)):
        cmdline_args.append("--by-hash")

    report = subprocess.run(cmdline_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    actions = []

    if (report.returncode == 0):
        stdout = report.stdout.decode().splitlines()

        dedupes = {}

        i = 0

        while (i < len(stdout)):
            line = stdout[i]

            match = re.match(r"([0-9a-fA-F]+): ([\d]+) duplicates", line)

            if match is not None:
                hash = match[1]
                n = int(match[2])

                files = []

                for j in range(1, n + 1):
                    tokens = stdout[i + j].split(",")
                    files.append(tokens[-1].strip())

                i += n
                dedupes[hash] = files[::-1]  # for some reason, what it seems to be the original file is the last

            i += 1

        for hashes, files in dedupes.items():

            orig = fs.new_path(files[0])

            for dup in files[1:]:
                duplicate_filepath = fs.new_path(dup)
                a = fs.get_file(orig)
                b = fs.get_file(duplicate_filepath)

                actions.append(SyncAction(a, b, ActionType.DELETE, ActionDirection.SRC2DST))

        _trigger("after_comparing", SyncEvent(actions))

    return actions


def compare_tree(src: Union[str | FileSystem],
                 dest: Union[str | FileSystem],
                 mode: SyncMode.UPDATE,
                 profile: RobinHoodProfile,
                 eventhandler: [RobinHoodBackend | None] = None
                 ) -> Iterable[SyncAction]:
    _trigger = _get_trigger_fn(eventhandler)

    _trigger("before_comparing", SyncEvent(src))

    if (type(src) == str):
        src = fs_auto_determine(src, True)
        src.cached = True

    if (type(dest) == str):
        dest = fs_auto_determine(dest, True)
        dest.cached = True

    # directories_to_visit = ['.']
    # tree = []

    src.load()
    dest.load()

    report = subprocess.run(['rclone', 'check', src.root, dest.root, '--combined', '-'], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)

    results = []
    files = report.stdout.decode().splitlines()

    for i, line in enumerate(files):
        action, path = line.split(" ", maxsplit=1)
        _trigger("on_comparing", SyncEvent(path, processed=i + 1, total=len(files)))

        src_path = src.new_path(path)
        dest_path = dest.new_path(path)

        source_object = None
        dest_object = None

        try:
            source_object = src.get_file(src_path)
        except FileNotFoundError:
            ...

        try:
            dest_object = dest.get_file(dest_path)
        except FileNotFoundError:
            ...

        direction = ActionDirection.SRC2DST

        match action:
            case "+":
                dest_object = FileSystemObject(fullpath=dest_path,
                                               type=source_object.type,
                                               size=None,
                                               mtime=None,
                                               exists=False,
                                               hidden=source_object.hidden)

                action = ActionType.COPY if source_object.type == FileType.REGULAR else ActionType.MKDIR

            case "-":
                source_object = FileSystemObject(fullpath=src_path,
                                                 type=dest_object.type,
                                                 size=None,
                                                 mtime=None,
                                                 exists=False,
                                                 hidden=dest_object.hidden)

                action = ActionType.COPY if dest_object.type == FileType.REGULAR else ActionType.MKDIR
                direction = ActionDirection.DST2SRC

            case "*":
                if source_object.mtime >= dest_object.mtime:
                    action = ActionType.UPDATE
                    direction = ActionDirection.SRC2DST
                else:
                    action = ActionType.UPDATE
                    direction = ActionDirection.DST2SRC

            case "!":
                action = ActionType.UNKNOWN

            case _:
                action = ActionType.NOTHING

        results.append(SyncAction(source_object, dest_object, action, direction))

    results = filter_results(results, profile)

    match mode:
        case SyncMode.UPDATE:
            results_for_update(results)
        case SyncMode.MIRROR:
            results_for_mirror(results)

    _trigger("after_comparing", SyncEvent(results))

    return results


def apply_changes(changes: Iterable[SyncAction],
                  local: [AbstractPath | None] = None,
                  remote: [AbstractPath | None] = None,
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

    # If both local and remote paths are provided, action grouping is possible

    if (local is not None) and (remote is not None):
        # Source to destination and Destination to source are managed separately
        bulky_copy_src2dst = BulkCopyAction(local, remote, ActionDirection.SRC2DST)
        bulky_copy_dst2src = BulkCopyAction(local, remote, ActionDirection.DST2SRC)

        # A list containing all the remaining actions that couldn't be bulked
        others = []

        # Check each action if it could be put inside one of the two copy bulks
        for itm in changes:
            if itm.action_type != ActionType.NOTHING:
                # Check the action direction
                match itm.direction:
                    case ActionDirection.SRC2DST:
                        try:
                            # the add_action method raises an exception if the provided action is not bulkable
                            bulky_copy_src2dst.add_action(itm)
                        except ValueError:
                            # in this case, it's another type of action that will be treated individually
                            others.append(itm)
                    case ActionDirection.DST2SRC:
                        # whatever happens here is the same as before, but in the opposite direction
                        try:
                            bulky_copy_dst2src.add_action(itm)
                        except ValueError:
                            others.append(itm)

        # The changes list/iterable is updated with a list containing the two bulks and the other changes to apply
        changes = [bulky_copy_src2dst, bulky_copy_dst2src] + others

    # here is where the real magic happens: all actions are applied
    for r in changes:
        if not isinstance(r,SyncAction) or r.action_type != ActionType.NOTHING:
            r.apply_action(show_progress=show_progress, eventhandler=eventhandler)

    _trigger("after_synching", SyncEvent())


def filter_results(results: Iterable[SyncAction], profile: RobinHoodProfile) -> Iterable[SyncAction]:
    exclusion_filters = profile.exclusion_filters
    filters: List[FileFilter] = []

    if exclusion_filters is not None:
        filters = [UnixPatternExpasionFilter(pattern) for pattern in profile.exclusion_filters]

    if profile.exclude_hidden_files:
        filters.append(RemoveHiddenFileFilter())

    if (len(filters) > 0):
        filter_set = FilterSet(*filters)

        results = filter_set(results, key=lambda x: x.a)
        results = filter_set(results, key=lambda x: x.b)

    return results


def results_for_update(results: Iterable[SyncAction]) -> None:
    for action in results:
        src = action.a
        dest = action.b

        if action.direction == ActionDirection.DST2SRC:
            action.action_type = ActionType.NOTHING
            action.direction = None
        elif (src is not None) and (dest is not None):
            if (action.action_type == ActionType.NOTHING):
                if (src.size != dest.size):
                    src_mtime = src.mtime
                    dest_mtime = dest.mtime
                    if src_mtime.timestamp() > dest_mtime.timestamp():
                        action.direction = ActionDirection.SRC2DST

                        match src.type:
                            case FileType.REGULAR:
                                action.action_type = ActionType.UPDATE
                            case FileType.DIR:
                                action.action_type = ActionType.MKDIR
                    else:
                        action.action_type = ActionType.UNKNOWN


def results_for_mirror(results: Iterable[SyncAction]) -> None:
    for action in results:
        src = action.a
        dest = action.b

        if (action.direction == ActionDirection.DST2SRC) or ((src is not None) and (dest is not None)):
            if (src.size != dest.size):
                action.direction = ActionDirection.SRC2DST

                if (not src.exists):
                    action.action_type = ActionType.DELETE
                else:
                    match src.type:
                        case FileType.REGULAR:
                            action.action_type = ActionType.UPDATE
                        case FileType.DIR:
                            action.action_type = ActionType.MKDIR

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
