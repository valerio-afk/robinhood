from typing import Union,List, Iterable, Callable, Dict, Tuple, Any
from dataclasses import dataclass
from abc import ABC, abstractmethod
from enums import SyncMode, SyncStatus, ActionType, ActionDirection
from filesystem import FileType,FileSystemObject,FileSystem,fs_auto_determine, mkdir, convert_to_bytes, LocalFileSystem
from file_filters import UnixPatternExpasionFilter,RemoveHiddenFileFilter, FilterSet, FileFilter
from datetime import datetime
from rclone_python.rclone import copy, delete
from config import RobinHoodProfile
import subprocess
import re
import rclone_python

def improved_extract_rclone_progress(buffer: str) -> Tuple[bool, Union[Dict[str, Any], None]]:
    # matcher that checks if the progress update block is completely buffered yet (defines start and stop)
    # it gets the sent bits, total bits, progress, transfer-speed and eta
    reg_transferred = re.findall(
        r"Transferred:\s+(\d+(.\d+)? \w+) \/ (\d+.\d+ \w+), (\d{1,3})%, (\d+(.\d+)? \w+\/\w+), ETA (\S+)", #fixed pattern
        buffer,
    )

    def _extract_value_unit(pattern:str) -> Tuple[float,str]:
        try:
            a,b = pattern.strip().split(" ")
        except ValueError:
            a = pattern.strip()
            b = "B/s"

        a = float(a)
        return a,b

    if reg_transferred:  # transferred block is completely buffered
        # get the progress of the individual files
        # matcher gets the currently transferring files and their individual progress
        # returns list of tuples: (name, progress, file_size, unit)
        prog_transferring = []
        prog_regex = re.findall(
            r"\* +(.+):[ ]+(\d{1,3})% \/(\d+.\d+)([a-zA-Z]+),", buffer
        )
        for item in prog_regex:
            prog_transferring.append(
                (
                    item[0],
                    int(item[1]),
                    float(item[2]),
                    # the suffix B of the unit is missing for subprocesses
                    item[3] + "B",
                )
            )

        out = {"prog_transferring": prog_transferring}
        sent_bits, _, total_bits, progress, transfer_speed_str, _,  eta = reg_transferred[0]
        out["progress"] = float(progress.strip())

        out["total_bits"] , out["unit_total"] = _extract_value_unit(total_bits)
        out["sent_bits"], out["unit_sent"] = _extract_value_unit(sent_bits)
        out["transfer_speed"], out["transfer_speed_unit"] = _extract_value_unit(transfer_speed_str)

        out["eta"] = eta

        return True, out

    else:
        return False, None

rclone_python.rclone.utils.extract_rclone_progress = improved_extract_rclone_progress



@dataclass(frozen=True)
class SyncProgress():
    prog_transferring:str=""
    progress:float = 0
    total_bits:float=0
    sent_bits:float = 0
    unit_sent:str = ""
    unit_total:str = ""
    transfer_speed:float = 0
    transfer_speed_unit:str = ""
    eta:str=""


    @property
    def bytes_transferred(this):
        return convert_to_bytes(this.sent_bits, this.unit_sent)

    @property
    def bytes_total(this):
        return convert_to_bytes(this.total_bits, this.unit_total)




class SyncEvent:

    def __init__(this, value:Any=None,*,processed:Union[int|None]=None,total:Union[int|None]=None):
        this.value = value
        this.processed = processed
        this.total = total


class SyncComparisonEvent(SyncEvent):

    def __init__(this, src_path=None, dest_path=None):
        super().__init__((src_path,dest_path))

    @property
    def source_path(this):
        return this.value[0]

    @property
    def destination_path(this):
        return this.value[1]

def _get_trigger_fn(eventhandler:Union[SyncEvent|None]=None) -> Callable[[str,SyncEvent],None]:
    def _trigger(mtd:str, e:SyncEvent)->None:
        if isinstance(eventhandler,RobinHoodBackend):
            try:
                fn = getattr(eventhandler,mtd)
                fn(e)
            except AttributeError:
                ... #event does not exist

    return _trigger
class SyncAction:
    def __init__(this,
                 a:FileSystemObject,
                 b:FileSystemObject,
                 action_type:ActionType=ActionType.NOTHING,
                 direction:Union[ActionDirection|None]=None,
                 timeout = 60
                 ):
        this.a=a
        this.b=b
        this.action_type=action_type
        this.direction=direction
        this._status = SyncStatus.NOT_STARTED
        this._update = None
        this._last_update = None
        this._timeout = timeout

    @property
    def status(this) -> SyncStatus:
        return this._status

    def __action_type_str(this) -> str:
        match(this.action_type):
            case ActionType.NOTHING: return '-'
            case ActionType.MKDIR: return 'D'
            case ActionType.COPY: return '*'
            case ActionType.UPDATE: return '+'
            case ActionType.DELETE: return 'x'

    def __str__(this) -> str:
        action_type = this.__action_type_str()

        if (this.action_type!=ActionType.NOTHING):
            action_type = str(this.direction) + action_type

        return f"{this.a} {action_type} {this.b}"

    def __repr__(this) -> str:
        return str(this)

    @property
    def get_one_path(this):
        return this.a if this.b is None else this.b
    def apply_action(this, show_progress=False, eventhandler:[SyncEvent|None]=None) -> None:
        this._last_update = datetime.now()
        _trigger = _get_trigger_fn(eventhandler)

        def _update_internal_status(d:Dict):
            this._last_update = datetime.now()
            this._update = SyncProgress(**d)
            _trigger("on_synching",SyncEvent(this))

        match this.action_type:
            case ActionType.MKDIR:
                mkdir(this.get_one_path)
            case ActionType.DELETE:
                try:
                    delete(this.get_one_path.absolute_path)
                except Exception:
                    this._status = SyncStatus.FAILED
            case ActionType.UPDATE | ActionType.COPY:
                x = this.a.absolute_path
                y = this.b.containing_directory

                if (this.direction == ActionDirection.DST2SRC):
                    x = this.b.absolute_path
                    y = this.a.containing_directory

                this._status = SyncStatus.IN_PROGRESS
                copy(x,y,show_progress=show_progress,listener=_update_internal_status)


        this._check_success()

        _trigger("on_synching", SyncEvent(this))

    def _check_success(this):
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

    def get_update(this) -> Union[SyncProgress|None]:
        return this._update




class RobinHoodBackend(ABC):
    @abstractmethod
    def before_comparing(this, event:SyncEvent) -> None:
        ...

    @abstractmethod
    def on_comparing(this,event:SyncEvent) -> None:
        ...

    @abstractmethod
    def after_comparing(this, event:SyncEvent) -> None:
        ...

    @abstractmethod
    def before_synching(this, event:SyncEvent) -> None:
        ...

    @abstractmethod
    def on_synching(this, event:SyncEvent) -> None:
        ...

    @abstractmethod
    def after_synching(this, event:SyncEvent) -> None:
        ...

def find_dedupe(path:Union[str|FileSystem],
                 eventhandler:[SyncEvent|None]=None
                 )->Iterable[SyncAction]:

    _trigger = _get_trigger_fn(eventhandler)

    _trigger("before_comparing", SyncEvent(path))

    if (type(path) == str):
        fs=fs_auto_determine(path,True)
        fs.cached=True

    fs.load()

    cmdline_args = ['rclone', 'dedupe', fs.root, '--dedupe-mode', 'list']

    if (isinstance(fs,LocalFileSystem)):
        cmdline_args.append("--by-hash")

    report = subprocess.run(cmdline_args,stdout=subprocess.PIPE,stderr=subprocess.PIPE)

    actions = []

    if (report.returncode==0):
        stdout = report.stdout.decode().splitlines()

        dedupes = {}

        i = 0

        while (i<len(stdout)):
            line = stdout[i]

            match = re.match(r"([0-9a-fA-F]+): ([\d]+) duplicates", line)

            if match is not None:
                hash = match[1]
                n = int(match[2])

                files = []

                for j in range(1,n+1):
                    tokens = stdout[i+j].split(",")
                    files.append(tokens[-1].strip())

                i += n
                dedupes[hash] = files[::-1] #for some reason, what it seems to be the original file is the last

            i+=1


        for hashes, files in dedupes.items():

            orig = fs.new_path(files[0])

            for dup in files[1:]:
                duplicate_filepath = fs.new_path(dup)
                a = fs.get_file(orig)
                b = fs.get_file(duplicate_filepath)

                actions.append(SyncAction(a,b,ActionType.DELETE,ActionDirection.SRC2DST))

        _trigger("after_comparing", SyncEvent(actions))

    return actions

def compare_tree(src:Union[str|FileSystem],
                 dest:Union[str|FileSystem],
                 mode:SyncMode.UPDATE,
                 eventhandler:[SyncEvent|None]=None
                 )->Iterable[SyncAction]:

    _trigger = _get_trigger_fn(eventhandler)

    _trigger("before_comparing", SyncEvent(src))

    if (type(src) == str):
        src=fs_auto_determine(src,True)
        src.cached=True


    if (type(dest) == str):
        dest=fs_auto_determine(dest,True)
        dest.cached=True


    #directories_to_visit = ['.']
    #tree = []

    src.load()
    dest.load()

    report = subprocess.run(['rclone', 'check', src.root, dest.root, '--combined', '-'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)

    results = []
    files  = report.stdout.decode().splitlines()

    for i,line in enumerate(files):
        action, path = line.split(" ", maxsplit=1)
        _trigger("on_comparing", SyncEvent(path,processed=i+1,total=len(files)))

        src_path  = src.new_path(path)
        dest_path = dest.new_path(path)


        source_object = None
        dest_object = None


        try:
            source_object = src.get_file(src_path)
        except FileNotFoundError:
            ...

        try:
            dest_object   = dest.get_file(dest_path)
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

    results = filter_results(results)

    match mode:
        case SyncMode.UPDATE: results_for_update(results)
        case SyncMode.MIRROR: results_for_mirror(results)

    _trigger("after_comparing",SyncEvent(results))

    return results


def apply_changes(changes:Iterable[SyncAction],
                 eventhandler:[SyncEvent|None]=None,
                 show_progress=False
                 )->None:

    _trigger = _get_trigger_fn(eventhandler)
    _trigger("before_synching", SyncEvent())

    for r in changes:
        r.apply_action(show_progress=show_progress,eventhandler = eventhandler)

    _trigger("after_synching", SyncEvent())

def filter_results(results:Iterable[SyncAction])->Iterable[SyncAction]:
    exclusion_filters = RobinHoodProfile().current_profile.exclusion_filters
    filters:List[FileFilter] = []

    if exclusion_filters is not None:
        filters = [UnixPatternExpasionFilter(pattern) for pattern in RobinHoodProfile().current_profile.exclusion_filters]

    if RobinHoodProfile().current_profile.exclude_hidden_files:
        filters.append(RemoveHiddenFileFilter())


    if (len(filters)>0):
        filter_set = FilterSet(*filters)

        results = filter_set(results,key=lambda x : x.a)
        results = filter_set(results, key=lambda x: x.b)

    return results



def results_for_update(results:Iterable[SyncAction]) -> None:
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

def results_for_mirror(results:Iterable[SyncAction]) -> None:
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

