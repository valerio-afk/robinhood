from typing import Union,List, Iterable, Callable, Dict
from dataclasses import dataclass
from abc import ABC, abstractmethod
from enum import Enum
from filesystem import FileType,FileSystemObject,FileSystem,fs_auto_determine, mkdir
from file_filters import UnixPatternExpasionFilter,RemoveHiddenFileFilter, FilterSet, FileFilter
from datetime import datetime
from rclone_python.rclone import copy, delete

class SyncMode(Enum):
    UPDATE:int = 0
    MIRROR:int = 1
    SYNC:int = 2

class ActionType(Enum):
    NOTHING:int=0
    MKDIR:int=1
    COPY:int=2
    UPDATE:int=3
    DELETE:int=4
    UNKNOWN:int=5

@dataclass(frozen=True)
class SyncProgress():
    prog_transferring:str=None
    progress:float = 0
    total_bits:float=0
    sent_bits:float = 0
    unit_sent:str = None
    unit_total:str = None
    transfer_speed:float = 0
    transfer_speed_unit:str = None

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


class ActionDirection(Enum):
    SRC2DST:str = '>'
    DST2SRC:str = '<'

    def __str__(this) ->str:
        return this.value


class SyncStatus(Enum):
    NOT_STARTED : int = 0
    IN_PROGRESS : int = 1
    SUCCESS     : int = 2
    FAILED      : int = 3
    INTERRUPTED : int = 4

class SyncEvent:

    def __init__(this, value=None):
        this.value = value


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
                 direction:Union[SyncMode|None]=None,
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
                this._status = SyncStatus.SUCCESS if mkdir(this.get_one_path) else SyncStatus.FAILED
            case ActionType.DELETE:
                try:
                    delete(this.get_one_path.absolute_path)
                    this._status = SyncStatus.SUCCESS
                except Exception:
                    this._status = SyncStatus.FAILED
            case ActionType.UPDATE | ActionType.COPY:
                x = this.a.absolute_path
                y = this.b.containing_directory

                if (this.direction == ActionDirection.DST2SRC):
                    x,y=y,x

                copy(x,y,show_progress=show_progress,listener=_update_internal_status)
                this._status = SyncStatus.SUCCESS if  (this._update is None) or (this._update.progress == 100) else SyncStatus.FAILED
            case ActionType.NOTHING:
                this._status = SyncStatus.SUCCESS

        _trigger("on_synching", SyncEvent(this))


    def get_update(this) -> Union[SyncProgress|None]:
        if  (this.status != SyncStatus.IN_PROGRESS):
            return None

        delta = datetime.now() - this._last_update

        if (delta.total_seconds()>this._timeout):
            this._status = SyncStatus.INTERRUPTED

        return this._update




class RobinHoodBackend(ABC):


    @abstractmethod
    def before_comparing(this, event):
        ...

    @abstractmethod
    def on_comparing(this,event):
        ...

    @abstractmethod
    def after_comparing(this, event):
        ...

    @abstractmethod
    def before_synching(this, event):
        ...

    @abstractmethod
    def on_synching(this, event):
        ...

    @abstractmethod
    def after_synching(this, event):
        ...


def compare_tree(src:Union[str|FileSystem],
                 dest:Union[str|FileSystem],
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


    directories_to_visit = ['.']
    tree = []

    src.load()
    dest.load()

    while len(directories_to_visit)>0:
        cp = directories_to_visit.pop()

        a = src.ls(cp)
        b = dest.ls(cp)

        _trigger("on_comparing", SyncEvent(cp))

        common_files = set(a) & set(b)
        unique_files = set(a) ^ set(b)

        directories_to_visit += [d.relative_path for d in list(common_files) if d.type == FileType.DIR]

        for f in common_files:
            if f.type == FileType.REGULAR:
                idx_src = a.index(f)
                idx_dst = b.index(f)

                tree.append(SyncAction(a[idx_src],b[idx_dst]))

        for f in unique_files:
            new_file_object = FileSystemObject(fullpath=None,
                                               type=f.type,
                                               size=f.size,
                                               mtime=f.mtime,
                                               hidden=f.hidden)

            if (src.root in f.absolute_path):
                new_file_object.fullpath = dest.new_path(f.relative_path)
                x = f
                y = new_file_object
                dir = ActionDirection.SRC2DST
            else:
                new_file_object.fullpath = src.new_path(f.relative_path)
                x = new_file_object
                y = f
                dir = ActionDirection.DST2SRC

            action = ActionType.COPY if f.type == FileType.REGULAR else ActionType.MKDIR

            if f.type == FileType.DIR:
                directories_to_visit.append(f.relative_path)

            tree.append(SyncAction(x,y,action,dir))


    tree = filter_results(tree)
    results_for_update(tree)

    _trigger("after_comparing",SyncEvent(tree))

    return tree


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
    exclusion_filters = RobinHoodConfiguration().exclusion_filters
    filters:List[FileFilter] = []

    if exclusion_filters is not None:
        filters = [UnixPatternExpasionFilter(pattern) for pattern in RobinHoodConfiguration().exclusion_filters]

    if RobinHoodConfiguration().exclude_hidden_files:
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
