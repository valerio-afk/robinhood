from typing import Iterable,Callable, Union
from fnmatch import fnmatch
from abc import ABC, abstractmethod
from filesystem import FileSystemObject
class FileFilter(ABC):

    def __init__(this) -> None:
        this.__hit = 0

    def __call__(this,fullpath) -> bool:
        check = this.filter(fullpath)

        if (check): this.__hit+=1

        return check

    @abstractmethod
    def filter (this,fso:FileSystemObject) -> bool:
        ...

    @property
    def hits(this):
        return this.__hit

class RemoveHiddenFileFilter(FileFilter):

    def filter (this,fso:FileSystemObject) -> bool:
        return fso.hidden

    def __str__(this) -> str:
        return f"Hidden file(s) removed: {this.hits}"

    def __repr__(this) -> str:
        return str(this)



class UnixPatternExpasionFilter(FileFilter):

    def __init__(this, pattern:str) -> None:
        super().__init__()
        this._pattern:str = pattern

    def filter(this,fso:FileSystemObject) -> bool:
        return fnmatch(fso.absolute_path,this.pattern)

    @property
    def pattern(this) -> str:
        return this._pattern

    def __str__(this) -> str:
        return f"Number of file(s) removed matching {this.pattern}: {this.hits}"

    def __repr__(this) -> str:
        return str(this)


class FilterSet:
    def __init__(this,*args):
        for i,x in enumerate(args):
            if not isinstance(x,FileFilter):
                raise TypeError(f"Argument {i+1} is not a (sub)type of FileFilter")

        this.filters = args

    def __call__(this,q:Iterable,key:Union[Callable|None]=None):

        if key is None: key = lambda x:x

        return [ itm for itm in q if not this.filter(key(itm)) ]

    def filter(this, file:Union[FileSystemObject|None]):

        if (file is not None):
            for filter in this.filters:
                if (filter(file)): return True

        return False