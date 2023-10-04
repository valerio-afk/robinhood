from typing import Any, Union
from abc import ABC, abstractmethod
from enum import Enum
from rclone_python import rclone
from datetime import datetime,timezone
from copy import copy
from psutil import disk_partitions
import os
import re
import stat
import subprocess
import json


is_windows = lambda : os.name =='nt'
current_timezone = lambda : datetime.now().astimezone().tzinfo

UNITS = ("", "K", "M", "G", "T", "P", "E", "Z")

def _fix_isotime(time):
    pattern = r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]*)\+[0-9]{2}:[0-9]{2}"

    matches = re.findall(pattern, time)

    for m in matches:
        m = m.lstrip(".")
        time = time.replace(m,m[:6])

    return time

#Adapted from https://stackoverflow.com/questions/1094841/get-human-readable-version-of-file-size
def sizeof_fmt(num:int, suffix:str="B") -> str:
    if (num == 0):
        return "-"
    
    for unit in UNITS:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Y{suffix}"


def convert_to_bytes(value:float,unit:str) -> int:
    for i,u in enumerate(UNITS[1:]):
        if u in unit:
            return int(value * (1024**(i+1)))

    return int(value)



def get_rclone_remotes():
    output = subprocess.run(
        ["rclone","listremotes","--long"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8"
    )

    return [tuple([x.strip() for x in line.split(":")[::-1]]) for line in output.stdout.strip().split("\n")]

class PathManager(ABC):
    PATH_SEPARATOR = '/'
    VOLUME_SEPARATOR = ":"

    def __init__(this,path,root=None):
        this._basepath = this.normalise(path if root is None else root)

        if (this.is_relative(this._basepath)):
            raise MissingAbsolutePathException(this._basepath,"Basepath")

        path = this.normalise(path)

        if not path.startswith(this._basepath):
            this._path = this.normalise(this.join(this.root,path))
        else:
            this._path = path

        if not this.is_under_root(this._path):
            raise PathOutsideRootException(this.absolute_path,this.root)

    def __copy__(this):
        return type(this)(path=this.absolute_path,root=this.root)

    @classmethod
    def as_posix(cls,p):
        return p.replace("\\", "/")
    @classmethod
    def join(cls,*args):
        paths = args[0] if len(args) == 1 else args

        if (len(paths) == 0):
            return None

        r = paths[0]

        for i in range(1,len(paths)):
            xx = r.endswith(cls.PATH_SEPARATOR)
            yy = paths[i].startswith(cls.PATH_SEPARATOR)

            if (xx ^ yy):
                r+=paths[i]
            elif (xx and yy):
                r+=paths[i][1:]
            else:
                if (cls.is_relative(paths[i])):
                    r+=cls.PATH_SEPARATOR+paths[i]
                else:
                    r = paths[i]

        return r

    def __str__(this):
        return this.relative_path

    def __repr__(this):
        return this.absolute_path

    @classmethod
    def is_special_dir(cls,d:str):
        return (d == ".") or (d == "..")

    @classmethod
    def is_absolute(cls, path):
        return path.startswith(cls.PATH_SEPARATOR)

    @classmethod
    def is_relative(cls, path):
        return not cls.is_absolute(path)

    @classmethod
    def normalise(cls,path):
        return cls.as_posix(path)

    @classmethod
    def split(cls, path):
        tokens = path.split(cls.PATH_SEPARATOR)

        if tokens[0] == '':
            tokens[0] = '/'

        return [t for t in tokens if len(t)>0]

    @property
    @abstractmethod
    def relative_path(this):
        ...

    @property
    def absolute_path(this):
        return this._path

    def cd(this, path):
        if (this.is_under_root(path)):
            if (this.is_absolute(path)):
                this._path = this.normalise(path)
            else:
                new_path = this.normalise(this._path + this.PATH_SEPARATOR + path)
                this._path = new_path if this.is_under_root(new_path) else this.root
        else:
            this._path = this.root

    def visit(this, path):
        c = copy(this)

        c.cd(path)
        return c

    @property
    def root(this):
        return this._basepath

    @root.setter
    def root(this,path):
        #when root is changed, the path needs to be re-rooted
        #therefore, the old relative path needs to be store
        #to be used later to re-root the whole thing
        old_relpath = this.relative_path

        this._basepath = this.normalise(path)

        if (this.is_absolute(this._path)):
            this._path = this.join(this._basepath,old_relpath)

    @classmethod
    def is_root_of(cls, path,root):
        if cls.is_relative(path):
            return True

        spath = cls.split(cls.normalise(path))
        sroot = cls.split(root)


        if (len(sroot)<=len(spath)):
            for i,(x,y) in enumerate(zip (sroot,spath)):
                if (i==0):
                    #this is also viable for posix paths because the first item will be just "/"
                    if (x.lower() != y.lower()) and (y!=cls.PATH_SEPARATOR):
                        return False
                elif x!=y:
                        return False

            return True
        else:
            return False

    def is_under_root(this, path):
        return this.is_root_of(path,this.root)


class FileType(Enum):
    OTHER=0
    REGULAR=1
    DIR=2

class FileSystemObject:

    def __init__(this,
                 fullpath:Union[PathManager|None],
                 type:FileType,
                 size:Union[int|None],
                 mtime:Union[datetime|None],
                 exists:Union[bool|None]=None,
                 hidden:bool=False):
        this.fullpath=fullpath
        this.type=type
        this._size=size
        this._mtime=None
        this.mtime=mtime
        this.hidden=hidden
        this._exists = exists
        this.processed=False

    @property
    def absolute_path(this) -> str:
        return this.fullpath.absolute_path

    @property
    def relative_path(this) -> str:
        return this.fullpath.relative_path

    @property
    def containing_directory(this) -> str:
        return os.path.split(this.absolute_path)[0]

    @property
    def filename(this) -> str:
        return os.path.split(this.absolute_path)[1]

    def __eq__(this,other) -> bool:
        if type(other) == str:
            return (this.absolute_path==other) or (this.relative_path == other)
        elif isinstance(other,FileSystemObject):
            return this.relative_path == other.relative_path
        else:
            return False

    def __hash__(this) -> int:
        return hash(this.relative_path)

    def __str__(this) -> str:
        return this.relative_path

    def __repr__(this) -> str:
        return str(this)

    @property
    def is_remote(this) -> bool:
        for _,drive in get_rclone_remotes():
            if this.path.startswith(drive):
                return True
        return False

    @property
    def is_local(this) -> bool:
        return not this.is_remote

    @property
    def size(this) -> int:
        if (this._size is None) or (this._size<0):
            this.update_information()

        return this._size

    @property
    def exists(this) -> bool:
        if this._exists is None:
            output = subprocess.run(['rclone','lsf',this.absolute_path],stdout=subprocess.PIPE,stderr=subprocess.PIPE)

            return output.returncode == 0
        else:
            return this._exists

    @property
    def mtime(this) -> Union[datetime|None]:
        return this._mtime

    @mtime.setter
    def mtime(this, mtime: Union[datetime|None]) -> None:
        this._mtime = mtime if (mtime is None) or (mtime.tzinfo is not None) else mtime.replace(tzinfo=current_timezone())



    def update_information(this):
        output = subprocess.run(['rclone', 'lsjson', this.absolute_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if output.returncode == 0:
            file_stats = json.loads(output.stdout.decode())

            for s in file_stats:
                if s['Name'] == this.filename:
                    this._size = s['Size']
                    this.mtime = _fix_isotime(s['ModTime'])
                    this._exists = True

                    return

        this._exists = False




class PathException(Exception):
    ...

class MissingAbsolutePathException(PathException):

    def __init__(this, path,desc="Path"):
        super().__init__(f"{desc} {path} is not an absolute path.")

class PathOutsideRootException(PathException):

    def __init__(this,root,path):
        super().__init__(f"The path {path} is not rooted in {root}")


class PosixPathManager(PathManager):

    def __init__(this,path,root=None):
        bp = this.normalise(path if root is None else root)
        path =  this.normalise(path)

        if (this.is_relative(bp)):
            raise MissingAbsolutePathException(bp,"Basepath")

        if not this.is_root_of(path,bp):
            raise PathOutsideRootException(path, bp)

        super().__init__(path,bp)

    @classmethod
    def normalise(cls, path):
        path = super(PosixPathManager,PosixPathManager).normalise(path)
        tokens = cls.split(path)

        tokens[1:]= [t for t in tokens[1:] if t != "."]

        while ".." in tokens:
            idx = tokens.index("..")
            del tokens[max(idx-1,0):idx+1]

        if (tokens is None) or (len(tokens) == 0):
            return cls.PATH_SEPARATOR

        return cls.join(tokens)

    @property
    def relative_path(this):
        path = this.absolute_path

        if (this.is_under_root(path)):
            relpath = path[len(this.root):]
            if (len(relpath) == 0):
                relpath = "."
            elif relpath[0] == this.PATH_SEPARATOR:
                relpath=relpath[1:]

            return relpath
        raise PathOutsideRootException(this.root, this.absolute_path)

class NTPathManager(PathManager):

    @classmethod
    def get_volume(cls,path):
        path = path.strip()
        if (path.find(cls.VOLUME_SEPARATOR) > 0):
            volume = path.split(cls.VOLUME_SEPARATOR)[0]  + cls.VOLUME_SEPARATOR

            return volume if len(volume)>0 else None

        return None

    @classmethod
    def strip_volume(cls, path):
        vol = cls.get_volume(path)
        return path.lstrip(vol)

    @classmethod
    def is_absolute(cls, path):
        path = cls.strip_volume(path)

        return super(NTPathManager,NTPathManager).is_absolute(path)

    @classmethod
    def normalise(cls, path):
        path = super(PosixPathManager, PosixPathManager).normalise(path)
        tokens = cls.split(path)

        tokens[1:]= [t for t in tokens[1:] if t != "."]

        vol = cls.get_volume(path)

        min_idx = 1 if (vol is not None) and tokens[0].startswith(vol) else 0

        while ".." in tokens:
            idx = tokens.index("..")
            del tokens[max(idx-1,min_idx):idx+1]

        return cls.join(tokens)

    @classmethod
    def split(cls, path):
        vol = cls.get_volume(path)
        tokens = super(NTPathManager,NTPathManager).split(path)

        if (vol is not None) and  (vol.lower() == tokens[0].lower()):
            tokens[0] += cls.PATH_SEPARATOR

        return tokens

    @property
    def relative_path(this):
        path = this.absolute_path
        if (this.is_under_root(path)):
            path = path[len(this.root):]
            if (len(path)==0):
                return "."

            if (path[0]==this.PATH_SEPARATOR):
                path = path[1:]

            return path

        else:
            raise PathOutsideRootException(this.root,this.absolute_path) #Exception(f"The path {this.absolute_path} is not rooted in {this.root}")

    def cd(this, path):
        if path.startswith("/"): # or path.startswith("./"):
            path = this.join(this.root,path)

        super().cd(path)
class FileSystem(ABC):

    def __init__(this,path:str,*,path_manager:PathManager.__class__,cached:bool=False,force:bool=False):
        this._path=path_manager(path)
        # this.filter_callback = []
        this._cache:Any = []
        this._path_manager = path_manager
        this._cached=cached
        this._file_objects = {}

        if (not force) and (not this.exists(this.root)):
            raise FileNotFoundError(this.root)

    def __str__(this) -> str:
        return this.current_path

    def __repr__(this) -> str:
        return str(this)

    def update_file_object(this,path:PathManager,fo:Union[FileSystemObject|None]):
        p = path.relative_path
        if fo is None:
            if p in this._file_objects.keys():
                del this._file_objects[p]
        else:
            this._file_objects[p] = fo

    def get_file_object(this,path:PathManager) -> Union[FileSystemObject|None]:
        p = path.relative_path
        return this._file_objects[p] if p in this._file_objects.keys() else None

    @property
    def cached(this) -> bool:
        return this._cached

    @cached.setter
    def cached(this,value:bool) -> None:
        this._cached = value

    @property
    def base_path(this) -> str:
        return this._path.root

    @property
    def root(this)  -> str:
        return this.base_path

    @property
    def current_path(this)  -> str:
        return this._path.absolute_path

    @property
    def cwd(this)  -> str:
        return this.current_path

    @abstractmethod
    def _load(this) -> None:
        ...

    def load(this,force=True) -> None:
        if force or (this._cache is None) or (len(this._cache)==0):
            if (not this.cached) or force:
                this._load()

    @abstractmethod
    def ls(this, path:Union[str|None]=None):
        ...

    @abstractmethod
    def _find_dir_in_cache(this, dir:str):
        ...

    def cd(this,path) -> None:
        exists = this.exists(path) if not this.cached else this._find_dir_in_cache(path)

        if not PathManager.is_special_dir(path) and exists is None:
            raise ValueError(f"Directory {path} not found.")

        this._path.cd(path)

    def visit(this, path):
        return this._path.visit(path)

    @abstractmethod
    def exists(this,filename) -> bool:
        ...

    @abstractmethod
    def get_file(this,path:PathManager) -> FileSystemObject:
        ...

    def new_path(this,path:str,root:Union[str|None]=None) -> PathManager:
        return this._path_manager(path,root if root is not None else this.root)


class LocalFileSystem(FileSystem):

    def __init__(this,*args,**kwargs):
        if ("path_manager" not in kwargs) or (kwargs['path_manager'] is None):
            kwargs['path_manager'] = NTPathManager if is_windows() else PosixPathManager


        super().__init__(*args,**kwargs)

        #this.add_filter_callback(lambda x : x.type != FileType.OTHER) # remove "other" files

    def _load(this):
        if this.cached:
            this._cache = [ (this.new_path(path,root=this.base_path).relative_path, dirs, files) for path, dirs, files in os.walk(this.base_path)]

    def _find_local(this,path):
        dirs = []
        files = []

        path = this.new_path(path,this.root)

        try:
            for itm in os.scandir(path.absolute_path):
                if itm.is_dir():
                    dirs.append(itm.name)
                elif itm.is_file():
                    files.append(itm.name)

            return (path.absolute_path,dirs,files)
        except FileNotFoundError:
            return None
    def _find_dir_in_cache(this,dir):
        dir_to_search = this.new_path(dir,root=this.base_path).relative_path
        for path,dirs,files in this._cache:
            if (path==dir) or (path == dir_to_search):
                return (path,dirs,files)

        return None


    def ls(this, path=None):
        cp = this.current_path if path is None else path

        listdir = this._find_dir_in_cache(cp) if this.cached else this._find_local(cp)

        if listdir is None:
            return []

        _,dirs,files = listdir

        content = [this._make_local_filesystem_object(x,cp) for x in dirs+files]

        #content = [f for f in content if all([fn(f) for fn in this.filter_callback]) ]

        return content

    def exists(this,filename):
        p = this.visit(filename)

        return os.path.exists(p.absolute_path)

    def get_file(this,path:PathManager) -> FileSystemObject:
        p, name = os.path.split(path.relative_path)

        if p == "":
            p = "./"

        return this._make_local_filesystem_object(name,p)

    def _make_local_filesystem_object(this,filename:str, path:str) -> FileSystemObject:
        fullpath = this.new_path(PathManager.join(path,filename),root=this.root)

        if (this.cached):
            cached_fso = this.get_file_object(fullpath)
            if (cached_fso is not None):
                cached_fso.update_information()
                return cached_fso

        info = os.stat(fullpath.absolute_path)

        type = FileType.OTHER

        if stat.S_ISDIR(info.st_mode):
            type = FileType.DIR
        elif stat.S_ISREG(info.st_mode):
            type = FileType.REGULAR

        hidden = filename.startswith(".") or (
                    is_windows() and ((info.st_file_attributes & stat.FILE_ATTRIBUTE_HIDDEN) != 0))

        fso = FileSystemObject(fullpath,
                               type=type,
                               size=info.st_size,
                               mtime=datetime.fromtimestamp(info.st_mtime),
                               exists=True,
                               hidden=hidden)

        if (this.cached):
            this.update_file_object(fullpath,fso)

        return fso
class RemoteFileSystem(FileSystem):

    def __init__(this,*args,**kwargs):
        if ("path_manager" not in kwargs) or (kwargs['path_manager'] is None):
            kwargs['path_manager'] = NTPathManager

        super().__init__(*args,**kwargs)

    def _load(this):
        this._cache = rclone.ls(this.base_path,args=["-R"])

    def _find_dir_in_cache(this,dir):
        dir_to_search = this.new_path(dir, root=this.base_path).relative_path

        for itm in this._cache:
            remote_path = "/" + itm['Path']
            if (remote_path==dir) or (remote_path == dir_to_search):
                return itm

        return None

    def _dir(this,path):
        dir = []

        #if (path.startswith("/")): path = path[1:]

        items = this._cache if this.cached else rclone.ls(path.absolute_path)
        relpath = path.relative_path

        if (relpath == "."): relpath = ""


        for itm in items:
            p,tail = os.path.split(itm['Path'])

            if (p==relpath):
                dir.append(itm)

        return dir


    def ls(this, path=None):
        cp = this.current_path if path is None else path
        cp = this.new_path(cp, root=this.base_path)

        content = [this._make_remote_filesystem_object(x, cp.relative_path) for x in this._dir(cp)]
        #content = [f for f in content if all([fn(f) for fn in this.filter_callback])]

        return content

    def exists(this,filename):
        p = this.visit(filename)

        output = subprocess.run(
            ["rclone", "size", p.absolute_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8"
        )


        return "not found" not in output.stderr

    def get_file(this,path:PathManager) -> FileSystemObject:
        p,name = os.path.split(path.relative_path)

        parent_path = this.new_path(p)

        content = this._dir(parent_path)

        for itm in content:
            if itm['Name'] == name:
                return this._make_remote_filesystem_object(itm,parent_path.absolute_path)

        raise FileNotFoundError(f"No such file or directory: '{path}'")


    def _make_remote_filesystem_object(this,dic:dict, path:str):
        type = FileType.DIR if dic['IsDir'] else FileType.REGULAR

        fullpath = this.new_path(PathManager.join(path, dic['Name']), root=this.root)

        if (this.cached):
            cached_fso = this.get_file_object(fullpath)
            if (cached_fso is not None):
                cached_fso.update_information()
                return cached_fso

        mod_time = _fix_isotime(dic['ModTime']) #fixing mega.nz bug

        fso = FileSystemObject(fullpath,
                               type=type,
                               size=dic['Size'],
                               mtime=datetime.fromisoformat(mod_time),
                               exists=True)

        if (this.cached):
            this.update_file_object(fullpath,fso)

        return fso




def fs_auto_determine(path:str,parse_all:bool=False) -> FileSystem:
    head, tail = os.path.split(path)

    rclone_drives = [drive for _, drive in get_rclone_remotes()]
    rclone_drives += [r.lower() for r in rclone_drives]

    partitions = rclone_drives.copy()

    if (is_windows()):
        local_drives = [p.device.replace("\\", PathManager.PATH_SEPARATOR) for p in disk_partitions() if
                        p.fstype != "" and p.mountpoint != ""]
        partitions += local_drives + [r.lower() for r in local_drives]
    else:
        partitions += ['/']

    fullpath = path if parse_all else head

    for p in partitions:
        if fullpath.startswith(p):
            if (p in rclone_drives):
                return RemoteFileSystem(fullpath)
            else:
                return LocalFileSystem(fullpath)
def fs_autocomplete(path:str,min_chars:int=3) -> str:
    _, tail = os.path.split(path)

    if (len(tail)<min_chars):
        return None

    fs = fs_auto_determine(path)

    if (fs is not None):
        fs.cached = False
        ls = fs.ls()

        #return str(ls)

        for obj in ls:
            _,name = os.path.split(obj.absolute_path)

            if name.startswith(tail):
                return obj.absolute_path

def mkdir(path:FileSystemObject):
    output = subprocess.run(
        ["rclone", "mkdir", path.absolute_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8"
    )

    return output.returncode == 0