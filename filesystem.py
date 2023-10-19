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
from typing import Any, Union, List, Tuple, Type, Iterable, Dict
from abc import ABC, abstractmethod
from enum import Enum
from rclone_python import rclone
from datetime import datetime
from copy import copy
from psutil import disk_partitions
from config import get_cache_file
import os
import re
import stat
import subprocess
import json

# Checks whether RH is running under windows or not
is_windows = lambda: os.name == 'nt'

# Gets the current time zone (useful to get rid of naive datetime)
current_timezone = lambda: datetime.now().astimezone().tzinfo

UNITS = ("", "K", "M", "G", "T", "P", "E", "Z")


def _fix_isotime(time: str) -> str:
    '''
    The ISO time returned by rclone (at least when tested on MEGA) returns a format that datetime doesn't like
    This function fixes these issues

    :param time: a string with a ISO timestamp
    :return: A string with a re-formatted timestamp
    '''

    # Defines a regular expression to find the inconsistencies
    pattern = r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]*)?(\+[0-9]{2}:[0-9]{2}|[a-zA-Z])?"

    # Finds and match inconsistencies
    matches = re.search(pattern, time)

    # Fixes inconsistencies
    if matches[1] is not None:
        time = time.replace(matches[1], "")
    if (matches[2] is not None) and (not matches[2].startswith("+")):
        time = time.replace(matches[2], "")

    # Returns a new string with fixed timestamp
    return time


def sizeof_fmt(num: int, suffix: str = "B") -> str:
    '''
    Formats an integer representing the size of a file into something more human-readable format
    This function has been adapted from https://stackoverflow.com/questions/1094841/get-human-readable-version-of-file-size
    :param num: Size in bytes
    :param suffix: The suffix of what the used unit (B == Bytes)
    :return: A string with a human-readable format
    '''

    # If the size is 0, I prefer to have a dash rather 0B
    if (num == 0):
        return "-"

    # For each unit
    for unit in UNITS:
        # Checks if the current number is less than 1024 (ie do we need more division by 1024?)
        if abs(num) < 1024.0:
            # If not, returns the formated number with unit and suffix
            return f"{num:3.1f}{unit}{suffix}"
        # Otherwise, it divides the number by 1024
        num /= 1024.0

    # Units get to Zettabyte. Beyond that, it'll be Yottabytes and whatever...
    return f"{num:.1f}Y{suffix}"


def convert_to_bytes(value: float, unit: str) -> int:
    '''
    Converts a floating point representing a formated file sizee into int
    This conversion is not perfect, as 15.1KB may likely be rounded

    :param: The value to convert
    :param: It's current unit
    :return: a integer representing the size int bytes
    '''

    # Mathematically this, fuction performs opposite operations than the one above (sizeof_fmt)
    for i, u in enumerate(UNITS[1:]):
        if u in unit:
            return int(value * (1024 ** (i + 1)))

    return int(value)


def get_rclone_remotes() -> List[Tuple[str, ...]]:
    '''
    Get the list of remote directories from rclone

    :return: A list of tuples each containing the name and type
    '''

    # Run rclone via subprocess to get the list of remotes
    output = subprocess.run(
        ["rclone", "listremotes", "--long"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8"
    )

    # make the tuples and the list via 2 list comprehensions
    return [tuple([x.strip() for x in line.split(":")[::-1]]) for line in output.stdout.strip().splitlines()]


class AbstractPath(ABC):
    '''
    This class represents a generic path within an operating system
    I had to make this class because rclone uses a Windows-like format for the remote (e.g., mega:/).
    However, pathlib doesn't support Windows like with volume named with a longer string than a single letter.

    So, it was better to reimplement these classes for only the stuff I needed for this project
    '''

    PATH_SEPARATOR = '/'
    VOLUME_SEPARATOR = ":"

    def __init__(this, path: str, root: Union[str | None] = None):
        '''
        Instantiate a new path

        :param path: A string representing a path. If root is not provided, it must be absolute
        :param root: An absolute path representing the root (starting point) of the previous parameter.
                     If not provided, then root = path
        '''

        # Converts the root path (if provided) considering special directories .. and .
        this._basepath = this.normalise(path if root is None else root)

        # Checks if the path is relative
        if (this.is_relative(this._basepath)):
            # It's a problem because root cannot be an absolute path
            raise MissingAbsolutePathException(this._basepath, "Basepath")

        # Makes normalisation steps for the provided path as well
        path = this.normalise(path)

        # Checks if path is relative, ie path does not start with its root
        if not path.startswith(this._basepath):
            # In this case, path is the merging of root and itself
            this._path = this.normalise(this.join(this.root, path))
        else:
            # Otherwise, path is kept as is
            this._path = path

        # If path is absolute, it needs to be clear whether it's under the provided root, otherwise nothing will work
        if not this.root_is_parent_of(this._path):
            raise PathOutsideRootException(this.absolute_path, this.root)

    @classmethod
    def make_path(cls, path: str) -> AbstractPath:
        '''
        Static method to generate a path from a string. This method should implement something like the factory D.P.
        :param path: The path to make an object from
        :return: An object representing the path
        '''

        # This is a guess. If the path has a volume separator, then it's assumed it's NT-lile, else Posix-like
        suitable_class = NTAbstractPath if path.find(NTAbstractPath.VOLUME_SEPARATOR) >= 0 else PosixAbstractPath

        return suitable_class(path=".", root=path)

    @classmethod
    def as_posix(cls, p: str) -> str:
        '''
        I gave this method a nice name, because replacing_the_stupid_windows_backslashes_with_normal_slashes sounded kinda bad.
        I think you know understand what it does
        :param p: The path
        :return:
        '''
        return p.replace("\\", "/")

    @classmethod
    def join(cls, *args) -> Union[str | None]:
        '''
        Similar in concept as os.path.join
        :param args:
        :return: A merged path or none if the list is empty
        '''
        paths = args[0] if len(args) == 1 else args

        if (len(paths) == 0):
            return None

        r = paths[0]

        for i in range(1, len(paths)):
            # To avoid to have double slashes, each path part is checked whether they end/start with slash
            xx = r.endswith(cls.PATH_SEPARATOR)
            yy = paths[i].startswith(cls.PATH_SEPARATOR)

            # The operator ^ is the XOR operator.
            # If either of them have a slash, I simply concatenate them
            if (xx ^ yy):
                r += paths[i]
            elif (xx and yy):
                # if both have a slash, I remove the slash from the second part
                r += paths[i].lstrip(" /")
            else:
                # if neither of them has a slash, it's added
                if (cls.is_relative(paths[i])):
                    r += cls.PATH_SEPARATOR + paths[i]
                else:
                    # in the case a path part is an absolute path, well, everything done so far gets wiped out
                    r = paths[i]

        # Returns the merged path
        return r

    @classmethod
    def is_special_dir(cls, d: str) -> bool:
        '''
        Check if the provided argument is the dir '.' or '..'
        :param d: directory name to check
        :return: A boolean representing whether d is  either '.' or '..'
        '''
        return (d == ".") or (d == "..")

    @classmethod
    def is_absolute(cls, path: str) -> bool:
        """
        Check if the path is absolute. This method needs to be overridden for specific case, eg NT-like paths
        :param path: the path to check if it's absolute or not
        :return: TRUE if the path is absolute, FALSE otherwise
        """
        return path.startswith(cls.PATH_SEPARATOR)

    @classmethod
    def is_relative(cls, path: str) -> bool:
        """
        Check if the path is relative.
        :param path: the path to check if it's absolute or not
        :return: TRUE if the path is relative, FALSE otherwise
        """
        return not cls.is_absolute(path)

    @classmethod
    def normalise(cls, path: str) -> str:
        """
        Normalise the path were appropriate. This method needs to be overridden for specific case
        :param path: the path to normalise
        :return: Normalised path
        """
        return cls.as_posix(path)

    @classmethod
    def split(cls, path: str) -> List[str]:
        '''
        Split the path - opposite to the join method
        :param path: The path to split
        :return: A list containing directory and file names
        '''
        tokens = path.split(cls.PATH_SEPARATOR)

        if tokens[0] == '':
            tokens[0] = cls.PATH_SEPARATOR

        return [t for t in tokens if len(t) > 0]

    @classmethod
    def is_root_of(cls, path:str, root:str) -> bool:
        if cls.is_relative(path):
            return True

        spath = cls.split(cls.normalise(path))
        sroot = cls.split(root)

        if (len(sroot) <= len(spath)):
            for i, (x, y) in enumerate(zip(sroot, spath)):
                if (i == 0):
                    # this is also viable for posix paths because the first item will be just "/"
                    if (x.lower() != y.lower()) and (y != cls.PATH_SEPARATOR):
                        return False
                elif x != y:
                    return False

            return True
        else:
            return False

    def __copy__(this) -> AbstractPath:
        return type(this)(path=this.absolute_path, root=this.root)

    def __str__(this) -> str:
        return this.relative_path

    def __repr__(this) -> str:
        return this.absolute_path

    @property
    @abstractmethod
    def relative_path(this) -> str:
        """
        Abstract property to retrieve the relative path
        :return: The relative path
        """
        pass

    @property
    def absolute_path(this) -> str:
        '''
        Returns the absolute path
        :return: The absolute path
        '''

        # By convention, the property _path should already contain the absolute (and normalised) path
        return this._path

    @property
    def root(this) -> str:
        '''
        Gets the current root directory
        :return: The current root directory
        '''
        return this._basepath

    @root.setter
    def root(this, path: str) -> None:
        '''
        Set a new root to the Path
        :param path: An absolute path to the new root
        '''
        # when root is changed, the path needs to be re-rooted
        # therefore, the old relative path needs to be stored
        # to be used later to re-root the whole thing
        old_relpath = this.relative_path

        this._basepath = this.normalise(path)

        # Not sure why this is here. It works - who cares. I should've put comments earlier.
        if (this.is_absolute(this._path)):
            this._path = this.join(this._basepath, old_relpath)

    def cd(this, path: str) -> None:
        '''
        Change directory (similar to the cd command in any shell/terminal)
        :param path: The new (absolute/relative) path to navigate into
        '''

        # Firstly, we need to check if the new path is under the root of the current object
        # We cannot explore paths outside the root
        if (this.root_is_parent_of(path)):
            # If the path is absolute, then it simply replaces the _path property
            if (this.is_absolute(path)):
                this._path = this.normalise(path)
            else:
                # If it's relative, it gets joined and then normalised
                new_path = this.normalise(AbstractPath.join(this._path, path))

                # if the new path (after normalisation) is still under the root, we keep it
                # otherwise, if we are above the root (this can happen with a lot of ../../../)
                # we set the current path as root.
                this._path = new_path if this.root_is_parent_of(new_path) else this.root
        else:
            # In this case, the current path is the root (very similar to the above case)
            this._path = this.root

    def visit(this, path: str) -> AbstractPath:
        '''
        Very similar to the `cd` method, but it creates a new object instead of changing the current one
        :param path: The new path to visit
        :return: A new object rooted in the same  root but with the path provided as parameter
        '''

        c = copy(this)
        c.cd(path)
        return c

    def root_is_parent_of(this, path: str) -> bool:
        '''
        Very similar to the `is_root_of` static method. This method implements the instance version of it
        :param path: The path to check if it's under root of the current root path
        :return: TRUE if the path is under the current root, FALSE otherwise
        '''
        return this.is_root_of(path, this.root)

    def is_parent_of(this, path: str) -> bool:
        '''
        Very similar to the `is_parent_of_root` but considers parent directory as root
        :param path: The path to check if it's under root of the current path
        :return: TRUE if the path is under the current path, FALSE otherwise
        '''

        return this.is_root_of(path, this.absolute_path)


class FileType(Enum):
    '''
    Enumeration of the supported file types (OTHER is of all the other we don't give a crap of)
    '''
    OTHER = 0
    REGULAR = 1
    DIR = 2


class FileSystemObject:
    '''
    This class represents any suitable object in a file system (in our case, mainly regular files and directories)
    '''

    def __init__(this,
                 fullpath: Union[AbstractPath | None],
                 *,
                 type: FileType,
                 size: Union[int | None] = None,
                 mtime: Union[datetime | None] = None,
                 exists: Union[bool | None] = None,
                 checksum: Union[str | None] = None,
                 hidden: bool = False):
        """
        :param fullpath: Full path to the FS object
        :param type: Type of the file (see `FileType` enumeration)
        :param size: Size (in bytes) of the file if known, None otherwise
        :param mtime: Timestamp of the last modification time if known, None otherwise
        :param exists: TRUE if the object truly exists, FALSE otherwise (you can have a local file that doesn't exist remotely)
        :param hidden: TRUE if it's a hidden file (according to the definition of the hosting OS), FALSE otherwise.
        """
        this.fullpath = fullpath
        this.type = type
        this._size = size
        this._mtime = mtime
        this.hidden = hidden
        this._exists = exists
        this._checksum = checksum

    @property
    def absolute_path(this) -> str:
        """Gets the absolute path of the fs object"""
        return this.fullpath.absolute_path

    @property
    def relative_path(this) -> str:
        """Gets the relative path of the fs object"""
        return this.fullpath.relative_path

    @property
    def containing_directory(this) -> str:
        """Gets the containing directory of the FS object (extracted from its absolute path)"""
        return os.path.split(this.absolute_path)[0]

    @property
    def filename(this) -> str:
        """Gets the file- or directory name of the fs object"""
        return os.path.split(this.absolute_path)[1]

    @property
    def is_remote(this) -> bool:
        """
        Checks if the fs object is rooted in a remote drive (doesn't check if it exists remotely)
        :return: TRUE if it's in any of the remote drives, FALSE otherwise
        """
        for _, drive in get_rclone_remotes():
            if this.absolute_path.startswith(drive):
                return True
        return False

    @property
    def is_local(this) -> bool:
        """
        Checks if the fs object is rooted in a local drive (doesn't check if it exists remotely)
        :return: TRUE if it's in any of the local drives, FALSE otherwise
        """
        return not this.is_remote

    @property
    def size(this) -> Union[int | None]:
        """
        Gets the file size
        :return: The size in bytes of the fs object. It's set by -1 if it's the size of a directory
        """

        if (not this.exists) or (this.type == FileType.DIR):
            return None

        if (this._size is None) or (this._size < 0):
            this.update_information()

        return this._size

    @property
    def exists(this) -> bool:
        '''
        Checks if a file system object exists
        :return: TRUE if the object exists, FALSE otherwise
        '''

        if this._exists is None:
            this.update_information()

        return this._exists

    @property
    def mtime(this) -> Union[datetime | None]:
        """Gets the modification time of the filesystem object"""
        if this._mtime is None:
            this.update_information()

        return this._mtime

    @mtime.setter
    def mtime(this, mtime: Union[datetime | None]) -> None:
        """
        Sets the modification time of the fs object
        :param mtime: An object of type datetime for the new modification t ime
        """
        this._mtime = mtime if (mtime is None) or (mtime.tzinfo is not None) else mtime.replace(
            tzinfo=current_timezone())

    @property
    def has_checksum(this) -> bool:
        return this._checksum is not None

    @property
    def checksum(this) -> Union[str | None]:
        if this.type == FileType.DIR:
            return None

        if not this.has_checksum:
            args = ["rclone", "hashsum", "md5", this.absolute_path]

            if this.is_remote:
                args.append(["--download"])

            output = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            if output.returncode == 0:
                this._checksum = output.stdout.decode().split(" ")[0]

        return this._checksum

    @checksum.setter
    def checksum(this, value: str) -> None:
        """
        This can be used in those cases the checksum has been precomputed

        :param value: Precomputed checksum value
        :return:
        """
        this._checksum = value

    def __eq__(this, other) -> bool:
        if type(other) == str:
            return (this.absolute_path == other) or (this.relative_path == other)
        elif isinstance(other, FileSystemObject):
            return this.relative_path == other.relative_path
        else:
            return False

    def __hash__(this) -> int:
        return hash(this.relative_path)

    def __str__(this) -> str:
        return this.relative_path

    def __repr__(this) -> str:
        return str(this)

    def update_information(this) -> None:
        """Update the information about the file system object, eg size, modificafion time and its existance"""

        # Using rclone is the best way to have this information formated in the  same way, regardless if we have a local
        # or remote file/directory
        output = subprocess.run(['rclone', 'lsjson', this.absolute_path], stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)

        # If rclone returns code is non-zero, then the object doesn't exist
        if output.returncode == 0:
            # If it does exist, then the new information are used to update the current object status
            file_stats = json.loads(output.stdout.decode())

            for s in file_stats:
                if s['Name'] == this.filename:
                    this._size = s['Size']
                    this.mtime = datetime.fromisoformat(_fix_isotime(s['ModTime']))
                    this._exists = True
        else:
            this._exists = False

    def to_dict(this) -> Dict[str, Any]:
        return {
            "path": this.relative_path,
            "type": this.type.value,
            "size": this.size,
            "mtime": this.mtime.timestamp(),
            "exists": this.exists,
            "checksum": this._checksum,
            "hidden": this.hidden
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any], *, mtime: Union[int | None] = None) -> FileSystemObject:

        d["mtime"] = mtime if d["mtime"] is None else d['mtime']

        if d['mtime'] is not None:
            d['mtime'] = datetime.fromtimestamp(d['mtime'])

        return FileSystemObject(**d)


class PathException(Exception):
    """An exception related to problems with Paths"""
    ...


class MissingAbsolutePathException(PathException):
    """An exception generated when an absolute path is missing"""

    def __init__(this, path, desc="Path"):
        super().__init__(f"{desc} {path} is not an absolute path.")


class PathOutsideRootException(PathException):
    """An exception raised when someone wants to go beyond the allowed boundaries of the file system
    (the root parameter in AbsolutePath) sets a boundary and no one can go above that.
    """

    def __init__(this, root, path):
        super().__init__(f"The path {path} is not rooted in {root}")


class PosixAbstractPath(AbstractPath):
    """
    Extends the Abstract path for Unix-like path management
    Most of the functionality needed for this class are in the parent class.
    It's required to adapt a few things to make it work with POSIX paths
    """

    def __init__(this, path: str, root: Union[str | None] = None):
        bp = this.normalise(path if root is None else root)
        path = this.normalise(path)

        if (this.is_relative(bp)):
            raise MissingAbsolutePathException(bp, "Basepath")

        if not this.is_root_of(path, bp):
            raise PathOutsideRootException(path, bp)

        super().__init__(path, bp)

    @classmethod
    def normalise(cls, path: str) -> str:
        path = super(PosixAbstractPath, PosixAbstractPath).normalise(path)
        tokens = cls.split(path)

        tokens[1:] = [t for t in tokens[1:] if t != "."]

        while ".." in tokens:
            idx = tokens.index("..")
            del tokens[max(idx - 1, 0):idx + 1]

        if (tokens is None) or (len(tokens) == 0):
            return cls.PATH_SEPARATOR

        return cls.join(tokens)

    @property
    def relative_path(this) -> str:
        path = this.absolute_path

        if (this.root_is_parent_of(path)):
            relpath = path[len(this.root):]
            if (len(relpath) == 0):
                relpath = "."
            elif relpath[0] == this.PATH_SEPARATOR:
                relpath = relpath[1:]

            return relpath
        raise PathOutsideRootException(this.root, this.absolute_path)


class NTAbstractPath(AbstractPath):
    """
    Extends the Abstract path for Windows-like path management
    Some of the functionality needed for this class are in the parent class.
    It's required to adapt a few things to make it work with NT paths
    """

    @classmethod
    def get_volume(cls, path: str) -> [str | None]:
        """
        Gets the volume from the path
        :param path: The path from where to get the volume of the drive
        :return: the volume of where the path is rooted, None otherwise (thing of certain relative paths)
        """
        path = path.strip()
        if (path.find(cls.VOLUME_SEPARATOR) > 0):
            volume = path.split(cls.VOLUME_SEPARATOR)[0] + cls.VOLUME_SEPARATOR

            return volume if len(volume) > 0 else None

        return None

    @classmethod
    def strip_volume(cls, path: str) -> str:
        """
        Removes the volume from the given path
        :param path: The path to remove the volume from
        :return: A new path without the volume
        """
        vol = cls.get_volume(path)
        return path.lstrip(vol)

    @classmethod
    def is_absolute(cls, path):
        path = cls.strip_volume(path)

        return super(NTAbstractPath, NTAbstractPath).is_absolute(path)

    @classmethod
    def normalise(cls, path):
        path = super(PosixAbstractPath, PosixAbstractPath).normalise(path)
        tokens = cls.split(path)

        tokens[1:] = [t for t in tokens[1:] if t != "."]

        vol = cls.get_volume(path)

        min_idx = 1 if (vol is not None) and tokens[0].startswith(vol) else 0

        while ".." in tokens:
            idx = tokens.index("..")
            del tokens[max(idx - 1, min_idx):idx + 1]

        return cls.join(tokens)

    @classmethod
    def split(cls, path):
        vol = cls.get_volume(path)
        tokens = super(NTAbstractPath, NTAbstractPath).split(path)

        if (vol is not None) and (vol.lower() == tokens[0].lower()):
            tokens[0] += cls.PATH_SEPARATOR

        return tokens

    @property
    def relative_path(this):
        path = this.absolute_path
        if (this.root_is_parent_of(path)):
            path = path[len(this.root):]
            if (len(path) == 0):
                return "."

            if (path[0] == this.PATH_SEPARATOR):
                path = path[1:]

            return path

        else:
            raise PathOutsideRootException(this.root, this.absolute_path)

    def cd(this, path):
        if path.startswith("/"):
            path = this.join(this.root, path)

        super().cd(path)


class FileSystem(ABC):
    """
    This object represents an abstract file system
    It contains some useful functionality (eg caching) for the inherited classes
    """

    def __init__(this, path: str, *, path_manager: Type[AbstractPath], cached: bool = False, force: bool = False):
        """

        :param path: The root path of the file system
        :param path_manager: Path convention to use (POSIX- or NT-like)
        :param cached: Whether to cache content or not
        :param force: Force to check the existence of the root directory
        """
        this._path = path_manager(path)
        # Directory tree cache
        this._tree_cache: Any = []

        # File System Object cache
        this._file_objects_cache: Dict[str, FileSystemObject] = {}
        this._previous_file_objects_cache: Dict[str, FileSystemObject] = {}

        # The path manager is a concerete subtype of AbstractPath that is specialised in managing paths in
        # specific environments/cases (e.g., POSIXPaths)
        this._path_manager = path_manager

        this._cached = cached

        if (not force) and (not this.exists(this.root)):
            raise FileNotFoundError(this.root)

    @abstractmethod
    def _load(this) -> None:
        """
        Loads the cache into memory.
        This method needs to be implemented becaue each subclass can load and represents cache differently.
        """
        ...

    @abstractmethod
    def _find_dir_in_cache(this, dir: str) -> Union[Any | None]:
        """
        Finds a directory and its content in the cache
        :param dir: directory to search in the cache
        :return: An iterable if the directory exists, None otherwise
        """
        ...

    @abstractmethod
    def ls(this, path: Union[str | None] = None) -> Iterable[FileSystemObject]:
        """
        Returns the content of the path. If path is not provided, returns the content of the current working directory
        :param path: The path where to list its content. None will return the content of the cwd
        :return: An iterable of FileSystemObjects representing the content of the path
        """
        ...

    @abstractmethod
    def exists(this, filename) -> bool:
        """
        Checks if a file or directory exists
        :param filename: File or directory name to check its existance
        :return: TRUE if exists, FALSE otherwise
        """
        ...

    @abstractmethod
    def get_file(this, path: AbstractPath) -> FileSystemObject:
        """
        Returns a FileSystemObject from path. The FileSystemObject contains useful information about the file/directory
        :param path: The path to get information from
        :return: A FileSystemObject of representing path
        """
        ...

    def set_file(this, path: AbstractPath, fo: Union[FileSystemObject | None]) -> None:
        """
        Sets updated information of a specific file system object
        :param path: Path of the file/directory
        :param fo: Updated information. If None, the entry in the cache will be removed
        """

        if not AbstractPath.is_root_of(path.absolute_path, this.root):
            raise ValueError(f"{fo.absolute_path} is not rooted in this file system ({this.root})")

        p = path.relative_path
        if fo is None:
            if p in this._file_objects_cache.keys():
                del this._file_objects_cache[p]
        else:
            this._file_objects_cache[p] = fo

    @property
    def cached(this) -> bool:
        return this._cached

    @cached.setter
    def cached(this, value: bool) -> None:
        this._cached = value

    @property
    def base_path(this) -> str:
        return this._path.root

    @property
    def root(this) -> str:
        return this.base_path

    @property
    def current_path(this) -> str:
        return this._path.absolute_path

    @property
    def cwd(this) -> str:
        """
        Gets the current working directory (that can be different than the root if `cd` method has been used)
        :return: A string representing the current working directory
        """
        return this.current_path

    def __str__(this) -> str:
        return this.current_path

    def __repr__(this) -> str:
        return str(this)

    def _get_fso_from_cache(this, path: AbstractPath) -> Union[FileSystemObject | None]:
        """
        Retrieve a file system object from the file object cache of the object
        :param path: Path of the file/directory
        :return: A file system object of the provided path, None if not found
        """
        p = path.relative_path
        return this._file_objects_cache[p] if p in this._file_objects_cache.keys() else None

    def _load_previous_file_system_objects_cache(this) -> None:
        cache_filename = get_cache_file(this.root)

        if not os.path.exists(cache_filename):
            return

        with open(cache_filename, 'r') as h:
            d = json.load(h)
            if d['root'] != this.root:
                return

        files = d.setdefault('files', [])

        fsos = {}

        for f in files:
            p = f['path']
            del f['path']

            f['fullpath'] = this.new_path(p)

            fsos[p] = FileSystemObject.from_dict(f, mtime=d['timestamp'])

        this._previous_file_objects_cache = fsos

    def get_previous_version(this, path: AbstractPath, match_fullpath=True) -> Union[FileSystemObject | None]:
        """
        Finds a file system object inside the cache obtained from a previous run of the program
        :param path: Path of the file to check if it was previously found
        :param match_fullpath: If TRUE, if compares the full relative path. If FALSE, just the file name
                               As there could be multiple matches, only the first one will be returned
        :return:
        """
        if match_fullpath:
            p = path.relative_path
            return this._previous_file_objects_cache[p] if p in this._previous_file_objects_cache.keys() else None
        else:
            fname = AbstractPath.split(path.absolute_path)[-1]

            for p, fso in this._previous_file_objects_cache.items():
                if os.path.split(p)[-1] == fname:
                    return fso

            return None

    def load(this, force=True) -> None:
        """
        Loads the cache into memory
        :param force: if TRUE, loads the cache even if the cache is full
        """

        if force:
            # load the file system object cache from a previous run
            this._load_previous_file_system_objects_cache()
            # manages the tree cache
            if (this._tree_cache is None) or (len(this._tree_cache) == 0):
                if (not this.cached) or force:
                    this._load()

    def cd(this, path) -> None:
        """
        Change the current working directory
        :param path: Path to go
        """
        exists = this.exists(path) if not this.cached else this._find_dir_in_cache(path)

        if not AbstractPath.is_special_dir(path) and exists is None:
            raise ValueError(f"Directory {path} not found.")

        this._path.cd(path)

    def visit(this, path):
        """
        Returns a new Path located at the specified location
        :param path: New path to visit
        :return: A Path located in the specified location
        """
        return this._path.visit(path)

    def walk(this, path: Union[AbstractPath | None] = None) -> Iterable[FileSystemObject]:
        '''
        Iterate over all files recusiverly from the path (if provided)
        :param path: The path to start walking from. If not specified, the current working  directory is used
        :return: A generator yielding FileSystemObjecs
        '''
        cwd = this.current_path if path is None else path

        dirs = [cwd]

        while len(dirs) > 0:
            d = dirs.pop()

            for fso in this.ls(d):
                match fso.type:
                    case FileType.DIR:
                        dirs.append(fso.absolute_path)
                    case FileType.REGULAR:
                        yield fso

    def new_path(this, path: str, root: Union[str | None] = None) -> AbstractPath:
        """
        Return a new *AbstractPath object rooted `root` (if specified, else this.root is used instead)
        :param path: A string representing a path
        :param root: The root of this path. If not specified (None), this.root is used instead
        :return: An *AbstractPath object representing path
        """
        return this._path_manager(path, root if root is not None else this.root)

    def flush_file_object_cache(this) -> None:
        """
        Flushes changes of the file system objects into cache
        """

        if not this.cached or (this._file_objects_cache is None) or (len(this._file_objects_cache) == 0):
            return

        # Some files in the previous cache can have some useful information to be imported (eg, md5 hash)
        for path in this._previous_file_objects_cache:
            current = this._previous_file_objects_cache[path]
            previous = this.get_previous_version(current.fullpath)

            if (previous is not None) and (current is not None):
                if (previous.mtime.timestamp() == current.mtime.timestamp()) and \
                        (previous.size == current.size) and \
                        previous.has_checksum and (not current.has_checksum):
                    current.checksum = previous.checksum

        keys = sorted(this._file_objects_cache.keys(), key=lambda path: (len(AbstractPath.split(path)), path))

        fsos = [this._file_objects_cache[k].to_dict() for k in keys]

        cache_filename = get_cache_file(this.root)

        parent, _ = os.path.split(cache_filename)

        if not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)

        with open(cache_filename, "w") as h:
            json.dump({
                "root": this.root,
                "timestamp": datetime.now().timestamp(),
                "files": fsos
            }, h)


class LocalFileSystem(FileSystem):

    def __init__(this, *args, **kwargs):
        if ("path_manager" not in kwargs) or (kwargs['path_manager'] is None):
            kwargs['path_manager'] = NTAbstractPath if is_windows() else PosixAbstractPath

        super().__init__(*args, **kwargs)

        # this.add_filter_callback(lambda x : x.type != FileType.OTHER) # remove "other" files

    def _load(this):
        if this.cached:
            this._cache = [(this.new_path(path, root=this.base_path).relative_path, dirs, files) for path, dirs, files
                           in os.walk(this.base_path)]

    def _find_local(this, path):
        dirs = []
        files = []

        path = this.new_path(path, this.root)

        try:
            for itm in os.scandir(path.absolute_path):
                if itm.is_dir():
                    dirs.append(itm.name)
                elif itm.is_file():
                    files.append(itm.name)

            return (path.absolute_path, dirs, files)
        except FileNotFoundError:
            return None

    def _find_dir_in_cache(this, dir):
        dir_to_search = this.new_path(dir, root=this.base_path).relative_path
        for path, dirs, files in this._cache:
            if (path == dir) or (path == dir_to_search):
                return (path, dirs, files)

        return None

    def ls(this, path=None):
        cp = this.current_path if path is None else path

        listdir = this._find_dir_in_cache(cp) if this.cached else this._find_local(cp)

        if listdir is None:
            return []

        _, dirs, files = listdir

        content = [this._make_local_filesystem_object(x, cp) for x in dirs + files]

        # content = [f for f in content if all([fn(f) for fn in this.filter_callback]) ]

        return content

    def exists(this, filename):
        p = this.visit(filename)

        return os.path.exists(p.absolute_path)

    def get_file(this, path: AbstractPath) -> FileSystemObject:
        p, name = os.path.split(path.relative_path)

        if p == "":
            p = "./"

        return this._make_local_filesystem_object(name, p)

    def _make_local_filesystem_object(this, filename: str, path: str) -> FileSystemObject:
        fullpath = this.new_path(AbstractPath.join(path, filename), root=this.root)

        if (this.cached):
            cached_fso = this._get_fso_from_cache(fullpath)
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
            this.set_file(fullpath, fso)

        return fso


class RemoteFileSystem(FileSystem):

    def __init__(this, *args, **kwargs):
        if ("path_manager" not in kwargs) or (kwargs['path_manager'] is None):
            kwargs['path_manager'] = NTAbstractPath

        super().__init__(*args, **kwargs)

    def _load(this):
        this._cache = rclone.ls(this.base_path, args=["-R"])

    def _find_dir_in_cache(this, dir):
        dir_to_search = this.new_path(dir, root=this.base_path).relative_path

        for itm in this._cache:
            remote_path = "/" + itm['Path']
            if (remote_path == dir) or (remote_path == dir_to_search):
                return itm

        return None

    def _dir(this, path):
        dir = []

        # if (path.startswith("/")): path = path[1:]

        items = this._cache if this.cached else rclone.ls(path.absolute_path)
        relpath = path.relative_path

        if (relpath == "."): relpath = ""

        for itm in items:
            p, tail = os.path.split(itm['Path'])

            if (p == relpath):
                dir.append(itm)

        return dir

    def ls(this, path=None):
        cp = this.current_path if path is None else path
        cp = this.new_path(cp, root=this.base_path)

        content = [this._make_remote_filesystem_object(x, cp.relative_path) for x in this._dir(cp)]
        # content = [f for f in content if all([fn(f) for fn in this.filter_callback])]

        return content

    def exists(this, filename):
        p = this.visit(filename)

        output = subprocess.run(
            ["rclone", "size", p.absolute_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8"
        )

        return "not found" not in output.stderr

    def get_file(this, path: AbstractPath) -> FileSystemObject:
        p, name = os.path.split(path.relative_path)

        parent_path = this.new_path(p)

        content = this._dir(parent_path)

        for itm in content:
            if itm['Name'] == name:
                return this._make_remote_filesystem_object(itm, parent_path.absolute_path)

        raise FileNotFoundError(f"No such file or directory: '{path}'")

    def _make_remote_filesystem_object(this, dic: dict, path: str):
        type = FileType.DIR if dic['IsDir'] else FileType.REGULAR

        fullpath = this.new_path(AbstractPath.join(path, dic['Name']), root=this.root)

        if (this.cached):
            cached_fso = this._get_fso_from_cache(fullpath)
            if (cached_fso is not None):
                cached_fso.update_information()
                return cached_fso

        mod_time = _fix_isotime(dic['ModTime'])  # fixing mega.nz bug

        fso = FileSystemObject(fullpath,
                               type=type,
                               size=dic['Size'],
                               mtime=datetime.fromisoformat(mod_time),
                               exists=True)

        if this.cached:
            this.set_file(fullpath, fso)

        return fso


def fs_auto_determine(path: str, parse_all: bool = False) -> FileSystem:
    head, tail = os.path.split(path)

    rclone_drives = [drive for _, drive in get_rclone_remotes()]
    rclone_drives += [r.lower() for r in rclone_drives]

    partitions = rclone_drives.copy()

    if (is_windows()):
        local_drives = [p.device.replace("\\", AbstractPath.PATH_SEPARATOR) for p in disk_partitions() if
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


def fs_autocomplete(path: str, min_chars: int = 3) -> str:
    _, tail = os.path.split(path)

    if (len(tail) < min_chars):
        return None

    fs = fs_auto_determine(path)

    if (fs is not None):
        fs.cached = False
        ls = fs.ls()

        # return str(ls)

        for obj in ls:
            _, name = os.path.split(obj.absolute_path)

            if name.startswith(tail):
                return obj.absolute_path


def mkdir(path: FileSystemObject):
    output = subprocess.run(
        ["rclone", "mkdir", path.absolute_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8"
    )

    return output.returncode == 0
