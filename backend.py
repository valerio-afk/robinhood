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
from typing import Union, List, Iterable, Callable, Dict, Tuple, Any
from enums import SyncMode, SyncStatus, ActionType, ActionDirection
from filesystem import FileType, FileSystemObject, FileSystem, fs_auto_determine, AbstractPath
from file_filters import UnixPatternExpasionFilter, RemoveHiddenFileFilter, FilterSet, FileFilter
from rclone_python.rclone import copy
from config import RobinHoodProfile
from synching import SynchingManager, BulkCopySynchingManager, _get_trigger_fn, AbstractSyncAction, NoSyncAction
from events import SyncEvent, RobinHoodBackend
import os
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



def find_dedupe(path: Union[str | FileSystem],
                        eventhandler: [RobinHoodBackend | None] = None
                        ) -> Iterable[AbstractSyncAction]:

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
                action = SynchingManager.make_action(a,b,type=ActionType.DELETE, direction=ActionDirection.SRC2DST)
                actions.append(action)

    _trigger("after_comparing", SyncEvent(actions))

    return actions



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

    if isinstance(src, str):
        src = fs_auto_determine(src, True)
        src.cached = True

    # Same as above, but with the destination path
    if isinstance(dest,  str):
        dest = fs_auto_determine(dest, True)
        dest.cached = True

    # Loads both directories caches

    src.load()
    dest.load()

    # define an  empty set of directories that will be needed below

    directories = set('.')

    # Asks rclone to compute the differences betweeen those two directories

    rclone_command = ['rclone', 'check', src.root, dest.root, '--combined', '-', '--size-only']

    report = subprocess.run(rclone_command, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)

    # Parse the result obtained  from rclone
    sync_changes = SynchingManager(src , dest)

    files = report.stdout.decode().splitlines()


    # for each line in the stdout
    for i, line in enumerate(files):
        # for more information regarding the output of rclone check, please refer to https://rclone.org/commands/rclone_check/
        tag, path = line.split(" ", maxsplit=1)

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

        # add directory to the set of directories
        directories.add(os.path.split(src_path.relative_path)[0])

        # By default, it's assumed that the standard way to transfer files is from source -> destination
        # Specific cases are treated below
        direction = ActionDirection.SRC2DST
        type = ActionType.NOTHING

        match tag:
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
                    type = ActionType.COPY #if source_object.type == FileType.REGULAR else ActionType.MKDIR
                # Other cases
                else:
                    # The presence of a past_dest_object doesn't necessary mean it actually existed. Let's doublecheck it
                    if past_dest_object.exists:
                        # Case 2: if paths coincides, it's been deleted
                        if source_object.relative_path == past_dest_object.relative_path:
                            type = ActionType.DELETE
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
                    type = ActionType.COPY #if dest_object.type == FileType.REGULAR else ActionType.MKDIR
                # Other cases
                else:
                    # The presence of a past_dest_object doesn't necessary mean it actually existed. Let's doublecheck it
                    if past_source_object.exists:
                        # Case 2: if paths coincides, it's been deleted
                        if dest_object.relative_path == past_source_object.relative_path:
                            type = ActionType.DELETE
                        else:
                            # Case 3: it's been moved
                            NotImplementedError("Not yet mate!")

            # * means file exists in both side but somehow differs - more needs to be done to determine what to copy where
            #TODO: detects deleted/moved files from cache
            case "*":
                if source_object.mtime.timestamp() >= dest_object.mtime.timestamp():
                    type = ActionType.UPDATE
                    direction = ActionDirection.SRC2DST
                else:
                    type = ActionType.UPDATE
                    direction = ActionDirection.DST2SRC
            # ! means there's been an error - maybe to leave this
            case "!":
                type = ActionType.UNKNOWN
            case "=":
                type = ActionType.NOTHING
                if profile.deep_comparisons:
                    if source_object.checksum != dest_object.checksum:
                        direction = ActionDirection.SRC2DST if source_object.mtime > dest_object.mtime else ActionDirection.DST2SRC
                        type = ActionType.UPDATE

        action = SynchingManager.make_action(source_object, dest_object, type=type, direction=direction)
        sync_changes.add_action(action)

    # add directories information to the list of action
    # not sure if these should also go in the file cache - not doing it at the moment

    for directory in directories:
        if len(directory) > 0:
            a = FileSystemObject(src.new_path(directory),type=FileType.DIR, exists=True)
            b = FileSystemObject(dest.new_path(directory), type=FileType.DIR, exists=True)
            sync_changes.add_action(NoSyncAction(a,b))

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

    def _tree_sort_fn(x:FileSystemObject):
        p = list(AbstractPath.split(x.a.absolute_path))
        return p

    sync_changes.sort(key=_tree_sort_fn)

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
                            ...
                    case ActionDirection.DST2SRC:
                        # whatever happens here is the same as before, but in the opposite direction
                        try:
                            bulky_copy_dst2src.add_action(itm)
                        except ValueError:
                            ...
                # if the action has not been added in any of the two bulks, then it's queued in the other action
                # manager that will take care of it
                if (itm not in bulky_copy_src2dst) and (itm not in bulky_copy_dst2src):
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