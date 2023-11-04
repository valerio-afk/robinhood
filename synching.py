from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import List, Any, Union, Callable, Dict, Iterable, AsyncIterable
from datetime import datetime
from dataclasses import dataclass
from filesystem import AbstractPath
from filesystem import convert_to_bytes, FileSystemObject, FileSystem, FileType
from enums import ActionDirection, SyncStatus, ActionType
from events import SyncEvent, RobinHoodBackend
from bigtree import Node, add_dict_to_tree_by_path, preorder_iter, postorder_iter, find_path


import sys
sys.path.append("/home/tuttoweb/Documents/repositories/pyrclone")
from pyrclone import rclone
from pyrclone.jobs import RCloneTransferJob, RCJobStatus, RCloneTransferDetails


def _get_trigger_fn(eventhandler: Union[SyncEvent | None] = None) -> Callable[[str, SyncEvent], None]:
    def _trigger(mtd: str, e: SyncEvent) -> None:
        if isinstance(eventhandler, RobinHoodBackend):
            try:
                fn = getattr(eventhandler, mtd)
                fn(e)
            except AttributeError:
                ...  # event does not exist

    return _trigger


# class SyncComparisonEvent(SyncEvent):
#
#     def __init__(this, src_path=None, dest_path=None):
#         super().__init__((src_path, dest_path))
#
#     @property
#     def source_path(this):
#         return this.value[0]
#
#     @property
#     def destination_path(this):
#         return this.value[1]


class SyncDirectionNotPermittedException(Exception):
    ...

class AbstractSyncAction(ABC):

    def __init__(this,
                 a: FileSystemObject,
                 b: FileSystemObject,
                 type: ActionType = ActionType.NOTHING,
                 direction: Union[ActionDirection | None] = None):

        this.a = a
        this.b = b
        this._type:ActionType = type
        this._direction:Union[ActionDirection|None] = direction
        this._update = None
        this.excluded=False
        this._status = SyncStatus.NOT_STARTED

        this._validate_action_direction()

    @property
    def type(this):
        return this._type

    @property
    def direction(this):
        return this._direction

    @property
    def get_one_path(this) -> FileSystemObject:
        return this.a if this.b is None else this.b

    @property
    def status(this) -> SyncStatus:
        return this._status

    @property
    def update(this) -> Union[RCloneTransferJob | None]:
        return this.get_update()

    # @update.setter
    # def update(this, value):
    #     this._update = value

    @abstractmethod
    async def apply_action(this, rclone_engine: rclone) -> None:
        ...

    @abstractmethod
    def _repr_type(this) -> str:
        ...

    @abstractmethod
    async def update_status(this, rclone_engine:rclone) -> SyncStatus:
        ...


    def _validate_action_direction(this):
        if this.direction is not None:
            if (this.direction == ActionDirection.SRC2DST) and (not this.a.exists):
                raise SyncDirectionNotPermittedException(
                    "Destination file does not exist to support a source-to-destination operation")
            elif (this.direction == ActionDirection.DST2SRC) and (not this.b.exists):
                raise SyncDirectionNotPermittedException(
                    "Source file does not exist to support a destination-to-source operation")

        if this.direction == ActionDirection.BOTH and not this.type.supports_both:
            raise SyncDirectionNotPermittedException("The provided action type does not support bidirectional changes")

    def swap_direction(this) -> None:

        new_dir = ActionDirection.DST2SRC if this.direction == ActionDirection.SRC2DST else ActionDirection.SRC2DST

        fs = this.a if new_dir == ActionDirection.SRC2DST else this.b

        if not fs.exists:
            raise SyncDirectionNotPermittedException(
                f"Action swapping not possible because {fs.absolute_path} does not exist")

        this._direction = new_dir


    def apply_both_sides(this) -> None:
        if not this.type.supports_both:
            raise SyncDirectionNotPermittedException(f"This action does not support bidirectional changes")

        if not this.a.exists:
            raise FileNotFoundError(f"The file {this.a.absolute_path} does not exist")

        if not this.b.exists:
            raise FileNotFoundError(f"The file {this.b.absolute_path} does not exist")

        this._direction = this.direction.BOTH

    def get_update(this) -> Union[RCloneTransferJob | None]:
        return this._update

    def __str__(this) -> str:
        action_type = this._repr_type()

        if (this.type != ActionType.NOTHING):
            action_type = str(this.direction) + action_type

        return f"{this.a} {action_type} {this.b}"

    def __repr__(this) -> str:
        return str(this)


class NoSyncAction(AbstractSyncAction):

    def __init__(this, a: FileSystemObject, b: FileSystemObject):
        super().__init__(a, b, type=ActionType.NOTHING)

    def _repr_type(this) -> str:
        return "-"

    async def apply_action(this, rclone_engine: rclone) -> None:
        ...
    async def update_status(this, rclone_engine:rclone) -> None:
        this._status = SyncStatus.SUCCESS

    def swap_direction(this) -> None:
        ...

    def apply_both_sides(this) -> None:
        ...

    def _validate_action_direction(this):
        ...


class CopySyncAction(AbstractSyncAction):
    def __init__(this, a: FileSystemObject, b: FileSystemObject, direction=ActionDirection.SRC2DST):
        type = ActionType.UPDATE if (a.exists and b.exists) else ActionType.COPY

        super().__init__(a, b, type=type, direction=direction)
        this._progress = None
        this._jobid = None

    @property
    def is_updating(this):
        return this.type == ActionType.UPDATE

    def _repr_type(this) -> str:
        return "+" if this.is_updating else "*"

    async def apply_action(this, rclone_engine: rclone) -> None:
        if this.excluded:
            return

        src_root = this.a.fullpath.root
        src_path = this.a.fullpath.relative_path

        dst_root = this.b.fullpath.root
        dst_path = this.b.fullpath.relative_path

        if this.direction == ActionDirection.DST2SRC:
            src_root, dst_root = dst_root, src_root
            src_path, dst_path = dst_path, src_path

        if (this.a.type == FileType.REGULAR) and (this.b.type == FileType.REGULAR):
            this._jobid = await rclone_engine.copy_file(src_root, src_path, dst_root, dst_path)

    async def update_status(this, rclone_engine:rclone) -> None:
        if this._jobid in rclone_engine.jobid_to_be_started:
            this._status = SyncStatus.NOT_STARTED
        else:
            async for j in rclone_engine.started_jobs:
                if j.id == this._jobid:
                    if j.status == RCJobStatus.IN_PROGRESS:
                        this._update = await rclone_engine.get_transfer_status(this._jobid)
                        this._status = SyncStatus.IN_PROGRESS
                        return
                    elif j.status == RCJobStatus.FINISHED:
                        this._status = SyncStatus.SUCCESS
                        return
                    else:
                        this._status = SyncStatus.FAILED
                        return

            this._status = SyncStatus.SUCCESS


class DeleteSyncAction(AbstractSyncAction):
    def __init__(this,
                 a: FileSystemObject,
                 b: FileSystemObject,
                 direction: Union[ActionDirection | None] = None):

        super().__init__(a, b, type=ActionType.DELETE, direction=direction)

    def _repr_type(this) -> str:
        return "x"

    def _validate_action_direction(this):
        if this is not None:
            x = this.a.exists
            y = this.b.exists

            if (this.direction == ActionDirection.SRC2DST) and (not y):
                raise SyncDirectionNotPermittedException(
                    "Destination file does not exist to support a source-to-destination operation")
            elif (this.direction == ActionDirection.DST2SRC) and (not x):
                raise SyncDirectionNotPermittedException(
                    "Source file does not exist to support a destination-to-source operation")
            elif (this.direction == ActionDirection.BOTH) and (not x) and (not y):
                raise SyncDirectionNotPermittedException("Both files must exist to support bidirectional action")

    async def apply_action(this, rclone_engine: rclone) -> None:
        if this.excluded:
            return

        fn = rclone_engine.delete_file

        if (this.a.type == FileType.DIR) and (this.b.type == FileType.DIR):
            fn = rclone_engine.rmdir

        if (this.direction == ActionDirection.SRC2DST) or (this.direction == ActionDirection.BOTH):
            await fn(this.b.fullpath.root, this.b.fullpath.relative_path)

        if (this.direction == ActionDirection.DST2SRC) or (this.direction == ActionDirection.BOTH):
            await fn(this.a.fullpath.root, this.a.fullpath.relative_path)

    async def update_status(this, rclone_engine:rclone) -> None:
        src_side = True
        dst_side = True

        if (this.direction == ActionDirection.SRC2DST) or (this.direction == ActionDirection.BOTH):
            dst_side = not (await rclone_engine.exists(this.b.fullpath.root, this.b.fullpath.relative_path))

        if (this.direction == ActionDirection.DST2SRC) or (this.direction == ActionDirection.BOTH):
            src_side = not (await rclone_engine.exists(this.a.fullpath.root, this.a.fullpath.relative_path))

        this._status = SyncStatus.SUCCESS if src_side and dst_side else SyncStatus.FAILED


    def swap_direction(this) -> None:
        new_dir = ActionDirection.DST2SRC if this.direction == ActionDirection.SRC2DST else ActionDirection.SRC2DST

        fs = this.a if new_dir == ActionDirection.DST2SRC else this.b

        if not fs.exists:
            raise SyncDirectionNotPermittedException(
                f"Action swapping not possible because {fs.absolute_path} does not exist")

        this._direction = new_dir


class SynchManager:
    """
    This class manages the application of each action between source and destination directories
    """
    def __init__(this, source: FileSystem, destination: Union[FileSystem|None] = None):
        this.source = source
        this.destination = destination if destination is not None else source

        this._idx = 0
        this._length = 0

        this._changes = Node(name=".")

    def __iter__(this) -> Iterable[AbstractSyncAction]:
        for node in preorder_iter(this._changes):
            if (action := node.get_attr('action')) is not None:
                yield action

    def __len__(this) -> int:
        return this._length

    # def __getitem__(this, item: int) -> AbstractSyncAction:
    #     return this._changes[item]

    # def __contains__(this, action: AbstractSyncAction) -> bool:
    #     return action in this._changes

    @property
    async def changes(this) -> AsyncIterable[AbstractSyncAction]:
        for action in this:
            yield action

    # def index_of(this, action: AbstractSyncAction) -> Union[int|None]:
    #     """
    #     Returns the position of the provided action within the manager
    #
    #     :param action: the action to retrieve its position
    #     """
    #
    #     for k,v in this._changes.items():
    #         if v == action:
    #             return k
    #
    #     return None

    # def clear(this) -> None:
    #     this._changes = []
    #
    # def remove_action(this, idx:int) -> None:
    #     del this._changes[idx]
    #     this._length-=1

    def add_action(this, action: AbstractSyncAction) -> int:
        src_path = this.source.root
        dst_path = this.destination.root

        # Check if the paths in the provided action are rooted properly in both source and dest directories
        if not AbstractPath.is_root_of(action.a.absolute_path,
                                       src_path):  # this._root_source.is_under_root(action.a.absolute_path):
            raise ValueError(f"The file '{action.a.relative_path} 'is not in '{src_path}'")

        if not AbstractPath.is_root_of(action.b.absolute_path,
                                       dst_path):  # this._root_destination.is_under_root(action.b.absolute_path):
            raise ValueError(f"The file '{action.b.relative_path} 'is not in '{dst_path}'")

        path = f"./{action.b.relative_path if action.direction == ActionDirection.DST2SRC else action.a.relative_path}"

        idx = this._idx

        new_node = {path:{'action':action, 'id': idx}}

        this._changes = add_dict_to_tree_by_path(this._changes,new_node)
        this._idx+=1
        this._length += 1

        return idx

    def cancel_action(this, action: AbstractSyncAction, in_place: bool = False) -> AbstractSyncAction:
        """
        Converts any action with a NoSyncAction
        :param action: the action to nullify
        :return: A new action with similar parameters as the one in input
        """

        if (isinstance(action, NoSyncAction)):
            return action

        new_action = NoSyncAction(action.a, action.b)



        if in_place:
            this.replace(action, new_action)

        return new_action

    def convert_to_delete(this, action: AbstractSyncAction, in_place: bool = False) -> AbstractSyncAction:

        if isinstance(action, DeleteSyncAction):
            return action

        dir = action.direction if action.direction is not None else ActionDirection.SRC2DST
        opposite_dir = ActionDirection.SRC2DST if dir == ActionDirection.DST2SRC else ActionDirection.DST2SRC

        try:
            new_action = SynchManager.make_action(action.a, action.b, type=ActionType.DELETE, direction=dir)
        except SyncDirectionNotPermittedException:
            # attempted one direction - if the other fails, the exception will be escalated
            new_action = SynchManager.make_action(action.a, action.b, type=ActionType.DELETE, direction=opposite_dir)


        if in_place:
            this.replace(action, new_action)

        return new_action

    def replace(this, action: AbstractSyncAction, replace_with: AbstractSyncAction) -> None:
        path = action.b.relative_path if action.direction == ActionDirection.DST2SRC else action.a.relative_path

        path = f"./{path}"

        node = find_path(this._changes, path)

        node.action = replace_with

        this.make_children_as_parent(node)

        this.make_subtree_consistent(replace_with, True)


    async def apply_changes(this, rclone_manager: rclone, eventhandler: [SyncEvent | None] = None) -> None:
        _trigger = _get_trigger_fn(eventhandler)

        submitted_actions = []
        async for x in this.changes:
            if (not isinstance(x, NoSyncAction)) and (x.status != SyncStatus.SUCCESS) and (not x.excluded):
                await x.apply_action(rclone_manager)
                submitted_actions.append(x)

        has_pending_actions = True
        while has_pending_actions:
            has_pending_actions = False

            action_still_in_progress = []

            for a in submitted_actions:
                await a.update_status(rclone_manager)

                if a.status == SyncStatus.IN_PROGRESS:
                    has_pending_actions = True
                    action_still_in_progress.append(a)
                elif a.status == SyncStatus.NOT_STARTED:
                    has_pending_actions = True

            _trigger("on_synching", SyncEvent(action_still_in_progress))


        for a in submitted_actions:
            this.flush_action(a)

        this._flush_cache()

    def _flush_cache(this) -> None:
        """
        Flushes the file system cache into the disk (JSON file) after changes have been applied
        """
        this.source.flush_file_object_cache()

        if this.destination is not None:
            this.destination.flush_file_object_cache()

    def flush_action(this, action: AbstractSyncAction) -> None:
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

                if fso.type == FileType.REGULAR:
                    side.set_file(fso.fullpath, fso)
            case ActionType.DELETE:
                if (action.direction == ActionDirection.SRC2DST) or (action.direction == ActionDirection.BOTH):
                    fso = action.b
                    if fso.type == FileType.REGULAR:
                        this.destination.set_file(fso.fullpath, None)

                if (action.direction == ActionDirection.DST2SRC) or (action.direction == ActionDirection.BOTH):
                    fso = action.a
                    if fso.type == FileType.REGULAR:
                        this.source.set_file(fso.fullpath, None)




    def make_children_as_parent(this, action: [AbstractSyncAction | Node], force_no_action: bool = True) -> None:

        if isinstance(action, AbstractSyncAction):
            path = action.b.relative_path if action.direction == ActionDirection.DST2SRC else action.a.relative_path
            path = f"./{path}"

            node = find_path(this._changes, path)
        elif isinstance(action, Node):
            node = action
            action = node.get_attr("action")
        else:
            raise TypeError("The provided action type is not supported")


        type = action.type
        dir = action.direction

        for x in node.descendants:
            a = x.get_attr("action")

            if (a is not None) and (not a.excluded):
                a = SynchManager.make_action(a.a,a.b,type,dir)
                x.set_attrs({"action":a})

    def make_action_consistent(this,action:[AbstractSyncAction|Node], force_no_action:bool=True) -> None:

        if isinstance(action, AbstractSyncAction):
            path = action.b.relative_path if action.direction == ActionDirection.DST2SRC else action.a.relative_path
            path = f"./{path}"

            node = find_path(this._changes, path)
        elif isinstance(action,Node):
            node = action
            action = node.get_attr("action")
        else:
            raise TypeError("The provided action type is not supported")

        type = []
        dir  = []
        excluded = []

        for n in node.descendants:
            a = n.get_attr("action")
            excluded.append(a.excluded)

            if not a.excluded:
                type.append(a.type)
                dir.append(a.direction)

        if (len(excluded)>0) and (all(excluded)):
            new_action = SynchManager.make_action(action.a, action.b, ActionType.NOTHING, ActionDirection.SRC2DST)
            new_action.excluded = True
        else:
            if (len(type) == 0) or (len(dir) == 0):
                return

            set_type = set(type)
            set_dir = set(dir)

            new_type = ActionType.NOTHING
            new_dir  = ActionDirection.SRC2DST

            if (len(set_type)==1) and (len(set_dir)==1):
                new_type = set_type.pop()
                new_dir = set_dir.pop()
            elif not force_no_action:
                return

            new_action = SynchManager.make_action(action.a, action.b, new_type, new_dir)

        node.set_attrs({"action": new_action })

    def make_subtree_consistent (this,action:[AbstractSyncAction|Node], force_no_action:bool=True) -> None:

        if isinstance(action, AbstractSyncAction):
            path = action.b.relative_path if action.direction == ActionDirection.DST2SRC else action.a.relative_path
            path = f"./{path}"

            node = find_path(this._changes, path)
        elif isinstance(action,Node):
            node = action
        else:
            raise TypeError("The provided action type is not supported")


        parents = [node]

        while (node:=node.parent) is not None:
            if node.get_attr("action") is not None:
                parents.append(node)

        for n in parents:
            this.make_action_consistent(n, force_no_action)

    async def make_all_actions_consistend(this):
        for node in postorder_iter(this._changes):
            if node.get_attr("action") is not None:
                this.make_action_consistent(node, True)
                await asyncio.sleep(0)

    @classmethod
    def make_action(cls, source: FileSystemObject,
                    destination: FileSystemObject,
                    type: ActionType,
                    direction: ActionDirection,*,
                    excluded:bool = False) -> AbstractSyncAction:

        action = None
        match type:
            case ActionType.NOTHING:
                action = NoSyncAction(source, destination)
            case ActionType.DELETE:
                action = DeleteSyncAction(source, destination, direction)
            case ActionType.COPY | ActionType.UPDATE:
                action = CopySyncAction(source, destination, direction=direction)
            case _:
                raise NotImplementedError("Not yet mate!")

        action.excluded = excluded

        return action



# class BulkCopySynchingManager(SynchManager):
#
#     def __init__(this,
#                  source: FileSystem,
#                  destination: FileSystem,
#                  direction: ActionDirection):
#
#         super().__init__(source, destination)
#
#         this._direction = direction
#         this._actions_in_progress = []
#
#     def add_action(this, action: AbstractSyncAction) -> None:
#
#         if action.type not in [ActionType.COPY, ActionType.UPDATE]:
#             raise ValueError("The provided action is not copying or updating a file")
#
#         if (action.direction != this._direction):
#             raise ValueError("The provided action is towards a different synching direction")
#
#         super().add_action(action)
#
#     def apply_changes(this, show_progress: bool = False, eventhandler: [SyncEvent | None] = None) -> None:
#         '''
#         Applies bulk copy/update actions to destionation directory
#         :param show_progress: A boolean representing whether to show the progress bar or not (useful for batch processes)
#         :param eventhandler: A class extending RobinHoodBackend (where events will be passed to)
#         '''
#
#         # Gets the function that facilitate the triggering of events in the eventhandler (if provided)
#         _trigger = _get_trigger_fn(eventhandler)
#
#         def _update_internal_status(d: Dict) -> None:
#             '''
#             This internal function is used as callback function for the rclone_python copy function
#             The updates coming from there are formatted and passed to the right SyncAction object
#             :param d: Dictionary of updates as provided by rclone
#             '''
#
#             # Creates an object to format the dictionary provided by rclone_python with the current transfer update
#             sync_update = _parse_rclone_progress(this.changes, this._direction, d)
#
#             for current_action in sync_update.prog_transferring:
#                 # Let's check if this action is a new one (this means that we are either at the very
#                 # beginning or an action finished (either successfully or not)
#
#                 if current_action not in this._actions_in_progress:
#                     # As this is an action that just started, its status is updated
#                     current_action.status = SyncStatus.IN_PROGRESS
#                     # And gets inside the club of actions in progress
#                     this._actions_in_progress.append(current_action)
#
#             for x in this._actions_in_progress:
#                 # if some actions have a time before the current_time value, it means that it doesn't
#                 # appear in the stdout of rclone, ie it's done (no matter if it's successful or not)
#                 if (x.update.timestamp < sync_update.timestamp):
#                     x._check_success()
#                     # Notify that this action is concluded
#                     _trigger("on_synching", SyncEvent(x))
#
#                     # Flush changes into file system
#                     this.flush_action(x)
#
#             # Filter out all the terminated actions
#             this._actions_in_progress = [x for x in this._actions_in_progress if x.status == SyncStatus.IN_PROGRESS]
#
#             # Notify that these actions are still in progress and send them as a list
#             _trigger("on_synching", SyncEvent(sync_update))
#
#         tmp_dir = site_cache_path()
#         tmp_fname = f"rh_sync_tmp_{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.txt"
#
#         path = os.path.join(tmp_dir, tmp_fname)
#
#         with open(path, "w") as handle:
#             for x in this.changes:
#                 if not x.excluded:
#                     fso = x.a if this._direction == ActionDirection.SRC2DST else x.b
#                     if fso.type == FileType.REGULAR:
#                         handle.write(f"{fso.relative_path}\n")
#
#         a = this.source.root
#         b = this.destination.root
#
#         if this._direction == ActionDirection.DST2SRC:
#             a, b = b, a
#
#         copy(a,
#              b,
#              show_progress=show_progress,
#              listener=_update_internal_status,
#              args=['--files-from', path, '--no-check-dest', '--no-traverse'])
#
#         # Better double-checking again when it's done if everything has been copied successfully
#         for itm in this.changes:
#             if (itm.status in [SyncStatus.IN_PROGRESS, SyncStatus.NOT_STARTED]) and (not itm.excluded):
#                 itm._check_success()
#                 _trigger("on_synching", SyncEvent(itm))
#                 this.flush_action(itm)
#
#         this._flush_cache()
