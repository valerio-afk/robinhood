from __future__ import annotations

import asyncio
from random import randint
from abc import ABC, abstractmethod
from typing import List, Any, Union, Callable, Dict, Iterable, AsyncIterable

from filesystem import AbstractPath
from filesystem import convert_to_bytes, FileSystemObject, FileSystem, FileType
from enums import ActionDirection, SyncStatus, ActionType
from events import SyncEvent, RobinHoodBackend
from bigtree import Node, add_dict_to_tree_by_path, preorder_iter, postorder_iter, find_path


import sys
sys.path.append("/home/tuttoweb/Documents/repositories/pyrclone")
from pyrclone import rclone
from pyrclone.jobs import RCJobStatus, RCloneTransferDetails


def _get_trigger_fn(eventhandler: Union[SyncEvent | None] = None) -> Callable[[str, SyncEvent], None]:
    def _trigger(mtd: str, e: SyncEvent) -> None:
        if isinstance(eventhandler, RobinHoodBackend):
            try:
                fn = getattr(eventhandler, mtd)
                fn(e)
            except AttributeError:
                ...  # event does not exist

    return _trigger



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
    def type(this) -> ActionType:
        return this._type

    @property
    def direction(this) -> ActionDirection:
        return this._direction

    @property
    def status(this) -> SyncStatus:
        return this._status

    @property
    def update(this) -> Union[RCJobStatus | None]:
        return this.get_update()

    @property
    def is_folder(this) -> bool:
        return (this.a.type == FileType.DIR) or (this.b.type == FileType.DIR)

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

    def get_update(this) -> Union[RCJobStatus | None]:
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

        if this._jobid is not None:
            return

        src_root = this.a.fullpath.root
        src_path = this.a.fullpath.relative_path

        dst_root = this.b.fullpath.root
        dst_path = this.b.fullpath.relative_path

        if this.direction == ActionDirection.DST2SRC:
            src_root, dst_root = dst_root, src_root
            src_path, dst_path = dst_path, src_path

        if not this.is_folder:
            this._jobid = await rclone_engine.copy_file(src_root, src_path, dst_root, dst_path)

    async def update_status(this, rclone_engine:rclone) -> None:

        if this._jobid is not None:
            timeout = True

            async for id,status in rclone_engine.jobs:
                if id == this._jobid:
                    match status:
                        case RCJobStatus.NOT_STARTED:
                            this._status = SyncStatus.NOT_STARTED
                        case RCJobStatus.IN_PROGRESS:
                            this._status = SyncStatus.IN_PROGRESS
                        case RCJobStatus.FINISHED:
                            this._status = SyncStatus.SUCCESS
                        case RCJobStatus.FAILED:
                            this._status = SyncStatus.FAILED

                    this._update = rclone_engine.get_last_status_update(this._jobid)


        else:
            if this.is_folder:
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

        this.max_transfers = 4

        this._changes = Node(name=".")

    def __iter__(this) -> Iterable[AbstractSyncAction]:
        for node in preorder_iter(this._changes):
            if (action := node.get_attr('action')) is not None:
                yield action

    def __len__(this) -> int:
        return this._length


    @property
    async def changes(this) -> AsyncIterable[AbstractSyncAction]:
        for action in this:
            yield action

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

        manager = TransferManager(rclone_manager,max_transfers=this.max_transfers)

        async for x in this.changes:
            manager.append(x)

        while not (await manager.has_finished()):
            await manager.attempt_job_submission()
            active_actions = [x async for x in manager.actions]
            if len(active_actions) > 0:
                _trigger("on_synching", SyncEvent(active_actions))

        async for a in manager.actions_finished:
            this.flush_action(a)

        await this._flush_cache()

    async def _flush_cache(this) -> None:
        """
        Flushes the file system cache into the disk (JSON file) after changes have been applied
        """
        await  this.source.flush_file_object_cache()

        if this.destination is not None:
            await this.destination.flush_file_object_cache()

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
                fso = action.a if action.direction == ActionDirection.SRC2DST else action.b

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


class TransferManager:

    def __init__(this, rclone:rclone, max_transfers:int = 4):
        this._max_transfers = max_transfers
        this._actions:List[AbstractSyncAction] = []
        this._rclone = rclone

    @property
    def max_transfers(this) -> int:
        return this._max_transfers

    @max_transfers.setter
    def max_transfers(this, value:int) -> None:
        this._max_transfers = value

    async def filter_by_status(this, status:SyncStatus):
        async for action in this.actions:
            if action.status == status:
                yield action

    @property
    async def actions(this) -> AsyncIterable:
        for action in this._actions:
            await action.update_status(this._rclone)
            yield action


    @property
    async def queued_actions(this) -> AsyncIterable[AbstractSyncAction]:
        async for action in this.filter_by_status(SyncStatus.NOT_STARTED):
            yield action

    @property
    async def actions_in_progress(this) -> AsyncIterable[AbstractSyncAction]:
        async for action in this.filter_by_status(SyncStatus.IN_PROGRESS):
            yield action

    @property
    async def actions_successful(this) -> AsyncIterable[AbstractSyncAction]:
        async for action in this.filter_by_status(SyncStatus.SUCCESS):
            yield action

    @property
    async def actions_failed(this) -> AsyncIterable[AbstractSyncAction]:
        async for action in this.filter_by_status(SyncStatus.FAILED):
            yield action

    @property
    async def actions_finished(this) -> AsyncIterable[AbstractSyncAction]:
        async for action in this.actions_successful:
            yield action

        async for action in this.actions_failed:
            yield action


    @property
    async def active_actions(this) -> AsyncIterable[AbstractSyncAction]:
        async for action in this.queued_actions:
            yield action

        async for action in this.actions_in_progress:
            yield action


    async def has_finished(this) -> bool:
        async for _ in this.queued_actions:
            return False

        async for _ in this.actions_in_progress:
            return False

        return True


    async def current_capacity(this) -> int:
        max = this.max_transfers

        async for _ in this.actions_in_progress:
            max-=1

        return max if max>=0 else 0

    async def attempt_job_submission(this):
        capacity = await this.current_capacity()

        if capacity > 0:
            async for action in this.queued_actions:
                if capacity > 0 :
                    await action.apply_action(this._rclone)
                    capacity-=1

            await asyncio.sleep(0.5)


    def append(this, action:AbstractSyncAction) -> None:
        if (not isinstance(action, NoSyncAction)) and (action.status != SyncStatus.SUCCESS) and (not action.excluded):
            this._actions.append(action)

    def remove(this, action:AbstractSyncAction) -> None:
        del this[action]
    def __delitem__(this, action:AbstractSyncAction) -> None:
        idx = this._actions.index(action)
        del this._actions[idx]
