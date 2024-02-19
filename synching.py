from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import List,  Union, Callable,  Iterable, AsyncIterable
from filesystem import AbstractPath
from filesystem import  FileSystemObject, FileSystem, FileType
from enums import ActionDirection, SyncStatus, ActionType
from events import SyncEvent, RobinHoodBackend
from bigtree import Node, add_dict_to_tree_by_path, preorder_iter, postorder_iter, find_path
from pyrclone.pyrclone import rclone, RCJobStatus
from aiohttp import ClientOSError, ClientResponseError



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


    def retry(this):
        """
        This method allows to retry transfering an action that has failed
        """

        if this.status == SyncStatus.FAILED:
            this._status = SyncStatus.NOT_STARTED
            this._update = None

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
        #this._progress = None
        this._jobid = None

    @property
    def is_updating(this):
        return this.type == ActionType.UPDATE

    def _repr_type(this) -> str:
        return "+" if this.is_updating else "*"

    def retry(this):
        super().retry()
        this._jobid = None

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
            try:
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
            except ClientOSError: #This exception appears when sometimes I stop the jobs. Not sure what's wrong with rclone
                ...


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

        this._status = SyncStatus.IN_PROGRESS

        fn = rclone_engine.delete_file

        if (this.a.type == FileType.DIR) and (this.b.type == FileType.DIR):
            fn = rclone_engine.rmdir

        try:
            if (this.direction == ActionDirection.SRC2DST) or (this.direction == ActionDirection.BOTH):
                await fn(this.b.fullpath.root, this.b.fullpath.relative_path)

            if (this.direction == ActionDirection.DST2SRC) or (this.direction == ActionDirection.BOTH):
                await fn(this.a.fullpath.root, this.a.fullpath.relative_path)
        except ClientResponseError as e:
            if "directory not empty" in e.message:
                ... #needs to be addressed somehow, for the time being I'll suppose it's a fail

    async def update_status(this, rclone_engine:rclone) -> None:

        if this.status == SyncStatus.IN_PROGRESS:
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

        this._rclone_manager:Union[rclone|None] = None

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
        """
        Replace an action with another one - Useful when an action has changed (ie, from NoActio to Copy)
        :param action: Action to be replaced
        :param replace_with: New action
        """

        #get the path to be found within the tree
        path = action.b.relative_path if action.direction == ActionDirection.DST2SRC else action.a.relative_path
        path = f"./{path}"

        #find the corresponding node in the tree
        node = find_path(this._changes, path)

        #replace the action
        node.action = replace_with

        #make sure that the tree of changes is consistent with the new change
        this.make_children_as_parent(node)

        this.make_subtree_consistent(replace_with, True)

    async def abort(this):
        assert this._rclone_manager is not None, "No jobs started"

        await  this._rclone_manager.stop_pending_jobs()



        async for x in this.changes:
            await x.update_status(this._rclone_manager)


    async def apply_changes(this, rclone_manager: rclone, eventhandler: [SyncEvent | None] = None) -> None:
        """
        Apply changes to the source and/or remote

        :param rclone_manager: An rclone object
        :param eventhandler: The handler to update with events
        """
        _trigger = _get_trigger_fn(eventhandler)

        #make a new transfer manager obkect
        manager = TransferManager(rclone_manager,max_transfers=this.max_transfers)

        this._rclone_manager = rclone_manager # I set this internally to be used by abort (or other methods)

        # clean previous stopped jobs if any
        async for gr in rclone_manager.get_group_list():
            await rclone_manager.delete_group_stats(gr)

        async for x in this.changes:
            x.retry() # if there were failed transfers/actions, it'll reset their status to be attempted a new transfer
            manager.append(x) #append the action to the transfer manager

        manager.rearrange_actions()

        # the manager will transfer files at batches (E.g., 4) and the while loop checks if there are still pending actions
        while not (await manager.has_finished()):
            # submit new jobs if I am below the quota
            await manager.attempt_job_submission()

            #checking which actions are still active
            active_actions = [x async for x in manager.actions]
            if len(active_actions) > 0:
                # if any, I will signal the event handler
                _trigger("on_synching", SyncEvent(active_actions))

        #when all it's done, I make the change effective inside the action
        async for a in manager.actions_finished:
            this.flush_action(a)

        #local cache is also flushed
        await this._flush_cache()

        # don't this anymore
        this._rclone_manager = None

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




    def make_children_as_parent(this, action: [AbstractSyncAction | Node]) -> None:
        """
        Make children nodes of the same type as the parent. Useful when the user wants to apply the same action to
        all files in a directory and subdirectories below it
        :param action: (New) parent node
        """

        # Check the input to determine the correct node in the tree
        if isinstance(action, AbstractSyncAction):
            path = action.b.relative_path if action.direction == ActionDirection.DST2SRC else action.a.relative_path
            path = f"./{path}"

            node = find_path(this._changes, path)
        elif isinstance(action, Node):
            node = action
            action = node.get_attr("action")
        else:
            raise TypeError("The provided action type is not supported")

        # get type and direction of the current action
        type = action.type
        dir = action.direction

        #change all the descendend accordingly
        for x in node.descendants:
            a = x.get_attr("action")

            if (a is not None) and (not a.excluded):
                a = SynchManager.make_action(a.a,a.b,type,dir)
                x.set_attrs({"action":a})

    def make_action_consistent(this,action:[AbstractSyncAction|Node], force_no_action:bool=True) -> None:
        """
        This method is useful to propagate changes in an action when (at least) one of the descendant has changed
        If all the descendant would have the same action/direction, then the parent node (provided) will be changed as
        such. If descendants don't have consistend actions, then the parent node (the provided one) will be changed
        into no action

        :param action: the parent node to check
        :param force_no_action: Change the action of the parent to no_action no matter what's below it
        """

        # retrieve the right node from the tree
        if isinstance(action, AbstractSyncAction):
            path = action.b.relative_path if action.direction == ActionDirection.DST2SRC else action.a.relative_path
            path = f"./{path}"

            node = find_path(this._changes, path)
        elif isinstance(action,Node):
            node = action
            action = node.get_attr("action")
        else:
            raise TypeError("The provided action type is not supported")

        #list of action types, directions, and whether they are excluded
        type = []
        dir  = []
        excluded = []

        #retrieve the above information from all descendants
        for n in node.descendants:
            a = n.get_attr("action")
            excluded.append(a.excluded)

            if not a.excluded:
                type.append(a.type)
                dir.append(a.direction)

        #if the descendants are all excluded, then the parent node will be excluded too
        if (len(excluded)>0) and (all(excluded)):
            new_action = SynchManager.make_action(action.a, action.b, ActionType.NOTHING, ActionDirection.SRC2DST)
            new_action.excluded = True
        else:
            # if it has not descendant, then return
            if (len(type) == 0) or (len(dir) == 0):
                return

            set_type = set(type)
            set_dir = set(dir)

            # setting default action/direction to Nothing/>
            new_type = ActionType.NOTHING
            new_dir  = ActionDirection.SRC2DST

            # if all descendants have one direction in the same action
            if (len(set_type)==1) and (len(set_dir)==1):
                # if so, the current action will be as the descendants
                new_type = set_type.pop()
                new_dir = set_dir.pop()
            #in this case, descendats have mismatched actions and direction, will do something if forced
            elif not force_no_action:
                return

            new_action = SynchManager.make_action(action.a, action.b, new_type, new_dir)

        #update the node tree accordingly
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

    def rearrange_actions(this) -> None:
        # Sort the actions in a way that should minimise the risk of deleting a non-empty directory
        # rclone won't delete non-empty directories. However, it'll cause the deletion to fail
        # Having any deletion of directories at the end (in reversed order) giving the opportunity to process
        # all directory's content (if any), this will reduce the risk of annoying X's

        folder_deletion = []
        other_actions = []

        for action in this._actions:
            if (action.type == ActionType.DELETE) and (action.is_folder):
                folder_deletion.append(action)
            else:
                other_actions.append(action)


        folder_deletion.sort(key=lambda x:x.a.filename if x.a.filename is not None else x.b.filename, reverse=True)

        this._actions = other_actions + folder_deletion
