import asyncio
from typing import Union
from enums import SyncMode, SyncStatus, ActionType, ActionDirection
from config import RobinHoodProfile
from filesystem import FileSystemObject, FileSystem, fs_auto_determine,  rclone_instance, synched_walk
from synching import SynchingManager, BulkCopySynchingManager, _get_trigger_fn, AbstractSyncAction, NoSyncAction
from events import SyncEvent, RobinHoodBackend



async def compare_tree(src: Union[str | FileSystem],
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

    rc = rclone_instance()

    # if the provided source path is a dir, then the function fs_auto_determine attempts to determine if it's local or remote
    if isinstance(src, str):
        src = await fs_auto_determine(src, True)
        src.cached = True

    # Same as above, but with the destination path
    if isinstance(dest, str):
        dest = await fs_auto_determine(dest,True)
        dest.cached = True

    await src.load()
    await dest.load()


    # Parse the result obtained  from rclone
    sync_changes = SynchingManager(src, dest)


    tree = [x async for x in synched_walk(src,dest)]
    processed_items = 0

    for path, a, b in tree:
        _trigger("on_comparing", SyncEvent(path, processed=processed_items, total=len(tree) ) )

        if a is None:
            a = FileSystemObject(fullpath=src.new_path(path),
                                 type=b.type,
                                 size=None,
                                 mtime=None,
                                 exists=False,
                                 hidden=b.hidden)
        elif b is None:
            b = FileSystemObject(fullpath=dest.new_path(path),
                                 type=a.type,
                                 size=None,
                                 mtime=None,
                                 exists=False,
                                 hidden=a.hidden)


        action = SynchingManager.make_action(a,b,ActionType.NOTHING,ActionDirection.SRC2DST)

        sync_changes.add_action(action)

        processed_items+=1
        await asyncio.sleep(0)


    _trigger("after_comparing", SyncEvent(sync_changes))
    return sync_changes