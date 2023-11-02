import asyncio
from typing import Union, List
from enums import SyncMode, SyncStatus, ActionType, ActionDirection
from config import RobinHoodProfile
from filesystem import FileSystemObject, FileSystem, fs_auto_determine,  rclone_instance, synched_walk
from file_filters import FileFilter, UnixPatternExpasionFilter, RemoveHiddenFileFilter, FilterSet
from synching import SynchManager, BulkCopySynchingManager, _get_trigger_fn, AbstractSyncAction, NoSyncAction
from events import SyncEvent, RobinHoodBackend


async def compare_tree(src: Union[str | FileSystem],
                 dest: Union[str | FileSystem],
                 mode: SyncMode.UPDATE,
                 profile: RobinHoodProfile,
                 eventhandler: [RobinHoodBackend | None] = None
                 ) -> SynchManager:
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


    # load file system cache

    await src.load()
    await dest.load()


    # Parse the result obtained  from rclone
    sync_changes = SynchManager(src, dest)

    # Set file filters

    exclusion_filters = profile.exclusion_filters
    filters: List[FileFilter] = []

    if exclusion_filters is not None:
        filters = [UnixPatternExpasionFilter(pattern) for pattern in profile.exclusion_filters]

    if profile.exclude_hidden_files:
        filters.append(RemoveHiddenFileFilter())


    filter_set = FilterSet(*filters)


    # Get list of directories/files from both sides
    tree = [x async for x in synched_walk(src,dest)]
    processed_items = 0


    for path, a, b in tree:
        _trigger("on_comparing", SyncEvent(path, processed=processed_items, total=len(tree) ) )

        direction = ActionDirection.BOTH
        type = ActionType.NOTHING

        if a is None:
            # file doesn't exist in source, copy to it
            #TODO: unless it's been deleted
            a = FileSystemObject(fullpath=src.new_path(path),
                                 type=b.type,
                                 size=None,
                                 mtime=None,
                                 exists=False,
                                 hidden=b.hidden)
            direction = ActionDirection.DST2SRC
            type = ActionType.COPY
        elif b is None:
            # file doesn't exist in destination, copy to it
            # TODO: unless it's been deleted
            b = FileSystemObject(fullpath=dest.new_path(path),
                                 type=a.type,
                                 size=None,
                                 mtime=None,
                                 exists=False,
                                 hidden=a.hidden)

            direction = ActionDirection.SRC2DST
            type = ActionType.COPY
        else:
            #file exists in both side, let's see which one is newer
            if a.size != b.size:
                type = ActionType.COPY

                if a.mtime.timestamp() > b.mtime.timestamp():
                    direction = ActionDirection.SRC2DST
                else:
                    direction = ActionDirection.DST2SRC

        # At this point, we will have both a & b for sure (no matter whether they exist or not)
        # This means I can filter them out if necessary

        excluded = filter_set.filter(a) or filter_set.filter(b)

        if excluded:
            type = ActionType.NOTHING

        match mode:
            case SyncMode.UPDATE: # any action destination-to-source is not considered
                if direction == ActionDirection.DST2SRC:
                    type = ActionType.NOTHING
                    direction = ActionDirection.SRC2DST
                elif direction == ActionDirection.BOTH:
                    direction = ActionDirection.SRC2DST
            case SyncMode.MIRROR:
                if direction == ActionDirection.DST2SRC:
                    if not a.exists:
                        action = ActionType.DELETE
                    else:
                        action = ActionType.COPY

                    direction = ActionDirection.SRC2DST

        action = SynchManager.make_action(a, b, type, direction, excluded = excluded)


        sync_changes.add_action(action)

        processed_items+=1
        await asyncio.sleep(0)

    await sync_changes.make_all_actions_consistend()

    # Flush file info to cache
    await src.flush_file_object_cache()
    await dest.flush_file_object_cache()

    _trigger("after_comparing", SyncEvent(sync_changes))
    return sync_changes