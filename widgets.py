from __future__ import annotations
from config import RobinHoodProfile
from rich.text import Text
from rich.style import Style
from rich.console import RenderableType
from typing import Any, ClassVar, Union, Iterable, Tuple, Dict, List, Set
from textual import on, events
from textual.events import Event
from textual.reactive import reactive
from textual.widgets import Switch, Input, Label, Static, Button, Select, DataTable, TextArea
from textual.widgets.data_table import Column
from textual.containers import Horizontal, Container
from highlighted_progressbar import HighlightedProgressBar
from textual.app import Widget, ComposeResult, Binding
from textual.coordinate import Coordinate
from synching import (SynchManager,
                      SyncStatus,
                      ActionType,
                      AbstractSyncAction,
                      SyncDirectionNotPermittedException,
                      CopySyncAction )
from filesystem import (FileType, sizeof_fmt,
                        FileSystemObject,
                        fs_autocomplete,
                        AbstractPath,
                        NTAbstractPath,
                        rclone_instance)
from datetime import datetime
from textual.suggester import Suggester
from enum import Enum
from enums import SyncMode, ActionDirection
from fnmatch import fnmatch
import re

Column.percentage_width = None  # to overcome textual limitations :)

_SyncMethodsPrefix: Dict[SyncMode, str] = {
    SyncMode.UPDATE: ">>",
    SyncMode.MIRROR: "->",
    SyncMode.SYNC: "<>",
    SyncMode.DEDUPE: "**"
}

_SyncMethodsNames: Dict[SyncMode, str] = {
    SyncMode.UPDATE: "Update",
    SyncMode.MIRROR: "Mirror",
    SyncMode.SYNC: "Bidirectional sync",
    SyncMode.DEDUPE: "Find deduplicates"
}

SyncMethods: List[Tuple[str, SyncMode]] = [(_SyncMethodsPrefix[x] + " " + str(x).split(".")[1].capitalize(), x) for x in
                                           SyncMode]


def _render_action(action: AbstractSyncAction) -> Tuple[RenderableType, RenderableType, RenderableType]:
    #_make_suitable_icon = lambda x: ":page_facing_up:" if x.exists else ":white_medium_star:[i]"
    def _make_suitable_icon(x:FileSystemObject):
        file_type = ":page_facing_up:" if x.type == FileType.REGULAR else ":open_file_folder:"
        new_file = "" if x.exists else ":white_medium_star:"

        return file_type+new_file

    def _compress_path(x: FileSystemObject):
        fname = x.filename
        spaces = "  " * x.relative_path.count(AbstractPath.PATH_SEPARATOR)

        return spaces+_make_suitable_icon(x)+fname


    a = action.a
    b = action.b

    match action.status:
        case SyncStatus.SUCCESS:
            middle_column = ":white_heavy_check_mark:"
        case SyncStatus.FAILED:
            middle_column = ":cross_mark:"
        case _:
            middle_column = "-" if action.direction is None else action.direction.value



    return _compress_path(a),middle_column,_compress_path(b)


def _render_action_as_no_action(action: AbstractSyncAction) -> Tuple[RenderableType, RenderableType, RenderableType]:
    c1, _, c3 = _render_action(action)
    c2 = "-"

    return (c1, c2, c3)


def _render_action_as_copy_action(action: AbstractSyncAction, *,
                                  width=10) -> Tuple[RenderableType, RenderableType, RenderableType]:
    columns= list(_render_action(action))

    colour = "[green]"

    if isinstance(action, CopySyncAction) and action.is_updating:
        colour = "[bright_green]"

    for i,c in enumerate(columns):
        columns[i] = f"{colour}{c}[/]"

    c1,c2,c3 = columns

    if action.status == SyncStatus.IN_PROGRESS:
        update = action.get_update()

        if update is not None:
            progress_update = update.stats
            if progress_update is not None:
                p = progress_update.percentage
                progress_bar = HighlightedProgressBar(1,width)

                label = None

                if (action.update is not None) and (action.update.stats is not None):
                    label = sizeof_fmt(action.update.stats.average_speed) + "/s"

                progress_bar.update(p,label)
                c2 = progress_bar.render()
                #c2 = Bar((0, width * p), highlight_style="green1", background_style="dark_green")

    return c1,c2,c3

def _render_action_as_delete_action(action: AbstractSyncAction) -> Tuple[RenderableType, RenderableType, RenderableType]:
    c1,c2,c3 = _render_action(action)

    colour = "[red]"

    _add_after_spaces = lambda x,s : re.sub("^(\s*)(.+)",f"\\1{s}\\2",x)

    if (action.direction == ActionDirection.SRC2DST) or (action.direction == ActionDirection.BOTH):
        c3 = _add_after_spaces(c3,"[s]")
    if (action.direction == ActionDirection.DST2SRC) or (action.direction == ActionDirection.BOTH):
        c1 = _add_after_spaces(c1,"[s]")

    return colour + c1 + "[/]", colour + c2 + "[/]", colour + c3 + "[/]"


def _render_row(action:AbstractSyncAction, width=10):
    match action.type:
        case ActionType.NOTHING:
            return _render_action_as_no_action(action)
        case ActionType.COPY | ActionType.UPDATE:
            return _render_action_as_copy_action(action, width=width)
        case ActionType.DELETE:
            return _render_action_as_delete_action(action)

class RobinHoodExcludePath(Static):
    """
    Show a pane where the user can provide a list of patterns for paths to exclude
    """
    def __init__(this, profile: RobinHoodProfile, *args, **kwargs):
        """

        :param profile: The profile to be read/edited
        :param args: arguments to be provided to the super class
        :param kwargs: keyword arguments to be provided to the super class
        """
        super().__init__(*args, **kwargs)
        this.border_title = "Path to Exclude"
        this._pattern_text_area = TextArea()
        this._exclude_hidden_files_switch = Switch(id="exclude_hidden")
        this._profile = profile

    @property
    def paths(this) -> Iterable[str]:
        """
        Provides the list of path to exclude
        :return: List of strings with patterns
        """
        return this._profile.exclusion_filters

    @property
    def exclude_hidden(this) -> bool:
        """
        Whether or not to exclude hidden files. The definition of hidden file is system dependent
        :return: TRUE if hidden files need to be excluded, FALSE otherwise
        """
        return this._profile.exclude_hidden_files

    @property
    def deep_comparisons(this) -> bool:
        """
        Whether or not deep comparisons are enabled. This functionality has not been exposed because is too
        computationally demanding and may download files from remote

        :return: TRUE if deep comparisons are enabled, FALSE otherwise
        """
        return this._profile.deep_comparisons

    def compose(this) -> ComposeResult:
        yield this._pattern_text_area
        yield Horizontal(
            Label("Exclude hidden files"),
            this._exclude_hidden_files_switch,
            # Label("Deep comparison"),
            # Switch(id="deep_comparisons"),
            id="other_settings"
        )

    @on(events.Show)
    def show_filters(this) -> None:
        """
        This event is triggered when the pane is shown on screen
        Patterns are shown in the text area and the switch is updated to show whether hidden files are to be
        excluded or not
        """
        if this.paths is not None:
            this._pattern_text_area.load_text("\n".join(this.paths))

        this._exclude_hidden_files_switch.value = this.exclude_hidden
        # this.query_one("#deep_comparisons").value = this.deep_comparisons

    @on(events.Hide)
    def save_filters(this) -> None:
        """
        This event is triggered when the pane is hidden from the screen. All changes are stored in the profile
        """
        new_filters = [line for line in this._pattern_text_area.text.splitlines() if len(line) > 0]

        this._profile.exclusion_filters = new_filters
        this._profile.exclude_hidden_files = this._exclude_hidden_files_switch.value
        # this._profile.deep_comparisons = this.query_one("#deep_comparisons").value

class RobinHoodRemoteList(Static):
    """
    This class makes a Static renderable object to display a list of remotes configured on rclone
    """

    def __init__(this, *args, **kwargs):
        super().__init__(*args, **kwargs)

        this.remotes: List[Tuple[str, ...]] = []
        this.border_title = "Remotes"
        this._datatable = DataTable(cursor_type="row")

    def compose(this) -> ComposeResult:
        """
        Generates the necessary widgets
        :return: Yields a datatable
        """
        yield this._datatable

    async def on_mount(this) -> None:
        """
        This event is triggered when the widget this class creates is mounted in the TUI
        """

        # if the list of remotes is 0, it means this is the first time this widget has been shown on screen
        # let's pull the list of remotes from rclone (can take some time)
        if len(this.remotes) == 0:
            this.remotes = await rclone_instance().list_remotes()

        # format the data table
        header = ("Type", "Drive")
        table = this._datatable

        # add the headers
        table.add_columns(*header)

        # now let's add the remotes row by row
        for r in this.remotes:
            table.add_row(r[0], r[1] + NTAbstractPath.PATH_SEPARATOR)

class ComparisonSummary(Widget):
    COMPONENT_CLASSES: ClassVar[set[str]] = {
        "cs--description",
        "cs--key",
    }

    DEFAULT_CSS = """
      ComparisonSummary {
          background: $accent;
          color: $text;
          height: 1;
      }

      ComparisonSummary > .cs--key {
          text-style: bold;
          background: $accent-darken-2;
      }
      """

    KEY_LABELS = ['To upload', 'To download', 'To delete (source)', 'To delete (destination)']

    def __init__(this, results: Union[SynchManager | None] = None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        this._results = results

    @property
    def pending_actions(this):
        if this._results is not None:
            for r in this._results:
                if (r.status != SyncStatus.SUCCESS) and (r.type != ActionType.NOTHING):
                    yield r

    @property
    def has_pending_actions(this):
        for _ in this.pending_actions:
            return True

        return False

    @property
    def results(this):
        return this._results

    @results.setter
    def results(this, new_results: Union[Iterable[AbstractSyncAction] | None]) -> None:
        this._results = new_results
        this.refresh()

    @property
    def transfer_bytes(this) -> Tuple[int, int, int, int]:
        upload = 0
        download = 0
        delete_source = 0
        delete_target = 0

        # if (this.results is not None):
        for r in this.pending_actions:
            action = r.type

            if (action == ActionType.COPY) or (action == ActionType.UPDATE):
                match r.direction:
                    case ActionDirection.SRC2DST:
                        upload += r.a.size if r.a.type == FileType.REGULAR else 0
                    case ActionDirection.DST2SRC:
                        download += r.b.size if r.b.type == FileType.REGULAR else 0

            if (action == ActionType.DELETE):
                if (r.direction == ActionDirection.DST2SRC) or (r.direction == ActionDirection.BOTH):
                    delete_source += r.a.size if r.a.type == FileType.REGULAR else 0
                if (r.direction == ActionDirection.SRC2DST) or (r.direction == ActionDirection.BOTH):
                    delete_target += r.b.size if r.b.type == FileType.REGULAR else 0

        return (upload, download, delete_source, delete_target)

    def render(this) -> RenderableType:

        if (this.results is None):
            return Text("")

        base_style = this.rich_style
        text = Text(
            style=this.rich_style,
            no_wrap=True,
            overflow="ellipsis",
            justify="left",
            end="",
        )

        key_style = this.get_component_rich_style("cs--key")
        description_style = this.get_component_rich_style("cs--description")

        for lbl, size in zip(this.KEY_LABELS, this.transfer_bytes):
            txt = Text.assemble((f" {lbl} ", base_style + description_style), (f" {sizeof_fmt(size)} ", key_style))

            text.append_text(txt)

        return text
class DisplayFilters(Widget):
    class FilterType(Enum):
        NO_ACTION: int = 0
        NEW_FILES: int = 1
        COPY: int = 2
        DELETE: int = 3
        EXCLUDED: int = 4
        PATTERN: int = 5

    class FilterChanged(Event):
        def __init__(this, filter_type: DisplayFilters.FilterType, value: Any):
            super().__init__()

            this.filter_type = filter_type
            this.value = value

    def __init__(this, *args, **kwargs):
        super().__init__(*args, **kwargs)

        this._sub_widgets = {
            "No action": Switch(id="df_no_action", value=True),
            "New files": Switch(id="df_new_files", value=True),
            "Copy": Switch(id="df_copy", value=True),
            "Delete": Switch(id="df_delete", value=True),
            "Excluded": Switch(id="df_excluded_files", value=False),
            "Filter by name": Input(None, placeholder="Filter by name", id="df_by_name")
        }

    def compose(this) -> ComposeResult:
        widgets = []

        for lbl, switch in this._sub_widgets.items():
            widgets.append(Label(lbl))
            widgets.append(switch)

        yield Horizontal(
            Static("[bold]SPACE:[/bold] Cancel - "
                   "[bold]RIGHT Key:[/bold] Move to destination - [bold]LEFT Key:[/bold] Move to source - "
                   "[bold]DELETE[/bold] Delete file - [bold]EQUAL (=):[/bold] Apply changes to both sides (if applicable)")
        )

        yield Horizontal(*widgets, id="display_filters")

    @on(Switch.Changed)
    def on_changed(this, event: Switch.Changed) -> None:
        type = None
        if event.switch == this._sub_widgets["No action"]:
            type = DisplayFilters.FilterType.NO_ACTION
        elif event.switch == this._sub_widgets["Copy"]:
            type = DisplayFilters.FilterType.COPY
        elif event.switch == this._sub_widgets["Delete"]:
            type = DisplayFilters.FilterType.DELETE
        elif event.switch == this._sub_widgets["New files"]:
            type = DisplayFilters.FilterType.NEW_FILES
        elif event.switch == this._sub_widgets["Excluded"]:
            type = DisplayFilters.FilterType.EXCLUDED

        if type is not None:
            this.post_message(DisplayFilters.FilterChanged(type, event.value))

    @on(Input.Submitted)
    def on_change(this, event: Input.Submitted):
        this.post_message(DisplayFilters.FilterChanged(DisplayFilters.FilterType.PATTERN, event.value))


class FileDetailsSummary(Widget):
    COMPONENT_CLASSES: ClassVar[set[str]] = ComparisonSummary.COMPONENT_CLASSES

    DEFAULT_CSS = """
      FileDetailsSummary {
          background: $accent;
          color: $text;
          height: 1;
      }

      FileDetailsSummary > .cs--key {
          text-style: bold;
          background: $accent-darken-2;
      }
      """

    source_file: FileSystemObject = None
    destination_file: FileSystemObject = None


    def show(this, src_file: Union[FileSystemObject | None], dest_file: Union[FileSystemObject | None]):
        this.source_file = src_file
        this.destination_file = dest_file
        this.refresh()

    @property
    def source_size(this) -> Union[int, None]:
        return None if (this.source_file is None) \
                       or (this.source_file.size is None) or \
                       (this.source_file.size < 0) \
            else this.source_file.size

    @property
    def source_mtime(this) -> Union[datetime | None]:
        return None if this.source_file is None else this.source_file.mtime

    @property
    def destination_size(this) -> Union[int, None]:
        return None if (this.destination_file is None) \
                       or (this.destination_file.size is None) or \
                       (this.destination_file.size < 0) \
            else this.destination_file.size

    @property
    def destination_mtime(this) -> Union[datetime | None]:
        return None if this.destination_file is None else this.destination_file.mtime

    @property
    def full_path(this) -> Union[str | None]:
        if (this.source_file is None) and (this.destination_file is None):
            return None

        fullpath = this.destination_file.relative_path if this.source_file is None else this.source_file.relative_path

        #_, filename = os.path.split(fullpath.absolute_path)

        return fullpath

    def render(this) -> RenderableType:

        if (this.full_path is None):
            return Text("")

        base_style = this.rich_style
        text = Text(
            style=this.rich_style,
            no_wrap=True,
            overflow="ellipsis",
            justify="left",
            end="",
        )

        key_style = this.get_component_rich_style("cs--key")
        description_style = this.get_component_rich_style("cs--description")

        show_formatted_size = lambda x: sizeof_fmt(x) if x is not None else "-"

        local_size = show_formatted_size(this.source_size)
        dest_size = show_formatted_size(this.destination_size)

        text.append_text(Text.assemble((f" Filename ", base_style + description_style), (this.full_path, key_style)))
        text.append_text(Text.assemble((f" Size (source) ", base_style + description_style), (local_size, key_style)))
        text.append_text(
            Text.assemble((f" Size (destination) ", base_style + description_style), (dest_size, key_style)))

        return text

class FileSystemSuggester(Suggester):

    def __init__(this, *args, **kwargs):
        kwargs['case_sensitive'] = True

        super().__init__(*args, **kwargs)

    async def get_suggestion(this, value: str) -> str | None:
        x = await fs_autocomplete(value)

        return x


class RobinHoodTopBar(Container):
    def __init__(this,
                 src: Union[str | None] = None,
                 dst: Union[str | None] = None,
                 mode: Union[SyncMode | None] = None,
                 *args,
                 **kwargs
                 ) -> None:
        this._src: Union[str | None] = src
        this._dst: Union[str | None] = dst
        this._mode: Union[SyncMode | None] = mode

        super().__init__(*args, **kwargs)

    def compose(this) -> ComposeResult:
        yield Label("Welcome to RobinHood", id="status_text")
        yield Horizontal(
            Input(id="source_text_area", placeholder="Source directory", suggester=FileSystemSuggester(),
                  value=this._src),
            Button("Start", id="work_launcher", variant="success"),
            Input(id="dest_text_area", placeholder="Destination directory", value=this._dst),
            Select(SyncMethods, prompt="Sync Mode...", id="syncmethod", value=this._mode),
            id="textbox_container"
        )

class DirectoryComparisonDataTable(DataTable):
    BINDINGS = [
        Binding("space", "cancel_action", "Cancel Action", show=False),
        Binding("left", "change_direction('left')", "Change Action Direction", show=False),
        Binding("right", "change_direction('right')", "Change Action Direction", show=False),
        Binding("=", "change_direction_to_both", "Apply action to both sides", show=False),
        Binding("delete", "delete_file", "Delete File", show=False),
        Binding("enter","toggle_selection","Select Row", show=False),
        Binding("escape", "clear_selections", "Select Row", show=False),
        Binding("ctrl+a", "select_all", "Select all visible", show=False)
    ]

    COMPONENT_CLASSES: ClassVar[set[str]] = {
        "datatable--filtered-row",
        "datatable--selected-row"
    }

    show_no_action : reactive[bool] = reactive(True)
    show_copy_update : reactive[bool] = reactive(True)
    show_delete : reactive[bool] = reactive(True)
    show_new_files : reactive[bool] = reactive(True)
    show_excluded : reactive[bool] = reactive(False)
    filter_by_name : reactive[str|None] = reactive(None)

    class TableRefreshed(Event):
        ...

    class ActionRefreshed(Event):

        def __init__(this, action:AbstractSyncAction):
            super().__init__()
            this._action = action

        @property
        def action(this) -> AbstractSyncAction:
            return this._action

    def __init__(this, *args, **kwargs):
        kwargs["cursor_type"] = "row"

        super().__init__(*args, **kwargs)

        this.add_column(Text.from_markup("Source Directories", overflow="ellipsis"), key="src", width=45)
        this.add_column(Text.from_markup("Action", overflow="ellipsis"), key="action", width=7)
        this.add_column(Text.from_markup("Destination Directories", overflow="ellipsis"), key="dst", width=45)

        this._sync_manager: [SynchManager | None] = None
        this._displayed_actions: List[AbstractSyncAction] = []
        this._selected_actions: Set[int] = set()


    def adjust_column_sizes(this) -> None:
        """
        Adapts the size of the column intepreting the width parameter in terms of percentage
        """

        # the overall available space is given by the sie of the widget -6 (not sure where this comes from, but it works)
        psize = this.size.width - 6

        # in the case the height of the table is larger than the hight of the visible area, it means the scroll bar is
        # visible, eating more horizontal space
        if this.virtual_size.height > this.size.height:
            psize -= 2  # thickness of the scrollbar

        # total size of all columns
        tot_size = 0

        # colums in the table
        columns = list(this.columns.values())

        # For each column
        for c in columns:
            # Let's disable the auto_width (not necessary)
            c.auto_width = False

            # I need to store somewhere the width expressed in percentage, because I need te width property
            # to set the new calculated size
            if (not hasattr(c, "percentage_width") or (c.percentage_width is None)):
                c.percentage_width = c.width

            # calculate the actual size from the percentage
            c.width = int(psize * (c.percentage_width / 100))

            # update the total size
            tot_size += c.width

        # due to roudning issues, the total size can be different than the actual width
        # ie tot size can be less or greater than the actual width
        if tot_size != psize:
            # if so, the difference in sizes is redistributed to all columns
            delta = psize - tot_size

            # making bigger or smaller?
            unit = 1 if delta > 0 else -1

            if (delta < 0):
                delta *= -1

            i = 0

            # each column size is increased/decreased by 1 at each iteration until there's anything else to
            # ridestribute.
            while delta > 0:
                columns[i % len(columns)].width += unit
                i += 1
                delta -= 1

        this.show_horizontal_scrollbar = False

        # refresh the widget
        this.refresh()

    async def watch_show_no_action(this, old_state:bool, new_state:bool) -> None:
        await this._multi_watch(old_state, new_state)
    async def watch_show_new_files(this, old_state:bool, new_state:bool) -> None:
        await this._multi_watch(old_state, new_state)
    async def watch_show_copy_update(this, old_state:bool, new_state:bool) -> None:
        await this._multi_watch(old_state, new_state)
    async def watch_show_delete(this, old_state:bool, new_state:bool) -> None:
        await this._multi_watch(old_state, new_state)
    async def watch_show_excluded(this, old_state:bool, new_state:bool) -> None:
        await this._multi_watch(old_state, new_state)
    async def watch_filter_by_name(this, old_pattern:Union[str|None], new_pattern:Union[str|None]):
        await this._multi_watch(old_pattern, new_pattern)
    async def _multi_watch(this, previous:Any, now:Any) -> None:
        if previous != now:
            await this.refresh_table()

    @property
    def is_empty(this) -> bool:
        return (this.changes is None) or (len(this.changes) == 0)

    def __getitem__(this, index: int) -> AbstractSyncAction:
        return this._displayed_actions[index]

    @property
    def changes(this) -> Union[SynchManager | None]:
        """
        Returns the changes to be applied to the source/destination folders

        :return: The SynchingManager containing the list of changes. It can also return None if the table isn't displaying anything
        """
        return this._sync_manager




    async def show_results(this, changes: Union[SynchManager | None]) -> None:

        if (changes is None):
            return None

        this._sync_manager = changes

        await this.refresh_table()

        this.focus()

    async def refresh_table(this):
        previous_cursor = this.cursor_row
        previous_scroll_y = this.scroll_y

        this.clear(columns=False)

        if (this._sync_manager is None):
            return None

        # The property _selected_actions contains the position of the actions. When the table is refreshed, these
        # actions are likely to be placed in a different position, or even filtered out (ie not visible)
        # This needs to be fixed. I save the actual action objects retrieved from their positions
        # After I know what to display, I retrieve their new indices

        selected_actions = [this._displayed_actions[i] for i in this._selected_actions]

        # Reset the set of selected actions, as well as clean the list of displayed actions
        this._selected_actions = set()
        this._displayed_actions = []

        show_new_files = lambda itm : this.show_new_files or (itm.a.exists and itm.b.exists)

        def is_visible(action: AbstractSyncAction):
            if (this.filter_by_name != None) and (len(this.filter_by_name)>0):
                match = fnmatch(action.a.relative_path.lower(),this.filter_by_name.lower())

                if match == False:
                    return False

            if this.show_excluded and action.excluded:
                return show_new_files(action)
            else:
                return (not action.excluded) and show_new_files(action) and \
                       ((this.show_no_action and (action.type == ActionType.NOTHING)) or
                       (this.show_copy_update and (action.type in [ActionType.COPY, ActionType.UPDATE])) or
                       (this.show_delete and (action.type == ActionType.DELETE)) )

        i = 0
        async for itm in this._sync_manager.changes:
            if is_visible(itm):
                this._displayed_actions.append(itm)

                if itm in selected_actions:
                    this._selected_actions.add(i)

                i+=1

        rendered_rows = [''] * len(this._displayed_actions)

        for i, x in enumerate(this._displayed_actions):
            rendered_rows[i] = this._render_row(x)

        this.add_rows(rendered_rows)
        this.post_message(DirectoryComparisonDataTable.TableRefreshed())

        if previous_cursor > 0:
            this.move_cursor(row=previous_cursor)
            this.scroll_y = previous_scroll_y


    def update_action(this, action: AbstractSyncAction, replace_with: Union[AbstractSyncAction | None] = None) -> None:
        """
        Re-render a specific action in the table

        :param action: the action to be updated in the table
        """

        if (this._sync_manager is None):
            return

        try:
            # Finds the index of the given action
            i = this._displayed_actions.index(action)

            if replace_with is not None:
                this._sync_manager.replace(action, replace_with)
                this._displayed_actions[i] = action = replace_with

            # The action is re-rendered
            columns = this._render_row(action)

            # Colums of the i-th row are updated
            for j in range(len(this.columns)):
                this.update_cell_at(Coordinate(i, j), columns[j], update_width=False)

            this.refresh_row(i)

        except ValueError:  # This exception is raised when the .index_of method fails
            ...

    def clear_selections(this) -> None:
        this._selected_actions.clear()

    async def action_select_all(this) -> None:
        this._selected_actions = list(range(0,len(this._displayed_actions)))
        await this.refresh_table()
    async def action_clear_selections(this) -> None:
        this.clear_selections()
        await this.refresh_table()
    def action_toggle_selection(this):
        # get the action highlighted by the cursor - ie the one the user wants to change
        idx = this.cursor_row

        if idx in this._selected_actions:
            this._selected_actions.remove(idx)
        else:
            this._selected_actions.add(idx)

        this.refresh_row(this.cursor_row)
    async def action_cancel_action(this):
        if (this.changes is None) or (len(this._displayed_actions) == 0):
            return

        for action in this.selected_actions:
            this._sync_manager.cancel_action(action, in_place=True)

        await this.refresh_table()

    def _change_direction_to_action(this, action: AbstractSyncAction, new_direction:ActionDirection) -> AbstractSyncAction:
        new_action = None

        if action.type == ActionType.NOTHING:
            # in the case the action is nothing, it makes an attempt to convert into copy/update
            try:
                new_action = CopySyncAction(action.a, action.b, direction=new_direction)
            except SyncDirectionNotPermittedException:
                ...  # it's already nothing - so nothing to do
        elif action.direction != new_direction:
            try:
                action.swap_direction()
                new_action = action
            except SyncDirectionNotPermittedException:
                new_action = this._sync_manager.cancel_action(action)


        return new_action

    async def action_change_direction(this, key: str) -> None:
        """
        Change the direction of an action. If the action is of type NoSyncAction, it attempts to make it a
        copy/update action (if the direction is permitted)

        :param key: A strig representing "left" or "right"
        """
        if (this.changes is None) or (len(this._displayed_actions) == 0):
            return

        for action in this.selected_actions:
            new_dir = ActionDirection.SRC2DST if key == "right" else ActionDirection.DST2SRC

            new_action = this._change_direction_to_action(action, new_dir)

            if new_action is not None:
                this._sync_manager.replace(action, new_action)

        await this.refresh_table()

    async def action_delete_file(this):
        if (this.changes is None) or (len(this._displayed_actions) == 0):
            return

        for action in this.selected_actions:
            this._sync_manager.convert_to_delete(action, in_place= True)

        await this.refresh_table()

    async def action_change_direction_to_both(this):
        if (this.changes is None) or (len(this._displayed_actions) == 0):
            return

        for action in this.selected_actions:
            action.apply_both_sides()
            this._sync_manager.replace(action, action) # replace with itself to trigger all cascade effects

        await this.refresh_table()

    def _get_row_style(this, row_index: int, base_style: Style) -> Style:
        row_style = super()._get_row_style(row_index, base_style)

        if (len(this._displayed_actions)>0) and (row_index>=0):
            if row_index in this._selected_actions:
                row_style = this.get_component_styles("datatable--selected-row").rich_style
            elif this._displayed_actions[row_index].excluded:
                row_style = this.get_component_styles("datatable--filtered-row").rich_style


        return row_style


    def _render_row(this, action:AbstractSyncAction) -> Tuple[RenderableType, RenderableType, RenderableType]:
        c1, c2, c3 = _render_row(action, this.ordered_columns[1].width)

        styles = Style.null()

        c1 = Text.from_markup(c1)

        try:
            c2 = Text.from_markup(c2, justify="center")
        except TypeError:
            ... # in this case, the c2 cannot be converted into a Text object and will keep as is

        c3 = Text.from_markup(c3)

        c1.overflow = c3.overflow = "ellipsis"

        c1.no_wrap = c2.no_wrap = True

        c1.style = c2.style = c3.style = styles

        return c1,c2,c3

    @on(DataTable.RowHighlighted)
    def on_row_highlighted(this,*args, **kwargs) -> None:
        this._clear_caches()
        for idx in this._selected_actions:
            this.refresh_row(idx)

    @property
    def selected_actions(this) -> Iterable[AbstractSyncAction]:
        selected = this._selected_actions.copy()
        selected.add(this.cursor_row)

        for id in selected:
            yield this._displayed_actions[id]