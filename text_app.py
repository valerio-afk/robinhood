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

from typing import Dict, Tuple, List, Union, ClassVar, Iterable
from rich.text import Text
from rich.console import RenderableType
from rich.style import Style
from textual import on, work, events
from textual.screen import ModalScreen
from textual.app import App, Binding, Widget, ComposeResult
from textual.suggester import Suggester
from textual.worker import get_current_worker
from textual.containers import Container, Horizontal, Vertical
from textual.events import DescendantBlur
from textual.widgets import Header, Footer, Static, Input, Button, Select, DataTable, Label, ProgressBar
from textual.widgets import TextArea,Switch
from textual.widgets.data_table import Column
from textual.renderables.bar import Bar
from textual.coordinate import Coordinate
from backend import SyncMode, RobinHoodBackend, compare_tree, ActionType, SyncEvent, kill_all_subprocesses
from backend import ActionDirection, FileType, apply_changes, FileSystemObject, find_dedupe
from synching import SyncProgress, AbstractSyncAction, SynchingManager, SyncStatus, SyncDirectionNotPermittedException
from synching import CopySyncAction, DeleteSyncAction
from commands import make_command
from filesystem import get_rclone_remotes, AbstractPath, NTAbstractPath, fs_autocomplete, fs_auto_determine, sizeof_fmt
from datetime import datetime
from config import RobinHoodConfiguration, RobinHoodProfile
import re

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

Column.percentage_width = None  # to overcome textual limitations :)

#TODO: it works. needs to be improved recursively
def _get_new_parent(sync:SynchingManager, x : AbstractSyncAction) -> List[Tuple[AbstractSyncAction,AbstractSyncAction]]:
    """
    match homogeneity of nested actions
    :param sync:
    :param x:
    :return:
    """
    parents = x.parents

    new_parents = []


    for parent in parents:

        types = []
        direction = []

        # need to get the new ones because the action passed as parameter has likely changed
        # and not in the nested actions of the parent anymore

        nested_actions = list(sync.get_nested_changes(parent, levels=None))

        # I need to replace the new_parents that are not yet been replaced in the sync manager
        for old, new in new_parents:
            try:
                idx = nested_actions.index(old)
                nested_actions[idx] = new
            except ValueError:
                ... # the old action is not in the list - not the end of the world

        for x in nested_actions:
            types.append(x.type)
            direction.append(x.direction)

        if len(types) > 0:
            # this is the default
            parent_new_action = ActionType.NOTHING
            new_parent_direction = None

            #all_equal3 suggestion taken from https://stackoverflow.com/questions/3844801/check-if-all-elements-in-a-list-are-identical
            #we must make sure that all types & directions are the same
            if (types[1:] == types[:-1]) and (direction[1:] == direction[:-1]):
                parent_new_action = types.pop()
                new_parent_direction = direction.pop()

            # I distinguish 3 cases
            # Case 1: both action and direction (if applicable) are compatible with the current parent - do nothing
            # Case 2: action is the same, but direction not -> swap action/apply both
            # Case 3: neither action nor direction are the same - change action and revert on sync manager

            direct_descentant = parent.get_nested_actions()

            #case 1
            if parent.type == parent_new_action:
                # we don't need to match directions if the action is nothing - it says as is
                if parent_new_action != ActionType.NOTHING:
                    #case 2
                    if new_parent_direction == ActionDirection.BOTH:
                        parent.apply_both_sides()
                    else:
                        parent.swap_direction()

            else: #case 3
                new_action = SynchingManager.make_action(parent.a, parent.b, parent_new_action, new_parent_direction)
                new_action.parent_action = parent.parent_action_view
                new_action.set_nested_actions(direct_descentant)

                new_parents.append((parent, new_action))

            #parent.set_nested_actions(direct_descentant)

    return new_parents


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
    return (c1, "-", c3)


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
        progress_update = action.get_update()

        if progress_update is not None:
            p = progress_update.progress / 100
            c2 = Bar((0, width * p), highlight_style="green1", background_style="dark_green")

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


class FileSystemSuggester(Suggester):

    def __init__(this, *args, **kwargs):
        kwargs['case_sensitive'] = True

        super().__init__(*args, **kwargs)

    async def get_suggestion(this, value: str) -> str | None:
        x = fs_autocomplete(value)

        return x


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

    def __init__(this, results: Union[SynchingManager | None] = None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        this._results = results

    @property
    def pending_actions(this):
        if this.results is not None:
            for _, r in this._results:
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

    # @property
    # def has_pending_actions(this):
    #     for _ in this.pending_actions:
    #         return True
    #
    #     return False

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


class RobinHoodRemoteList(Static):
    def __init__(this, *args, **kwargs):
        super().__init__(*args, **kwargs)

        this.remotes: List[Tuple[str, ...]] = get_rclone_remotes()
        this.border_title = "Remotes"

    def compose(this) -> ComposeResult:
        yield DataTable(cursor_type="row")

    def on_mount(this) -> None:
        header = ("Type", "Drive")
        table = this.query_one(DataTable)

        table.add_columns(*header)

        for r in this.remotes:
            table.add_row(r[0], r[1] + NTAbstractPath.VOLUME_SEPARATOR + NTAbstractPath.PATH_SEPARATOR)


class RobinHoodExcludePath(Static):
    def __init__(this, profile: RobinHoodProfile, *args, **kwargs):
        super().__init__(*args, **kwargs)
        this.border_title = "Path to Exclude"
        this._profile = profile

    @property
    def paths(this) -> Iterable[str]:
        return this._profile.exclusion_filters

    @property
    def exclude_hidden(this) -> bool:
        return this._profile.exclude_hidden_files

    @property
    def deep_comparisons(this) -> bool:
        return this._profile.deep_comparisons

    def compose(this) -> ComposeResult:
        yield TextArea()
        yield Horizontal(
            Label("Exclude hidden files"),
            Switch(id="exclude_hidden"),
            Label("Deep comparison"),
            Switch(id="deep_comparisons"),
            id="other_settings"
        )

    @on(events.Show)
    def show_filters(this) -> None:
        if this.paths is not None:
            textarea = this.query_one(TextArea)
            textarea.load_text("\n".join(this.paths))

        this.query_one("#exclude_hidden").value = this.exclude_hidden
        this.query_one("#deep_comparisons").value = this.deep_comparisons

    @on(events.Hide)
    def save_filters(this) -> None:
        textarea = this.query_one(TextArea)
        new_filters = [line for line in textarea.text.splitlines() if len(line) > 0]

        this._profile.exclusion_filters = new_filters
        this._profile.exclude_hidden_files = this.query_one("#exclude_hidden").value
        this._profile.deep_comparisons = this.query_one("#deep_comparisons").value


class PromptProfileNameModalScreen(ModalScreen):
    BINDINGS = [Binding("escape", "force_close", priority=True)]
    CSS_PATH = "modal.tcss"

    def action_refresh(this):
        this.refresh()

    def compose(self) -> ComposeResult:
        vertical = Vertical(
            Label("Type the name for the new profile"),
            Input(placeholder="Profile name", id="profile_name"),
            id="modal-container")

        yield vertical

    def profile_name(this) -> str:
        return this.query_one("profile_name").value

    def action_force_close(this):
        this.query_one("#profile_name").value = ""
        this.app.pop_screen()

    @on(Input.Submitted)
    def on_submitted(this):
        name = this.query_one("#profile_name").value
        if (len(name) > 0):
            this.app.profile.name = name
            try:
                RobinHoodConfiguration().add_profile(name, this.app.profile)
                this.app.pop_screen()
                this.app.save_profile()
            except ValueError:
                this.query_one("Label").update(
                    Text.from_markup(f"[magenta]Profile name [yellow u]{name}[/yellow u] already in use[/]"))
        else:
            this.action_force_close()


class RobinHood(App):
    CSS_PATH = "main_style.tcss"
    SCREENS = {"NewProfile": PromptProfileNameModalScreen(classes="modal-window")}
    BINDINGS = [
        Binding("ctrl+c", "quit", "Quit", priority=True),
        Binding("ctrl+r", "show_remotes", "Toggle Remote"),
        Binding("ctrl+p", "show_filters", "Toggle Filter List"),
        Binding("ctrl+t", "switch_paths", "Switch Source/Destination Path"),
        Binding("ctrl+s", "save_profile", "Save profile")
    ]

    def __init__(this, profile: Union[RobinHoodProfile | None] = None, *args,
                 **kwargs):  # (this, src=   None, dst=None, syncmode=SyncMode.UPDATE, *args, **kwargs):
        super().__init__(*args, **kwargs)
        this.profile = profile if profile is not None else RobinHoodConfiguration().current_profile
        this._remote_list_overlay: RobinHoodRemoteList = RobinHoodRemoteList(id="remote_list")
        this._tree_pane: DirectoryComparisonDataTable = DirectoryComparisonDataTable(id="tree_pane")
        this._summary_pane: ComparisonSummary = ComparisonSummary(id="summary")
        this._details_pane: FileDetailsSummary = FileDetailsSummary(id="file_details")
        this._progress_bar: ProgressBar = ProgressBar(show_eta=False, id="synch_progbar")
        this._filter_list: RobinHoodExcludePath = RobinHoodExcludePath(profile=profile, id="filter_list")
        this._backend: RobinHoodGUIBackendMananger = RobinHoodGUIBackendMananger(this)
        this._display_filters: DisplayFilters = DisplayFilters(this._tree_pane, classes="hidden")

    # TODO: this seems computationally intensive. Can we find another hook/event?
    def post_display_hook(this) -> None:
        this._tree_pane.adjust_column_sizes()

    def set_status(this, text: str) -> None:
        lbl = Text.from_markup(text)
        lbl.no_wrap = True
        lbl.overflow = ("ellipsis")

        this.query_one("#status_text").update(lbl)

    @on(Select.Changed, "#syncmethod")
    def syncmethod_changed(this, event: Select.Changed) -> None:
        this.query_one("#syncmethod SelectCurrent").remove_class("error")

        this._update_job_related_interface()

    @on(DescendantBlur, "Input")
    def on_blur_input(this, event: DescendantBlur) -> None:
        this._validate_dir_inputs(event.widget)

    def _validate_dir_inputs(this, widget: Widget) -> bool:
        value: str = widget.value
        fail: bool = False

        try:
            filesystem = fs_auto_determine(value, parse_all=True)
            if filesystem is None:
                fail = True
            # else:
            #     setattr(this,attr,filesystem)
        except FileNotFoundError:
            fail = True

        widget.set_class(not fail, "-valid")
        widget.set_class(fail, "-invalid")

        if (fail):
            this.set_status(f"The path [underline yellow]{value}[/] is not valid")
            this.bell()
        else:
            this._update_profile_from_ui()
        return not fail

    @property
    def src(this) -> str:
        return this.query_one("#source_text_area").value

    @src.setter
    def src(this, value: str) -> str:
        this.query_one("#source_text_area").value = value

    @property
    def dst(this) -> str:
        return this.query_one("#dest_text_area").value

    @dst.setter
    def dst(this, value: str) -> str:
        this.query_one("#dest_text_area").value = value

    @property
    def syncmode(this) -> SyncMode:
        return this.query_one("#syncmethod").value

    @property
    def is_working(this) -> bool:
        return this.is_comparing or this.is_synching

    @property
    def is_synching(this) -> bool:
        for w in this.workers:
            if (w.name == "synching") and not w.is_cancelled:
                return True

        return False

    @property
    def is_comparing(this) -> bool:
        for w in this.workers:
            if (w.name == "comparison") and not w.is_cancelled:
                return True

        return False

    @property
    def show_progressbar(this):
        return this._progress_bar.has_class("synching")

    @show_progressbar.setter
    def show_progressbar(this, show: bool):
        if show:
            this._progress_bar.add_class("synching")
        else:
            this._progress_bar.remove_class("synching")

    def _kill_workers(this) -> None:
        for w in this.workers:
            if w.name in ["comparison", "synching"]:
                w.cancel()

    def _update_job_related_interface(this) -> None:
        button = this.query_one("#work_launcher")
        enablable = this.query("#topbar Input, #topbar Select")

        this._display_filters.set_class(this._tree_pane.is_empty, "hidden")
        this._display_filters.set_class(not this._tree_pane.is_empty, "displayed")

        if not this.is_working:
            if (this._summary_pane.has_pending_actions):
                button.variant = "warning"
                button.label = "Synch"
                this.bind("ctrl+n", "compare_again", description="Re-run comparison")
            else:
                button.variant = "success"
                button.label = "Start"

            for x in enablable:
                x.disabled = False

            this.query_one("#dest_text_area").disabled = this.syncmode == SyncMode.DEDUPE

            this.show_progressbar = False
        else:
            button.variant = "error"
            button.label = "Stop"

            this.show_progressbar = this.is_comparing

            for x in enablable:
                x.disabled = True

    def action_save_profile(this):
        if (this.profile.name is None):
            this.push_screen("NewProfile")
        else:
            this.save_profile()

    def save_profile(this):
        cfg = RobinHoodConfiguration()
        cfg.edit_profile(this.profile.name, this.profile)
        cfg.flush()
        this.set_status(f"Profile [yellow]{this.profile.name}[/] saved.")

    def action_compare_again(this) -> None:
        this._summary_pane.results = None
        this._tree_pane.show_results(None)
        this.query_one("#work_launcher").press()

    def action_switch_paths(this) -> None:
        this.src, this.dst = this.dst, this.src
        this._update_profile_from_ui()

    def _update_profile_from_ui(this):
        this.profile.source_path = this.src
        this.profile.destination_path = this.dst

    @on(events.Ready)
    def on_ready(this) -> None:
        this._update_job_related_interface()

    @on(DataTable.RowHighlighted)
    def on_row_selected(this, event: DataTable.RowHighlighted) -> None:

        if event.cursor_row >= 0:
            index = event.cursor_row  # int(event.row_key.value)

            action = this._tree_pane[index]

            this._details_pane.show(action.a, action.b)

    @on(Button.Pressed, "#work_launcher")
    async def work_launcher_pressed(this, event: Button.Pressed) -> None:
        if this.is_working:
            for w in this.workers:
                if w.name in ["comparison", "synching"]:
                    w.cancel()

            kill_all_subprocesses()

            this.set_status("[bright_magenta]Operation stopped[/]")
        elif this._summary_pane.has_pending_actions:
            this._run_synch()
        else:
            if (this.syncmode is None):
                this.query_one("#syncmethod SelectCurrent").add_class("error")
                this.set_status("You must select a synchronization method")
                this.bell()
                return

            if not this._validate_dir_inputs(this.query_one("#source_text_area")):
                return

            if (this.syncmode != SyncMode.DEDUPE) and (
            not this._validate_dir_inputs(this.query_one("#dest_text_area"))):
                return

            if this.syncmode == SyncMode.DEDUPE:
                this._run_dedupe()
            else:
                this._run_comparison(this.syncmode)

        this._update_job_related_interface()

    @work(exclusive=True, name="comparison", thread=True)
    def _run_comparison(this, mode: SyncMode) -> None:
        result = compare_tree(this.src, this.dst, mode=mode, profile=this.profile, eventhandler=this._backend)
        this.call_from_thread(this.show_results, result)

    @work(exclusive=True, name="comparison", thread=True)
    def _run_dedupe(this) -> None:
        result = find_dedupe(this.src, this._backend)
        this.call_from_thread(this.show_results, result)

    @work(exclusive=True, name="synching", thread=True)
    def _run_synch(this) -> None:
        apply_changes(this._tree_pane.changes, eventhandler=this._backend)

    def update_progressbar(this, update: Union[SyncProgress | SyncEvent]):
        if isinstance(update, SyncProgress):
            this._progress_bar.update(total=update.bytes_total, progress=update.bytes_transferred)
        elif isinstance(update, SyncEvent):
            this._progress_bar.update(total=update.total, progress=update.processed)

    def show_results(this, results: Union[SynchingManager | None]) -> None:
        this._tree_pane.show_results(results)
        this._summary_pane.results = results
        this._update_job_related_interface()

    def update_row_at(this, action: AbstractSyncAction):
        this._tree_pane.update_action(action)

    def compose(this) -> ComposeResult:
        this.screen.title = "🏹 Robin Hood"

        if (this.profile.name is not None):
            this.screen.title += f" [{this.profile.name}]"

        this.screen.sub_title = "Steal from the rich and give to the poor"

        yield Header()
        yield RobinHoodTopBar(
            src=this.profile.source_path,
            dst=this.profile.destination_path,
            mode=this.profile.sync_mode,
            id="topbar",
            classes="overlayable")
        yield Vertical(
            Horizontal(
                this._summary_pane,
                this._progress_bar,
                id="summary_block"
            ),
            this._details_pane,
            this._tree_pane,
            this._display_filters,
            id="main_pane",
            classes="overlayable"
        )
        yield this._remote_list_overlay
        yield this._filter_list
        yield Footer()

    def action_show_remotes(this) -> None:
        this.toggle_overlay(this._remote_list_overlay)

    def action_show_filters(this) -> None:
        this.toggle_overlay(this._filter_list)

    def toggle_overlay(this, overlay: Widget) -> None:
        match (overlay.styles.display):
            case "none":
                overlay.styles.display = "block"

                for itm in this.query(".overlayable"):
                    itm.disabled = True

            case _:
                overlay.styles.display = "none"
                for itm in this.query(".overlayable"):
                    itm.disabled = False

    def action_exit(this) -> None:
        this.action_quit()


class RobinHoodGUIBackendMananger(RobinHoodBackend):

    def __init__(this, gui: RobinHood) -> None:
        this._gui = gui

    def update_status(this, text: str) -> None:
        this._gui.call_from_thread(this._gui.set_status, text)

    def update_progressbar(this, update: [SyncProgress | SyncEvent]):
        this._gui.call_from_thread(this._gui.update_progressbar, update)

    def update_table_row(this, action: AbstractSyncAction):
        this._gui.call_from_thread(this._gui.update_row_at, action)

    def clean_up(this):
        this._gui.call_from_thread(this._gui._kill_workers)
        this._gui.call_from_thread(this._gui._update_job_related_interface)

    def _check_running_status(this) -> None:
        if (get_current_worker().is_cancelled):
            raise InterruptedError()

    def before_comparing(this, event: SyncEvent) -> None:
        this._check_running_status()
        this.update_status(f"Initiating directory analysis [underline yellow]{event.value}[/]")

    def on_comparing(this, event: SyncEvent) -> None:
        this._check_running_status()
        this.update_status(f"Analysing [underline yellow]{event.value}[/]")
        this.update_progressbar(event)

    def after_comparing(this, event: SyncEvent) -> None:
        this.clean_up()
        this.update_status(f"[green]Directory comparison finished[/]")

    def before_synching(this, event: SyncEvent) -> None:
        this.update_status(f"Initiating synchronisation ...")

    def on_synching(this, event: SyncEvent) -> None:
        # Check if the thread has been cancelled. This happens when the user click on the "Stop" Button
        this._check_running_status()

        value = event.value

        if isinstance(value, AbstractSyncAction):
            this.update_table_row(value)

        elif isinstance(value, SyncProgress):
            update = value

            # Format information about transfer speed
            transf_speed = f"[green]{update.transfer_speed} {update.transfer_speed_unit}[/green]"

            if (update.prog_transferring is not None):
                # If the action is just one, I can show more specific information about it
                if len(update.prog_transferring) == 1:
                    action = update.prog_transferring[0]

                    # Get the relative path from either src or dest (it shouldn't matter - it's just for displaying purposes)
                    p = action.get_one_path.relative_path

                    # Update the status bar if it's either started or in progress
                    if action.status in [SyncStatus.NOT_STARTED, SyncStatus.IN_PROGRESS]:
                        # Gets a precise label wrt the current action
                        match action.type:
                            case ActionType.DELETE:
                                desc_action = "Deleting"
                            case ActionType.UPDATE | ActionType.COPY:
                                desc_action = "Copying"
                            case _:
                                desc_action = "Doing something with"  # this case should never happen right?

                        # Compose the string to be displayed in the TUI
                        desc = f"{desc_action} [yellow]{p}[/] {transf_speed}"

                        # Update the status label in the TUI
                        this.update_status(desc)

                        # Update the row in the table
                        this.update_table_row(action)
                else:  # This means more files are processes that the same time (bulk operations)

                    for itm in update.prog_transferring:
                        # Update the corresponding
                        this.update_table_row(itm)

                    desc = f"Processing [bold]{len(update.prog_transferring)}[/] file(s) {transf_speed}"
                    this.update_status(desc)

    def after_synching(this, event: SyncEvent) -> None:
        this.clean_up()
        this.update_status(f"[green]Synchronisation finished[/]")

        cmd = make_command(this._gui.profile.on_completion)
        cmd()


class DirectoryComparisonDataTable(DataTable):
    BINDINGS = [
        Binding("space", "cancel_action", "Cancel Action", show=False),
        Binding("left", "change_direction('left')", "Change Action Direction", show=False),
        Binding("right", "change_direction('right')", "Change Action Direction", show=False),
        Binding("=", "change_direction_to_both", "Apply action to both sides", show=False),
        Binding("delete", "delete_file", "Delete File", show=False),
    ]

    def __init__(this, *args, **kwargs):
        kwargs["cursor_type"] = "row"

        super().__init__(*args, **kwargs)

        this.add_column(Text.from_markup("Source Directories", overflow="ellipsis"), key="src", width=45)
        this.add_column(Text.from_markup("Action", overflow="ellipsis"), key="action", width=7)
        this.add_column(Text.from_markup("Destination Directories", overflow="ellipsis"), key="dst", width=45)

        this._sync_manager: [SynchingManager | None] = None
        this._displayed_actions: List[AbstractSyncAction] = []

        this._show_no_action = True
        this._show_copy_update = True
        this._show_delete = True
        this._show_new_files = True
        this._show_filtered = False

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

    @property
    def show_no_action(this) -> bool:
        return this._show_no_action

    @show_no_action.setter
    def show_no_action(this, value: bool) -> None:
        refresh = this._show_no_action != value

        this._show_no_action = value

        if refresh:
            this.refresh_table()

    @property
    def show_copy_update(this) -> bool:
        return this._show_copy_update

    @show_copy_update.setter
    def show_copy_update(this, value: bool) -> None:
        refresh = this._show_copy_update != value

        this._show_copy_update = value

        if refresh:
            this.refresh_table()

    @property
    def show_delete(this) -> bool:
        return this._show_delete

    @show_delete.setter
    def show_delete(this, value: bool) -> None:
        refresh = this._show_delete != value

        this._show_delete = value

        if refresh:
            this.refresh_table()

    @property
    def show_new_files(this) -> bool:
        return this._show_new_files

    @show_new_files.setter
    def show_new_files(this, value: bool) -> None:
        refresh = this._show_new_files != value

        this._show_new_files = value

        if refresh:
            this.refresh_table()

    @property
    def show_filtered(this) -> bool:
        return this._show_filtered

    @show_filtered.setter
    def show_filtered(this, value: bool) -> None:
        refresh = this._show_filtered != value

        this._show_filtered = value

        if refresh:
            this.refresh_table()

    @property
    def is_empty(this) -> bool:
        return (this.changes is None) or (len(this.changes) == 0)

    def __getitem__(this, index: int) -> AbstractSyncAction:
        return this._displayed_actions[index]

    @property
    def changes(this) -> Union[SynchingManager | None]:
        """
        Returns the changes to be applied to the source/destination folders

        :return: The SynchingManager containing the list of changes. It can also return None if the table isn't displaying anything
        """
        return this._sync_manager

    def show_results(this, changes: Union[SynchingManager | None]) -> None:

        if (changes is None):
            return None

        this._sync_manager = changes

        this.refresh_table()

        this.focus()

    def refresh_table(this):
        this.clear(columns=False)

        if (this._sync_manager is None):
            return None

        this._displayed_actions = []

        #
        show_new_files = lambda itm : this.show_new_files or (itm.a.exists and itm.b.exists)
        #
        # is_visible = lambda itm : ( ( (this.show_filtered and is_new_file(itm)) or (not itm.filtered)) and
        #                           (
        #                            (this.show_no_action and (itm.type == ActionType.NOTHING)) or
        #                            (this.show_copy_update and (itm.type in [ActionType.COPY, ActionType.UPDATE])) or
        #                            (this.show_delete and (itm.type == ActionType.DELETE)) or
        #                            is_new_file(itm)
        #                           ) )

        def is_visible(action: AbstractSyncAction):
            if this.show_filtered and action.filtered:
                return show_new_files(action)
            else:
                return (not action.filtered) and show_new_files(action) and \
                       ((this.show_no_action and (action.type == ActionType.NOTHING)) or
                       (this.show_copy_update and (action.type in [ActionType.COPY, ActionType.UPDATE])) or
                       (this.show_delete and (action.type == ActionType.DELETE)) )




        for itm in this._sync_manager.changes:
            if is_visible(itm):
                this._displayed_actions.append(itm)


        rendered_rows = [''] * len(this._displayed_actions)

        for i, x in enumerate(this._displayed_actions):
            rendered_rows[i] = this._render_row(x)

        this.add_rows(rendered_rows)

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

    def action_cancel_action(this):
        if (this.changes is None) or (len(this._displayed_actions) == 0):
            return

        # get the action highlighted by the cursor - ie the one the user wants to change
        action = this._displayed_actions[this.cursor_row]

        # do we really need this guard clause?
        if (action is None):
            return

        actions = list(this._sync_manager.get_nested_changes(action, levels=None)) + [action]

        for itm in actions:
            new_action = this._sync_manager.cancel_action(itm)
            this.update_action(itm, new_action)

        if (len(actions)>0):
            new_parents = _get_new_parent(this._sync_manager, actions[-1])
            for old, new in new_parents:
                this.update_action(old, new)


        this.app.query_one("#summary").refresh()
        this.app._update_job_related_interface()

    def _change_direction_to_action(this, action: AbstractSyncAction, new_direction:ActionDirection):
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
            except SyncDirectionNotPermittedException:
                new_action = this._sync_manager.cancel_action(action)

        # if new_action is not None:
        #     new_action.set_nested_actions(this._sync_manager.get_nested_changes(action, levels=1))

        this.update_action(action, new_action)

    def action_change_direction(this, key: str) -> None:
        """
        Change the direction of an action. If the action is of type NoSyncAction, it attempts to make it a
        copy/update action (if the direction is permitted)

        :param key: A strig representing "left" or "right"
        """
        if (this.changes is None) or (len(this._displayed_actions) == 0):
            return

        action = this._displayed_actions[this.cursor_row]
        new_dir = ActionDirection.SRC2DST if key == "right" else ActionDirection.DST2SRC

        actions = list(this._sync_manager.get_nested_changes(action, levels=None)) + [action]

        for itm in actions:
            this._change_direction_to_action(itm, new_dir)

        if (len(actions) > 0):
            new_parents = _get_new_parent(this._sync_manager, actions[-1])
            for old, new in new_parents:
                this.update_action(old, new)

        this.app.query_one("#summary").refresh()
        this.app._update_job_related_interface()

    def action_delete_file(this):
        if (this.changes is None) or (len(this._displayed_actions) == 0):
            return

        action = this._displayed_actions[this.cursor_row]
        actions = list(this._sync_manager.get_nested_changes(action, levels=None)) + [action]

        for itm in actions:
            new_action = this._sync_manager.convert_to_delete(itm)
            this.update_action(itm, new_action)

        if (len(actions) > 0):
            new_parents = _get_new_parent(this._sync_manager, actions[-1])
            for old, new in new_parents:
                this.update_action(old, new)

        this.app.query_one("#summary").refresh()
        this.app._update_job_related_interface()

    def action_change_direction_to_both(this):
        if (this.changes is None) or (len(this._displayed_actions) == 0):
            return

        # get the action highlighted by the cursor - ie the one the user wants to change
        action = this._displayed_actions[this.cursor_row]

        actions = list(this._sync_manager.get_nested_changes(action, levels=None)) + [action]

        for itm in actions:
            try:

                itm.apply_both_sides()

                this.update_action(itm)
                this.app.query_one("#summary").refresh()
                this.app._update_job_related_interface()
            except (SyncDirectionNotPermittedException, FileNotFoundError):
                ...

        if len(actions)==1:
            old_parent = actions[0].parent_action
            new_parent = _get_new_parent(this._sync_manager, actions[0])
            this.update_action(old_parent, new_parent)

    def _render_row(this, action:AbstractSyncAction) -> Tuple[RenderableType, RenderableType, RenderableType]:
        c1, c2, c3 = _render_row(action, this.ordered_columns[1].width)

        styles = Style.null()

        if action.filtered:
            styles = Style(color="orange_red1")

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

class DisplayFilters(Widget):

    def __init__(this, data_table: DirectoryComparisonDataTable, *args, **kwargs):
        super().__init__(*args, **kwargs)

        this._data_table = data_table

        this._sub_widgets = {
            "No action": Switch(id="df_no_action", value=True),
            "New files": Switch(id="df_new_files", value=True),
            "Copy": Switch(id="df_copy", value=True),
            "Delete": Switch(id="df_delete", value=True),
            "Filtered": Switch(id="df_filtered_files", value=False),
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
        if event.switch == this._sub_widgets["No action"]:
            this._data_table.show_no_action = event.value
        elif event.switch == this._sub_widgets["Copy"]:
            this._data_table.show_copy_update = event.value
        elif event.switch == this._sub_widgets["Delete"]:
            this._data_table.show_delete = event.value
        elif event.switch == this._sub_widgets["New files"]:
            this._data_table.show_new_files = event.value
        elif event.switch == this._sub_widgets["Filtered"]:
            this._data_table.show_filtered = event.value
