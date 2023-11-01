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
from typing import Tuple, List, Union, ClassVar, Iterable, Set
from rich.text import Text
from rich.console import RenderableType
from textual import on, work, events
from textual.screen import ModalScreen
from textual.app import App, Binding, Widget, ComposeResult
from textual.worker import Worker, WorkerState
from textual.containers import  Horizontal, Vertical
from textual.events import DescendantBlur
from textual.messages import ExitApp, Message
from textual.widgets import Header, Footer, Static, Input, Button, Select, DataTable, Label, ProgressBar
from textual.widgets import TextArea,Switch
from backend import SyncMode, RobinHoodBackend, ActionType, SyncEvent
from new_backend import compare_tree
from backend import  apply_changes, find_dedupe
from synching import SyncProgress, AbstractSyncAction, SynchManager, SyncStatus
from commands import make_command
from filesystem import NTAbstractPath,  fs_auto_determine, rclone_instance
from config import RobinHoodConfiguration, RobinHoodProfile
from widgets import ComparisonSummary, DisplayFilters, FileDetailsSummary, RobinHoodTopBar, DirectoryComparisonDataTable


class RobinHoodRemoteList(Static):
    def __init__(this, *args, **kwargs):
        super().__init__(*args, **kwargs)

        this.remotes: List[Tuple[str, ...]] = []
        this.border_title = "Remotes"

    def compose(this) -> ComposeResult:
        yield DataTable(cursor_type="row")

    async def on_mount(this) -> None:

        if len(this.remotes)==0:
            this.remotes = await rclone_instance().list_remotes()


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


class StatusUpdate(Message):
    def __init__(this, text:RenderableType, processed=None, total = None):
        super().__init__()
        this.text = text
        this.processed = processed
        this.total = total


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
        this._display_filters: DisplayFilters = DisplayFilters( classes="hidden")

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
    async def on_blur_input(this, event: DescendantBlur) -> None:
        await this._validate_dir_inputs(event.widget)

    @on(DisplayFilters.FilterChanged)
    async def on_filter_changed(this, event:DisplayFilters.FilterChanged):
        match event.filter_type:
            case DisplayFilters.FilterType.NO_ACTION:
                this._tree_pane.show_no_action = event.value
            case DisplayFilters.FilterType.NEW_FILES:
                this._tree_pane.show_new_files = event.value
            case DisplayFilters.FilterType.COPY:
                this._tree_pane.show_copy_update = event.value
            case DisplayFilters.FilterType.DELETE:
                this._tree_pane.show_delete = event.value
            case DisplayFilters.FilterType.EXCLUDED:
                this._tree_pane.show_excluded = event.value
            case DisplayFilters.FilterType.PATTERN:
                this._tree_pane.filter_by_name = event.value

    @on(DirectoryComparisonDataTable.TableRefreshed)
    async def on_table_refreshed(this, event:DirectoryComparisonDataTable.TableRefreshed):
        this._summary_pane.refresh()


    async def _validate_dir_inputs(this, widget: Widget) -> bool:
        value: str = widget.value
        fail: bool = False

        try:
            filesystem = await fs_auto_determine(value, parse_all=True)
            if filesystem is None:
                fail = True
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
            if (w.name == "synching") and (w.state == WorkerState.RUNNING):
                return True

        return False

    @property
    def is_comparing(this) -> bool:
        for w in this.workers:
            if (w.name == "comparison") and (w.state == WorkerState.RUNNING):
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

    async def _update_job_related_interface(this) -> None:
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

    async def action_compare_again(this) -> None:
        this._summary_pane.results = None
        await this._tree_pane.show_results(None)
        this.query_one("#work_launcher").press()

    def action_switch_paths(this) -> None:
        this.src, this.dst = this.dst, this.src
        this._update_profile_from_ui()

    def _update_profile_from_ui(this):
        this.profile.source_path = this.src
        this.profile.destination_path = this.dst

    @on(ExitApp)
    async def on_quit(this):
        await rclone_instance().quit()

    @on(events.Ready)
    async def on_ready(this) -> None:
        await this._update_job_related_interface()

    @on(StatusUpdate)
    async def update_status(this, event:StatusUpdate):
        this.set_status(event.text)
        if (event.processed is not None) and (event.total is not None):
            this.update_progressbar(processed=event.processed, total=event.total)

    @on(DataTable.RowHighlighted)
    async def on_row_selected(this, event: DataTable.RowHighlighted) -> None:

        if event.cursor_row >= 0:
            index = event.cursor_row  # int(event.row_key.value)

            action = this._tree_pane[index]

            this._details_pane.show(action.a, action.b)

    @on(Button.Pressed, "#work_launcher")
    async def work_launcher_pressed(this, event: Button.Pressed) -> None:
        if this.is_working:
            for w in this.workers:
                if (w.name in ["comparison", "synching"]) and (w.state == WorkerState.RUNNING):
                    w.cancel()

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

        #await this._update_job_related_interface()


    async def on_worker_state_changed(this, event: Worker.StateChanged) -> None:
        await this._update_job_related_interface()

    @work(exclusive=True, name="comparison")
    async def _run_comparison(this, mode: SyncMode) -> None:
        result = await compare_tree(src=this.src, dest=this.dst, mode=mode, profile=this.profile, eventhandler=this._backend)
        await this.show_results(result)


    @work(exclusive=True, name="comparison", thread=True)
    def _run_dedupe(this) -> None:
        result = find_dedupe(this.src, this._backend)
        this.call_from_thread(this.show_results, result)

    @work(exclusive=True, name="synching", thread=True)
    def _run_synch(this) -> None:
        apply_changes(this._tree_pane.changes, eventhandler=this._backend)

    def update_progressbar(this, processed, total):
        # if isinstance(update, SyncProgress):
        #     this._progress_bar.update(total=update.bytes_total, progress=update.bytes_transferred)
        # elif isinstance(update, SyncEvent):
        this._progress_bar.update(total=total, progress=processed)

    async def show_results(this, results: Union[SynchManager | None]) -> None:
        await this._tree_pane.show_results(results)
        this._summary_pane.results = results
        await this._update_job_related_interface()

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

    def update_status(this, text: RenderableType, processed = None, total=None) -> None:
        this._gui.post_message(StatusUpdate(text, processed, total))

    def update_table_row(this, action: AbstractSyncAction):
        this._gui.call_from_thread(this._gui.update_row_at, action)

    def before_comparing(this, event: SyncEvent) -> None:
        this.update_status(f"Initiating directory analysis [underline yellow]{event.value}[/]")

    def on_comparing(this, event: SyncEvent) -> None:
        this.update_status(f"Analysing [underline yellow]{event.value}[/]", processed=event.processed, total=event.total)

    def after_comparing(this, event: SyncEvent) -> None:
        this.update_status(f"[green]Directory comparison finished[/]")

    def before_synching(this, event: SyncEvent) -> None:
        this.update_status(f"Initiating synchronisation ...")

    def on_synching(this, event: SyncEvent) -> None:
        # Check if the thread has been cancelled. This happens when the user click on the "Stop" Button

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



