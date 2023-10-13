import os.path
from typing import Dict,Tuple, List, Union, ClassVar, Iterable
from rich.text import Text
from rich.console import RenderableType
from textual import on, work, events
from textual.screen import ModalScreen
from textual.app import App, Binding, Widget, ComposeResult
from textual.suggester import Suggester
from textual.worker import get_current_worker
from textual.containers import Container, Horizontal,  Vertical
from textual.events import DescendantBlur
from textual.widgets import Header, Footer, Static, Input, Button, Select, DataTable, Label, ProgressBar, TextArea, Switch
from textual.widgets.data_table import Column
from textual.renderables.bar import Bar
from textual.coordinate import Coordinate
from backend import SyncMode, RobinHoodBackend,compare_tree, ActionType,SyncAction,SyncEvent,kill_all_subprocesses
from backend import ActionDirection, FileType, apply_changes, SyncStatus, SyncProgress, FileSystemObject, find_dedupe
from commands import make_command
from filesystem import get_rclone_remotes, AbstractPath, NTAbstractPath, fs_autocomplete, fs_auto_determine,sizeof_fmt
from datetime import datetime
from config import RobinHoodConfiguration, RobinHoodProfile

_SyncMethodsPrefix:Dict[SyncMode,str] = {
    SyncMode.UPDATE: ">>",
    SyncMode.MIRROR: "->",
    SyncMode.SYNC: "<>",
    SyncMode.DEDUPE: "**"
}

_SyncMethodsNames:Dict[SyncMode,str] = {
    SyncMode.UPDATE: "Update",
    SyncMode.MIRROR: "Mirror",
    SyncMode.SYNC: "Bidirectional sync",
    SyncMode.DEDUPE: "Find deduplicates"
}

SyncMethods:List[Tuple[str,SyncMode]] = [(_SyncMethodsPrefix[x] + " " + str(x).split(".")[1].capitalize(), x) for x in SyncMode]

Column.percentage_width = None #to overcome textual limitations :)

class FileSystemSuggester(Suggester):

    def __init__(this,*args,**kwargs):
        kwargs['case_sensitive'] = True

        super().__init__(*args,**kwargs)

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

    KEY_LABELS=['To upload','To download','To delete (source)','To delete (destination)']

    def __init__(this,results:Union[Iterable[SyncAction]|None]=None,*args,**kwargs) -> None:
        super().__init__(*args,**kwargs)
        this._results = results

    @property
    def pending_actions(this):
        if this.results is not None:
            for r in this._results:
                if (r.status != SyncStatus.SUCCESS) and (r.action_type != ActionType.NOTHING):
                    yield r


    @property
    def has_pending_actions(this):
        for _ in this.pending_actions:
            return True

        return False

    @property
    def results (this):
        return this._results

    @results.setter
    def results(this,new_results:Union[Iterable[SyncAction]|None]) -> None:
        this._results = new_results
        this.refresh()

    @property
    def transfer_bytes(this) -> Tuple[int,int,int,int]:
        upload = 0
        download = 0
        delete_source = 0
        delete_target = 0

        #if (this.results is not None):
        for r in this.pending_actions:
            action = r.action_type

            if (action == ActionType.COPY) or (action == ActionType.UPDATE):
                match r.direction:
                    case ActionDirection.SRC2DST:
                        upload += r.a.size
                    case ActionDirection.DST2SRC:
                        download += r.b.size

            if (action == ActionType.DELETE):
                match r.direction:
                    case ActionDirection.DST2SRC:
                        delete_source += r.a.size
                    case ActionDirection.SRC2DST:
                        delete_target += r.b.size

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

        for lbl,size in zip(this.KEY_LABELS,this.transfer_bytes):
            txt = Text.assemble((f" {lbl} ", base_style + description_style), (f" {sizeof_fmt(size)} ",key_style) )

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

    source_file:FileSystemObject = None
    destination_file:FileSystemObject = None


    @property
    def has_pending_actions(this):
        for _ in this.pending_actions:
            return True

        return False

    def show(this, src_file:Union[FileSystemObject|None], dest_file:Union[FileSystemObject|None]):
        this.source_file = src_file
        this.destination_file = dest_file
        this.refresh()


    @property
    def source_size(this) -> Union[int,None]:
        return None if (this.source_file is None) \
                       or (this.source_file.size is None) or \
                       (this.source_file.size < 0) \
            else this.source_file.size

    @property
    def source_mtime(this) -> Union[datetime|None]:
        return None if this.source_file is None else this.source_file.mtime

    @property
    def destination_size(this) -> Union[int,None]:
        return None if (this.destination_file is None) \
                       or (this.destination_file.size is None) or \
                       (this.destination_file.size<0) \
            else this.destination_file.size

    @property
    def destination_mtime(this) -> Union[datetime|None]:
        return None if this.destination_file is None else this.destination_file.mtime

    @property
    def filename(this) -> Union[str|None]:
        if (this.source_file is None) and (this.destination_file is None):
            return None

        fullpath = this.destination_file.fullpath if this.source_file is None else this.source_file.fullpath

        _, filename = os.path.split(fullpath.absolute_path)

        return filename


    def render(this) -> RenderableType:

        if (this.filename is None):
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

        def _show_formatted_size(x:Union[FileSystemObject|None]):
            return sizeof_fmt(x) if x is not None else "-"

        local_size = _show_formatted_size(this.source_size)
        dest_size  = _show_formatted_size(this.destination_size)


        text.append_text(Text.assemble((f" Filename ", base_style + description_style), (this.filename,key_style)))
        text.append_text(Text.assemble((f" Size (source) ", base_style + description_style), (local_size, key_style)))
        text.append_text(Text.assemble((f" Size (destination) ", base_style + description_style), (dest_size, key_style)))


        return text



class RobinHoodTopBar(Container):
    def __init__(this,
                 src:Union[str|None]=None,
                 dst:Union[str|None]=None,
                 mode:Union[SyncMode|None]=None,
                 *args,
                 **kwargs
                ) -> None:

        this._src:Union[str|None] = src
        this._dst:Union[str|None] = dst
        this._mode:Union[SyncMode|None] = mode

        super().__init__(*args,**kwargs)

    def compose(this) -> ComposeResult:
        yield Label("Welcome to RobinHood", id="status_text")
        yield Horizontal(
            Input(id="source_text_area", placeholder="Source directory",suggester=FileSystemSuggester(),value=this._src),
            Button("Start", id="work_launcher", variant="success"),
            Input(id="dest_text_area", placeholder="Destination directory",value=this._dst),
        Select(SyncMethods, prompt="Sync Mode...", id="syncmethod",value=this._mode),
            id="textbox_container"
        )

class RobinHoodRemoteList(Static):
    def __init__(this,*args,**kwargs):
        super().__init__(*args,**kwargs)

        this.remotes:List[Tuple[str,str]] = get_rclone_remotes()
        this.border_title="Remotes"

    def compose(this) -> ComposeResult:
        yield DataTable(cursor_type="row")


    def on_mount(this) -> None:
        header = ("Type", "Drive")
        table = this.query_one(DataTable)

        table.add_columns(*header)

        for r in this.remotes:
            table.add_row(r[0], r[1] + NTAbstractPath.VOLUME_SEPARATOR + NTAbstractPath.PATH_SEPARATOR)


class RobinHoodExcludePath(Static):
    def __init__(this,*args,**kwargs):
        super().__init__(*args,**kwargs)
        this.border_title="Path to Exclude"


    @property
    def paths(this) -> Iterable[str]:
        return this.app.profile.exclusion_filters

    @property
    def exclude_hidden(this) -> Iterable[str]:
        return this.app.profile.exclude_hidden_files

    @property
    def deep_comparisons(this) -> Iterable[str]:
        return this.app.profile.deep_comparisons

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

        this.query_one("#exclude_hidden").value=this.exclude_hidden
        this.query_one("#deep_comparisons").value = this.deep_comparisons

    @on(events.Hide)
    def save_filters(this) -> None:
        textarea = this.query_one(TextArea)
        new_filters = [line for line in textarea.text.splitlines() if len(line) > 0]

        this.app.profile.exclusion_filters = new_filters
        this.app.profile.exclude_hidden_files =this.query_one("#exclude_hidden").value
        this.app.profile.deep_comparisons = this.query_one("#deep_comparisons").value


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

    def profile_name(this)->str:
        return this.query_one("profile_name").value

    def action_force_close(this):
        this.query_one("#profile_name").value = ""
        this.app.pop_screen()


    @on(Input.Submitted)
    def on_submitted(this):
        name = this.query_one("#profile_name").value
        if (len(name)>0):
            this.app.profile.name = name
            try:
                RobinHoodConfiguration().add_profile(name,this.app.profile)
                this.app.pop_screen()
                this.app.save_profile()
            except ValueError:
                this.query_one("Label").update(Text.from_markup(f"[magenta]Profile name [yellow u]{name}[/yellow u] already in use[/]"))
        else:
            this.action_force_close()



class RobinHood(App):
    CSS_PATH = "main_style.tcss"
    SCREENS = {"NewProfile": PromptProfileNameModalScreen(classes="modal-window")}
    BINDINGS =  [
                Binding("ctrl+c", "quit", "Quit", priority=True),
                Binding("ctrl+r","show_remotes","Toggle Remote"),
                Binding("ctrl+p", "show_filters", "Toggle Path Filter List"),
                Binding("ctrl+t", "switch_paths", "Switch Source/Destination Path"),
                Binding("ctrl+s","save_profile","Save profile")
    ]

    def __init__(this, profile:Union[RobinHoodProfile|None]=None, *args, **kwargs): #(this, src=   None, dst=None, syncmode=SyncMode.UPDATE, *args, **kwargs):
        super().__init__(*args, **kwargs)
        this.profile = profile if profile is not None else RobinHoodConfiguration().current_profile
        this._remote_list_overlay:RobinHoodRemoteList = RobinHoodRemoteList( id="remote_list")
        this._tree_pane:FileTreeTable = FileTreeTable(id="tree_pane")
        this._summary_pane:ComparisonSummary = ComparisonSummary(id="summary")
        this._details_pane:FileDetailsSummary = FileDetailsSummary(id="file_details")
        this._progress_bar:ProgressBar = ProgressBar(show_eta=False,id="synch_progbar")
        this._filter_list:RobinHoodExcludePath = RobinHoodExcludePath(id="filter_list")
        this._backend:RobinHoodGUIBackendMananger = RobinHoodGUIBackendMananger(this)

    def post_display_hook(this) -> None:
        this._tree_pane.adjust_column_sizes()


    def set_status(this, text:str) -> None:
        lbl = Text.from_markup(text)
        lbl.no_wrap=True
        lbl.overflow=("ellipsis")

        this.query_one("#status_text").update(lbl)


    @on(Select.Changed, "#syncmethod")
    def syncmethod_changed(this, event:Select.Changed) -> None:
        this.query_one("#syncmethod SelectCurrent").remove_class("error")

        this._update_job_related_interface()

    @on(DescendantBlur,"Input")
    def on_blur_input(this, event:DescendantBlur) -> None:
        this._validate_dir_inputs(event.widget)

    def _validate_dir_inputs(this,widget:Widget) -> bool:
        value:str = widget.value
        fail:bool = False


        try:
            filesystem = fs_auto_determine(value,parse_all=True)
            if filesystem is None:
                fail=True
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
    def src(this,value:str) -> str:
        this.query_one("#source_text_area").value = value

    @property
    def dst(this) -> str:
        return this.query_one("#dest_text_area").value

    @dst.setter
    def dst(this, value:str) -> str:
        this.query_one("#dest_text_area").value = value

    @property
    def syncmode(this) -> SyncMode :
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
    def show_progressbar(this, show:bool):
        if show:
            this._progress_bar.add_class("synching")
        else:
            this._progress_bar.remove_class("synching")

    def _kill_workers(this) -> None:
        for w in this.workers:
            if w.name in ["comparison","synching"]:
                w.cancel()

    def _update_job_related_interface(this) -> None:
        button = this.query_one("#work_launcher")
        enablable = this.query("#topbar Input, #topbar Select")

        if not this.is_working:
            if (this._summary_pane.has_pending_actions):
                button.variant = "warning"
                button.label = "Synch"
                this.bind("ctrl+n","compare_again",description="Re-run comparison")
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
                x.disabled=True


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
    def on_row_selected(this, event:DataTable.RowHighlighted) -> None:

        if event.cursor_row >= 0:
            index = event.cursor_row #int(event.row_key.value)

            action = this._tree_pane[index]

            this._details_pane.show(action.a, action.b)


    @on(Button.Pressed,"#work_launcher")
    async def work_launcher_pressed(this,event:Button.Pressed) -> None:
        if this.is_working:
            for w in this.workers:
                if w.name in ["comparison","synching"]:
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

            if not this._validate_dir_inputs(this.query_one("#source_text_area")) :
                return

            if  (this.syncmode != SyncMode.DEDUPE) and  (not this._validate_dir_inputs(this.query_one("#dest_text_area"))):
                return

            match this.syncmode:
                case SyncMode.UPDATE:
                    this._run_update()
                case SyncMode.MIRROR:
                    this._run_mirror()
                case SyncMode.DEDUPE:
                    this._run_dedupe()
                case _:
                    raise NotImplementedError("Sync mode not implemented yet!")

        this._update_job_related_interface()


    @work(exclusive=True,name="comparison",thread=True)
    def _run_update(this) -> None:
        result = compare_tree(this.src, this.dst, mode=SyncMode.UPDATE, profile=this.profile, eventhandler=this._backend)
        this.show_results(result)

    @work(exclusive=True,name="comparison",thread=True)
    def _run_mirror(this) -> None:
        result = compare_tree(this.src, this.dst, mode=SyncMode.MIRROR, profile=this.profile, eventhandler=this._backend)
        this.show_results(result)

    @work(exclusive=True, name="comparison", thread=True)
    def _run_dedupe(this) -> None:
        result = find_dedupe(this.src, this._backend)
        this.show_results(result)

    @work(exclusive=True,name="synching",thread=True)
    def _run_synch(this) -> None:

        # Makes source/destination Paths to help the bulk operation manager in apply_changes
        source = AbstractPath.make_path(this.src)
        destination = AbstractPath.make_path(this.dst) if this.syncmode != SyncMode.DEDUPE else None

        apply_changes(this._tree_pane.results,
                      local=source,
                      remote=destination,
                      eventhandler=this._backend
        )




    def update_progressbar(this, update:Union[SyncProgress|SyncEvent]):
        if isinstance(update,SyncProgress):
            this._progress_bar.update(total=update.bytes_total,progress=update.bytes_transferred)
        elif isinstance(update,SyncEvent):
            this._progress_bar.update(total=update.total,progress=update.processed)



    def show_results(this,results:Union[Iterable[SyncAction]|None]) -> None:
        this._tree_pane.show_results(results)
        this._summary_pane.results = results
        this._update_job_related_interface()

    def update_row_at(this,action:SyncAction):
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
    def toggle_overlay(this,overlay:Widget) -> None:
        match(overlay.styles.display):
            case "none":
                overlay.styles.display = "block"

                for itm in this.query(".overlayable"):
                    itm.disabled=True

            case _:
                overlay.styles.display = "none"
                for itm in this.query(".overlayable"):
                    itm.disabled = False

    def action_exit(this) -> None:
        this.action_quit()


class RobinHoodGUIBackendMananger(RobinHoodBackend):

    def __init__(this, gui: RobinHood) -> None:
        this._gui = gui


    def update_status(this,text:str)->None:
        this._gui.call_from_thread(this._gui.set_status,text)

    def update_progressbar(this, update:[SyncProgress|SyncEvent]):
        this._gui.call_from_thread(this._gui.update_progressbar, update)


    def update_table_row(this, action:SyncAction):
        this._gui.call_from_thread(this._gui.update_row_at, action)

    def clean_up(this):
        this._gui.call_from_thread(this._gui._kill_workers)
        this._gui.call_from_thread(this._gui._update_job_related_interface)

    def _check_running_status(this) -> None:
        if (get_current_worker().is_cancelled):
            raise InterruptedError()


    def before_comparing(this, event:SyncEvent) -> None:
        this._check_running_status()
        this.update_status(f"Initiating directory analysis [underline yellow]{event.value}[/]")


    def on_comparing(this, event:SyncEvent) -> None:
        this._check_running_status()
        this.update_status(f"Analysing [underline yellow]{event.value}[/]")
        this.update_progressbar(event)


    def after_comparing(this, event:SyncEvent) -> None:
        this.clean_up()
        this.update_status(f"[green]Directory comparison finished[/]")

    def before_synching(this, event:SyncEvent) -> None:
        this.update_status(f"Initiating synchronisation ...")

    def on_synching(this, event:SyncEvent) -> None:
        # Check if the thread has been cancelled. This happens when the user click on the "Stop" Button
        this._check_running_status()

        value = event.value

        if isinstance(value, SyncAction):
            this.update_table_row(value)

        elif isinstance(value,SyncProgress):
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
                        match action.action_type:
                            case ActionType.DELETE:
                                desc_action = "Deleting"
                            case ActionType.MKDIR:
                                desc_action = "Creating directory"
                            case ActionType.UPDATE | ActionType.COPY:
                                desc_action = "Copying"
                            case _:
                                desc_action = "Doing something with" # this case should never happen right?

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

                    #TODO To show transfer speed somehow

                    # The update param is set to the same value for global transfer speed. Getting the last one is fine
                    # update = itm.update
                    #
                    # if update is not None:
                    #     desc += f" [green]{update.transfer_speed} {update.transfer_speed_unit}[/yellow]"

                    this.update_status(desc)



    def after_synching(this, event:SyncEvent) -> None:
        this.clean_up()
        this.update_status(f"[green]Synchronisation finished[/]")

        cmd = make_command(this._gui.profile.on_completion)
        cmd()


class FileTreeTable(DataTable):

    def __init__(this,*args,**kwargs):
        kwargs["cursor_type"]="row"

        super().__init__(*args,**kwargs)

        this.add_column(Text.from_markup("Source Directories",overflow="ellipsis"),key="src",width=45)
        this.add_column(Text.from_markup("Action",overflow="ellipsis"),key="action",width=7)
        this.add_column(Text.from_markup("Destination Directories",overflow="ellipsis"),key="dst",width=45)

        this._results = None

    def adjust_column_sizes(this) -> None:
        psize = this.size.width-6

        if this.virtual_size.height > this.size.height:
            psize-=2 #thickness of the scrollbar

        tot_size=0

        columns = list(this.columns.values())


        #if all([c for c in size]):

        for c in columns:
            c.auto_width = False
            if (not hasattr(c, "percentage_width") or (c.percentage_width is None)):
                c.percentage_width = c.width

            c.width = int( psize * (c.percentage_width / 100))
            tot_size += c.width

        if tot_size != psize:
            delta = psize - tot_size

            unit = 1 if delta>0 else -1

            if (delta<0):
                delta*=-1

            i = 0

            while delta>0:
                columns[i%len(columns)].width += unit
                i+=1
                delta-=1




        this.refresh()


    def __getitem__(this, index:int):
        if this.results is None:
            raise IndexError()

        res = this.results
        if (not isinstance(res,list)):
            res = [x for x in res]

        return res[index]

    @property
    def results (this) -> Union[Iterable[SyncAction]|None]:
        return this._results

    def show_results(this,results:List[SyncAction]) -> None:
        this.clear(columns=False)

        this._results = results

        if (results is None):
            return None


        this._results = sorted(this._results,key=lambda x : str(x.action_type))

        rendered_rows = [None] * len(this._results)

        for i,x in enumerate(this._results):
            rendered_rows[i] = this._render_row(x)

        this.add_rows(rendered_rows)
        #this.add_row(*rendered_row,key=str(i))


        this.focus()

    def update_action(this, action:SyncAction):
        if (this._results is None): return

        # The _result property can be any iterable
        # As I like the .index method, I convert it into a list if necessary
        results = this._results

        if (not isinstance(this._results,list)):
            results = [x for x in results]

        try:
            i = results.index(action)
            columns = this._render_row(action)

            for j in range(len(this.columns)):
                this.update_cell_at(Coordinate(i,j),columns[j],update_width=False)
        except ValueError:
            ...


    def on_key(this,event:events.Key):

        if this._results is None:
            return

        action = this._results[this.cursor_row]

        if (action is not None):
            match event.name:
                case "space":
                    action.action_type = ActionType.NOTHING
                    this.update_action(action)
                case "right":
                    if action.action_type == ActionType.NOTHING:
                        if (action.a is not None) and action.a.exists:
                            action.direction = ActionDirection.SRC2DST
                            if action.a.type == FileType.DIR:
                                action.action_type = ActionType.MKDIR
                            else:
                                action.action_type = ActionType.UPDATE if action.b.exists else ActionType.COPY
                    elif action.direction != ActionDirection.SRC2DST:
                        if (((action.action_type == ActionType.UPDATE) or action.action_type.COPY) and action.a.exists) or \
                           ((action.action_type == ActionType.DELETE) and (action.b.exists)) or \
                           (action.action_type == ActionType.MKDIR):
                            action.direction = ActionDirection.SRC2DST
                        else:
                            action.action_type = ActionType.NOTHING
                case "left":
                    if action.action_type == ActionType.NOTHING:
                        if (action.b is not None) and action.b.exists:
                            action.direction = ActionDirection.DST2SRC
                            if action.b.type == FileType.DIR:
                                action.action_type = ActionType.MKDIR
                            else:
                                action.action_type = ActionType.UPDATE if action.a.exists else ActionType.COPY
                    elif action.direction != ActionDirection.DST2SRC:
                        if (((action.action_type == ActionType.UPDATE) or action.action_type.COPY) and action.b.exists) or \
                           ((action.action_type == ActionType.DELETE) and action.a.exists) or \
                           (action.action_type == ActionType.MKDIR):
                            action.direction = ActionDirection.DST2SRC
                        else:
                            action.action_type = ActionType.NOTHING
                case "delete":
                    action.action_type = ActionType.DELETE

                    if action.direction is None:
                        action.direction = ActionDirection.SRC2DST

                    match action.direction:
                        case ActionDirection.SRC2DST:
                            if not action.b.exists:
                                action.direction = ActionDirection.DST2SRC

                        case ActionDirection.DST2SRC:
                            if not action.a.exists:
                                action.direction = ActionDirection.SRC2DST



            this.update_action(action)
            this.app.query_one("#summary").refresh()
            this.app._update_job_related_interface()


    def _render_row(this,x:SyncAction):
        match x.action_type:
            case ActionType.MKDIR | ActionType.COPY:
                dir_frm = frm_src = frm_dest = "[green]"
            case ActionType.UPDATE:
                dir_frm = frm_src = frm_dest = "[bright_green]"
            case ActionType.DELETE:
                frm_src = '[magenta]'
                frm_dest = '[s magenta]'
                dir_frm = "[magenta]"
            case ActionType.UNKNOWN:
                dir_frm = frm_src = frm_dest = "[yellow]"

            case _:
                dir_frm = frm_src = frm_dest = "[grey]"

        match x.action_type:
            case ActionType.NOTHING: direction = "-"
            case ActionType.UNKNOWN: direction = "?"
            case _: direction = x.direction.value

        src = ""
        dst = ""

        icon_src = ""
        icon_dst = ""

        def _make_suitable_icon(x:FileSystemObject):
            if x.exists:
                return ":open_file_folder:" if x.type == FileType.DIR else ":page_facing_up:"
            else:
                return ":white_medium_star:[i]"

        #if (x.action_type == ActionType.NOTHING ) or ((x.a is not None) and (x.direction==ActionDirection.SRC2DST) ):
        if x.a is not None:
            icon_src = _make_suitable_icon(x.a)
            src = x.a.relative_path

        #if (x.action_type == ActionType.NOTHING ) or ((x.b is not None) and (x.direction==ActionDirection.DST2SRC) ):
        if x.b is not None:
            icon_dst = _make_suitable_icon(x.b)
            dst = x.b.relative_path



        if x.direction == ActionDirection.DST2SRC:
            frm_src, frm_dest = frm_dest, frm_src


        src_column = Text.from_markup(f"{frm_src}{icon_src}{src}[/]")
        dst_column = Text.from_markup(f"{frm_dest}{icon_dst}{dst}[/]")

        src_column.overflow = dst_column.overflow = "ellipsis"
        src_column.no_wrap = dst_column.no_wrap = True


        central_column = Text.from_markup(f"{dir_frm}{direction}[/]", justify="center")

        progress_update = x.get_update()

        if (x.status == SyncStatus.SUCCESS):
            central_column = Text.from_markup(":white_heavy_check_mark:", justify="center")
        elif (x.status == SyncStatus.FAILED):
            central_column = Text.from_markup(":cross_mark:", justify="center")
        elif (x.status == SyncStatus.IN_PROGRESS) and (progress_update is not None):
            central_column_size = this.ordered_columns[1].width
            p = progress_update.progress/100

            central_column = Bar((0,central_column_size*p),highlight_style="green1",background_style="dark_green")


        return (
            src_column,
            central_column,
            dst_column
        )