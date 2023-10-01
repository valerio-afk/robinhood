import os.path
from typing import Dict,Tuple, List, Union, ClassVar, Iterable
from rich.text import Text
from rich.console import RenderableType
from textual import on, work
from textual.app import App, Binding, Widget, ComposeResult
from textual.suggester import Suggester
from textual.worker import get_current_worker
from textual.containers import Container, Horizontal, VerticalScroll, Vertical
from textual.events import DescendantBlur
from textual.widgets import Header, Footer, Static, Input, Button, Select, DataTable, Label, ProgressBar
from textual.widgets.data_table import Column
from textual.coordinate import Coordinate
from backend import SyncMode, RobinHoodBackend,compare_tree, ActionType,SyncAction,SyncEvent, RobinHoodConfiguration
from backend import ActionDirection, FileType, apply_changes, SyncStatus, SyncProgress, FileSystemObject
from filesystem import get_rclone_remotes,NTPathManager, fs_autocomplete, fs_auto_determine,sizeof_fmt
from datetime import datetime

_SyncMethodsPrefix:Dict[SyncMode,str] = {
    SyncMode.UPDATE: ">>",
    SyncMode.MIRROR: "->",
    SyncMode.SYNC: "<>"
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
                    case ActionDirection.SRC2DST:
                        delete_source += r.a.size
                    case ActionDirection.DST2SRC:
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
        return None if this.source_file is None else this.source_file.size

    @property
    def source_mtime(this) -> Union[datetime|None]:
        return None if this.source_file is None else this.source_file.mtime

    @property
    def destination_size(this) -> Union[int,None]:
        return None if this.source_file is None else this.source_file.size

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

        local_size = sizeof_fmt(this.source_size) if this.source_size is not None else "-"
        dest_size  = sizeof_fmt(this.destination_size) if this.destination_size is not None else "-"


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
            table.add_row(r[0],r[1]+NTPathManager.VOLUME_SEPARATOR+NTPathManager.PATH_SEPARATOR)



class RobinHood(App):
    CSS_PATH = "topbar.tcss"
    BINDINGS =  [
                Binding("ctrl+c", "quit", "Quit", priority=True),
                Binding("ctrl+r","show_remotes","Show Remote"),
    ]

    def __init__(this, *args, **kwargs): #(this, src=None, dst=None, syncmode=SyncMode.UPDATE, *args, **kwargs):
        super().__init__(*args, **kwargs)
        this._remote_list_overlay:RobinHoodRemoteList = RobinHoodRemoteList( id="remote_list")
        this._tree_pane:FileTreeTable = FileTreeTable(id="tree_pane")
        this._summary_pane:ComparisonSummary = ComparisonSummary(id="summary")
        this._details_pane:FileDetailsSummary = FileDetailsSummary(id="file_details")
        this._progress_bar:ProgressBar = ProgressBar(id="synch_progbar")
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
        #this.syncmode = event.value
        this.query_one("#syncmethod SelectCurrent").remove_class("error")

    @on(DescendantBlur,"Input")
    def on_blur_input(this, event:DescendantBlur) -> None:
        this._validate_dir_inputs(event.widget)

    def _validate_dir_inputs(this,widget:Widget) -> bool:
        value:str = widget.value
        fail:bool = False
        #attr = None

        # match widget.id:
        #     case "source_text_area":
        #         attr = "src"
        #     case "dest_text_area":
        #         attr = "dst"

        #if (attr is not None):
            #fs = getattr(this,attr)
            #if (value is not None) and ((fs is None) or (fs.root!=value)):
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

        return not fail

    @property
    def src(this) -> str:
        return this.query_one("#source_text_area").value

    @property
    def dst(this) -> str:
        return this.query_one("#dest_text_area").value

    @property
    def syncmode(this) -> SyncMode :
        return this.query_one("#syncmethod").value

    @property
    def is_working(this) -> bool:
        for w in this.workers:
            if (w.name in ["comparison","synching"]) and w.is_running:
                return True

        return False

    @property
    def is_synching(this) -> bool:
        for w in this.workers:
            if (w.name == "synching") and w.is_running:
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

            this.show_progressbar = False
        else:
            button.variant = "error"
            button.label = "Stop"

            if (this.is_synching):
                this.show_progressbar = True

            for x in enablable:
                x.disabled=True

    def action_compare_again(this) -> None:
        this._summary_pane.results = None
        this._tree_pane.show_results(None)
        this.query_one("#work_launcher").press()


    @on(DataTable.RowSelected)
    def on_row_selected(this, event:DataTable.RowSelected) -> None:
        index = int(event.row_key.value)

        action = this._tree_pane[index]

        this._details_pane.show(action.a, action.b)


    @on(Button.Pressed,"#work_launcher")
    async def work_launcher_pressed(this,event:Button.Pressed) -> None:
        if this.is_working:
            for w in this.workers:
                if w.name in ["comparison","synching"]:
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

            if not (this._validate_dir_inputs(this.query_one("#source_text_area")) and this._validate_dir_inputs(this.query_one("#dest_text_area"))):
                return

            match this.syncmode:
                case SyncMode.UPDATE:
                    this._run_update()#,name="comparison", exclusive=True)
                case _:
                    raise NotImplementedError("Sync mode not implemented yet!")

        this._update_job_related_interface()


    @work(exclusive=True,name="comparison",thread=True)
    def _run_update(this) -> None:
        result = compare_tree(this.src, this.dst, this._backend)
        this.show_results(result)

    @work(exclusive=True,name="synching",thread=True)
    def _run_synch(this) -> None:
        apply_changes(this._tree_pane.results,this._backend)


    def update_progressbar(this, update:SyncProgress):
        this._progress_bar.update(total=update.bytes_total,
                                  progress=update.bytes_transferred
                                  )


    def show_results(this,results:Union[Iterable[SyncAction]|None]) -> None:
        this._tree_pane.show_results(results)
        this._summary_pane.results = results
        this._update_job_related_interface()

    def update_row_at(this,action:SyncAction):
        this._tree_pane.update_action(action)


    def compose(this) -> ComposeResult:
        this.screen.title = "🏹 Robin Hood"
        this.screen.sub_title = "Steal from the rich and give to the poor"

        yield Header()
        yield RobinHoodTopBar(
            src=RobinHoodConfiguration().source_path,
            dst=RobinHoodConfiguration().destination_path,
            mode=RobinHoodConfiguration().sync_mode,
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
        yield Footer()

    def action_show_remotes(this) -> None:
        match(this._remote_list_overlay.styles.display):
            case "none":
                this._remote_list_overlay.styles.display = "block"

                for itm in this.query(".overlayable"):
                    itm.disabled=True

                #this._remote_list_overlay.focus()
            case _:
                this._remote_list_overlay.styles.display = "none"
                for itm in this.query(".overlayable"):
                    itm.disabled = False

    def action_exit(this) -> None:
        this.action_quit()


class RobinHoodGUIBackendMananger(RobinHoodBackend):

    def __init__(this, gui: RobinHood) -> None:
        this._gui = gui


    def update_status(this,text:str)->None:
        this._gui.call_from_thread(this._gui.set_status,text)

    def update_progressbar(this, update:SyncProgress):
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


    def after_comparing(this, event:SyncEvent) -> None:
        this.clean_up()
        this.update_status(f"[green]Directory comparison finished[/]")

    def before_synching(this, event:SyncEvent) -> None:
        this.update_status(f"Initiating synchronisation ...")

    def on_synching(this, event:SyncEvent) -> None:
        this._check_running_status()

        action:SyncAction = event.value

        p = action.get_one_path.relative_path

        if action.status in [SyncStatus.NOT_STARTED, SyncStatus.IN_PROGRESS]:
            desc_action = ""
            more_info = ""

            update = action.get_update()

            match action.action_type:
                case ActionType.DELETE:
                    desc_action = "Deleting"
                case ActionType.MKDIR:
                    desc_action = "Creating directory"
                case ActionType.UPDATE | ActionType.COPY:
                    desc_action = "Copying"

                    if update is not None:
                        more_info = f"{update.progress}%"

            desc = f"{desc_action} {p} {more_info}"
            this.update_status(desc)

            if update is not None:
                this.update_progressbar(update)

        this.update_table_row(action)


    def after_synching(this, event:SyncEvent) -> None:
        this.clean_up()
        this.update_status(f"[green]Synchronisation finished[/]")


class FileTreeTable(DataTable):

    def __init__(this,*args,**kwargs):
        kwargs["cursor_type"]="row"

        super().__init__(*args,**kwargs)

        this.add_column("Source Directories",key="src",width=45)
        this.add_column("Action",key="action",width=10)
        this.add_column("Destination Directories",key="dst",width=45)

        this._results = None

    def adjust_column_sizes(this) -> None:
        size = this.size
        psize = this.parent.size[0]

        tot_size=0

        if all([c for c in size]):

            for c in this.columns.values():
                c.auto_width = False
                if (not hasattr(c, "percentage_width") or (c.percentage_width is None)):
                    c.percentage_width = c.width

                c.width = int( psize * (c.percentage_width / 100))
                tot_size += c.width

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
        for r in list(this.rows.keys())[::-1]:
            this.remove_row(r)

        this._results = results

        if (results is None):
            return None


        this._results = sorted(this._results,key=lambda x : str(x.a))

        for i,x in enumerate(this._results):
            rendered_row = FileTreeTable._render_row(x)
            this.add_row(*rendered_row,key=str(i))


        this.focus()

    def update_action(this, action:SyncAction):
        if (this._results is None): return

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



    @classmethod
    def _render_row(cls,x:SyncAction):
        match x.action_type:
            case ActionType.MKDIR | ActionType.COPY:
                dir_frm = frm = "[green]"
            case ActionType.UPDATE:
                dir_frm = frm = "[bright_green]"
            case ActionType.DELETE:
                frm = '[s magenta]'
                dir_frm = "[magenta]"
            case _:
                dir_frm = frm = "[grey]"

        direction = "-" if x.action_type == ActionType.NOTHING else x.direction.value

        src = ""
        dst = ""

        icon_src = ""
        icon_dst = ""

        if (x.action_type == ActionType.NOTHING ) or ((x.a is not None) and (x.direction==ActionDirection.SRC2DST) ):
            icon_src = ":open_file_folder:" if x.a.type == FileType.DIR else ":page_facing_up:"
            src = x.a.relative_path

        if (x.action_type == ActionType.NOTHING ) or ((x.b is not None) and (x.direction==ActionDirection.DST2SRC) ):
            icon_dst = ":open_file_folder:" if x.b.type == FileType.DIR else ":page_facing_up:"
            dst = x.b.relative_path

        src_column = Text.from_markup(f"{frm}{icon_src}{src}[/]")
        dst_column = Text.from_markup(f"{frm}{icon_dst}{dst}[/]")

        src_column.overflow = dst_column.overflow = "ellipsis"
        src_column.no_wrap = dst_column.no_wrap = True

        central_column = f"{dir_frm}{direction}[/]"

        if (x.status == SyncStatus.SUCCESS):
            central_column = ":white_heavy_check_mark:"
        elif (x.status == SyncStatus.FAILED):
            central_column = ":cross_mark:"

        return (
            src_column,
            Text.from_markup(central_column, justify="center"),
            dst_column
        )