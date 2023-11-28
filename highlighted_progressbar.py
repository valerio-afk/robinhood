from typing import Optional
from rich.console import Console, RenderableType, JustifyMethod
from rich.text import Text
from rich.style import Style

BACKGROUND = Style(color="white", bgcolor="#333333")
BAR = Style(color="black", bgcolor="#0087d7")
CURSOR = Style(color="black", bgcolor="#00afff")

class HighlightedProgressBar:

    def __init__(this, total:Optional[float]=None,size:Optional[int]=None,background:Style=BACKGROUND, bar:Style=BAR,cursor:Style=CURSOR):

        this.console = Console()

        if size is not None:
            assert size>0 , "The size of the progress bar must be greater than 0"
        else:
            size = this._console.width

        this.bgStyle:Style = background
        this.barStyle:Style = bar
        this.cursorStyle:Style = cursor

        this._size:int = size

        this._current:float = 0
        this.total:float = 100 if total is None else total

        this.label:str = ""
        this.alignment:JustifyMethod = 'left'

    @property
    def finished(this) -> bool:
        return this._current == this.total

    @property
    def width(this) -> int:
        return this._size

    @property
    def get_percentage(this) -> float:
        return this._current / this.total

    def advance(this,delta:float, label:Optional[str] = None) -> None:
        this._current += delta

        if this._current > this.total:
            this._current = this.total

        this.label = label if label is not None else ""

    def update(this,value:float, label:Optional[str] = None) -> None:
        this._current = value if value < this.total else this.total

        this.label = label if label is not None else ""

    def render(this) -> RenderableType:
        actual_text = this.label.replace("%%",f"{int(this.get_percentage*100)}%")
        text = Text(text=actual_text, no_wrap=True,style=this.bgStyle,end="")


        length = len(actual_text)

        if length < this.width:
            pad = this.width - length

            match this.alignment:
                case "center":
                    pad_left = int(pad/2)
                    pad_right = pad - pad_left

                    text.pad_left(pad_left)
                    text.pad_right(pad_right)
                case "right":
                    text.pad_left(pad)

        text.truncate(this.width, overflow="ellipsis", pad=True)

        completed_characters = this.get_percentage * this.width
        n = int(completed_characters)
        next_char_perc = completed_characters - n

        text.stylize(this.barStyle,end=n)

        if (completed_characters < this.width) and (next_char_perc>=0.5):
            text.stylize(this.cursorStyle,n,n+1)

        return text

    def print(this):
        this.console.print(this.render(),end="")

if __name__ == "__main__":
    import time
    pb = HighlightedProgressBar(100,50)
    pb.alignment="center"

    pb.console.show_cursor(False)

    for i in range(0,pb.total):
        pb.advance(1,"Prova [%%]")
        pb.print()
        print("\r",end="")
        time.sleep(1)

    pb.console.show_cursor(True)
