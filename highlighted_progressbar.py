from typing import Optional
from rich.console import Console, RenderableType, JustifyMethod
from rich.text import Text
from rich.style import Style

BACKGROUND = Style(color="white", bgcolor="#333333")
BAR = Style(color="black", bgcolor="#0087d7")
CURSOR = Style(color="black", bgcolor="#00afff")

class HighlightedProgressBar:

    def __init__(this, total:Optional[float]=None,
                       size:Optional[int]=None,
                       background:Style=BACKGROUND,
                       bar:Style=BAR,
                       cursor:Style=CURSOR):
        '''
        Initialise a highlighted progress bar

        :param total: Total number of units the progress bar should count
        :param size: Size of the progress bar in characters
        :param background: background colour (unfilled)
        :param bar: Bar colour (filled background)
        :param cursor: Colour of the cursor (it shows when smaller increment)
        '''

        #make a new console from Rich library
        this.console = Console()

        #check if the size is not not and, if it's not, check if it's a positive number
        if size is not None:
            assert size>0 , "The size of the progress bar must be greater than 0"
        else:
            #if the size has not been specified, then, I get the width of the console
            size = this.console.width

        #sert the colours
        this.bgStyle:Style = background
        this.barStyle:Style = bar
        this.cursorStyle:Style = cursor

        #set the size
        this._size:int = size

        #intialise the current status at 0 and total units
        this._current:float = 0
        #if total is none, 100 would be default
        this.total:float = 100 if total is None else total

        # A label to show (if any) - That's the reason I am doing all of this
        this.label:str = ""
        # And where to align it
        this.alignment:JustifyMethod = 'left'

    @property
    def finished(this) -> bool:
        '''
        Indicates whether the progress bar is done
        :return:  TRUE if it's done, FALSE otherwise
        '''
        return this._current == this.total

    @property
    def width(this) -> int:
        """
        Returns the size of the progress bar
        :return: An int indicatng the size of the progress bar in characters
        """
        return this._size

    @property
    def get_percentage(this) -> float:
        '''
        Calculates the percentage of the progression
        :return: A float from 0 to 1
        '''
        return this._current / this.total

    def advance(this,delta:float, label:Optional[str] = None) -> None:
        '''
        Advance the progress bar by a small amount

        :param delta: The amout to advance the progress bar
        :param label: A label to show on the progress bar (if any)
        :return: None
        '''
        this._current += delta

        # it's always good to check if we are overshooting...
        if this._current > this.total:
            this._current = this.total

        this.label = label if label is not None else ""

    def update(this,value:float, label:Optional[str] = None) -> None:
        '''
        Differently thant advance, update sets the progress bar at a specific value

        :param value: The value to set the progress bar
        :param label:  A label to show on the progress bar (if any)
        :return: None
        '''

        # it's always good to check if we are overshooting...
        this._current = value if value < this.total else this.total

        this.label = label if label is not None else ""

    def render(this) -> RenderableType:
        '''
        This is where the magic happen. Return RenderableType (that can be a string of an object of type Text)
        showing the exact characters of our progress bar

        :return: A RenderableType representing the progress bar
        '''

        # The label can have a placeholder to show the percentage, which is to percentage signs %%
        actual_text = this.label.replace("%%",f"{int(this.get_percentage*100)}%")

        #Text class in the Rich library comes at handy, because it easily allows me to align text
        #and set background colours up to where I need them.
        text = Text(text=actual_text, no_wrap=True,end="", )

        #size property will indicate the whole size of the progress bar,
        #However the label cannot be of that size because I need two characters
        #to render something resembling a box.

        padding = 2
        length = len(actual_text)

        #I check the total length of the actual label
        #If it's less than the width
        if length < this.width:
            pad = this.width - length - padding

            # I make some padding if I need to center or align to the right the label
            # Left alignment comes automatically
            match this.alignment:
                case "center":
                    pad_left = int(pad/2)
                    pad_right = pad - pad_left

                    text.pad_left(pad_left)
                    text.pad_right(pad_right)
                case "right":
                    text.pad_left(pad)


        #in all the cases the text is longer than the size, then I truncate the text
        #and replace the last character with an ellipsis ...
        text.truncate(this.width - padding, overflow="ellipsis", pad=True)

        #at this point, I calculate the number of characters needed to be filled with the given
        #progress bar percentage
        completed_characters = this.get_percentage * (this.width-padding)

        #this number must be round
        n = int(completed_characters)
        #this extra variable calculates how much left is to fill the next character. You'll see what it does
        next_char_perc = completed_characters - n

        #now I've got all I need - let's make the progress bar

        #the first thing is that I apply the bar colour up to it's needed
        text.stylize(this.barStyle,end=n)

        if (completed_characters < (this.width-padding)) and (next_char_perc>=0.5):
            #I colour the next character in the progress bar with a different shade if a certain threshold is reached
            text.stylize(this.cursorStyle,n,n+1)

        #let's make the whole progress bar by prefixing and affixing pipe characters |
        progress_bar = Text(text="│")
        progress_bar.append_text(text)
        progress_bar.append_text(Text(text="│"))

        # Apply the background coloyur the whole progress bar. Don't worry, previous colours won't overwritten.
        progress_bar.style = this.bgStyle

        return progress_bar

    def print(this):
        #a shortcut to print, if needed
        this.console.print(this.render(),end="")

if __name__ == "__main__":
    import time
    pb = HighlightedProgressBar(100,50)
    pb.alignment="center"

    pb.console.show_cursor(False)

    for i in range(0,pb.total):
        pb.advance(1,"Computerphile [%%]")
        pb.print()
        print("\r",end="")
        time.sleep(1)

    pb.console.show_cursor(True)
