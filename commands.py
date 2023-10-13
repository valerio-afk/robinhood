import subprocess
from dataclasses import dataclass
from filesystem import is_windows
@dataclass
class RunCommand:
    command:str
    def __call__(this):
        subprocess.run(this.command,shell=True)

    def __str__(this):
        return this.command

    def __repr__(this):
        return str(this)


class  NoCommand(RunCommand):

    def __init__(this):
        super().__init__("")

    def __call__(this):
        pass

    def __str__(this):
        return "NOTHING"
class ShutdownCommand(RunCommand):

    def __init__(this):
        cmd = "shutdown /s /t 0" if is_windows() else "systemctl poweroff"
        super().__init__(cmd)

    def __str__(this):
        return "SHUTDOWN"

class SupendCommand(RunCommand):

    def __init__(this):
        cmd = "shutdown /d /t 0" if is_windows() else "systemctl suspend"
        super().__init__(cmd)

    def __str__(this):
        return "SUSPEND"


def make_command(cmd:str) -> RunCommand:
    match cmd:
        case "SHUTDOWN":
            return ShutdownCommand()
        case "SUSPEND":
            return SupendCommand()
        case "NOTHING" | None:
            return NoCommand()
        case _:
            return RunCommand(cmd)
