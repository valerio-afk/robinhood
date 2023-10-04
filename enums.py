from enum import Enum
class SyncMode(Enum):
    UPDATE:int = 0
    MIRROR:int = 1
    SYNC:int = 2
    DEDUPE:int = 3

class ActionType(Enum):
    NOTHING:int=0
    MKDIR:int=1
    COPY:int=2
    UPDATE:int=3
    DELETE:int=4
    UNKNOWN:int=5


class ActionDirection(Enum):
    SRC2DST:str = '>'
    DST2SRC:str = '<'

    def __str__(this) ->str:
        return this.value


class SyncStatus(Enum):
    NOT_STARTED : int = 0
    IN_PROGRESS : int = 1
    SUCCESS     : int = 2
    FAILED      : int = 3
    INTERRUPTED : int = 4