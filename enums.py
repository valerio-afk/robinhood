from enum import Enum
class SyncMode(Enum):
    UPDATE:int = 0
    MIRROR:int = 1
    SYNC:int = 2
    DEDUPE:int = 3

class ActionType(Enum):
    NOTHING:int=0
    # MKDIR:int=1
    COPY:int=2
    UPDATE:int=3
    DELETE:int=4
    MOVE:int=5
    UNKNOWN:int=6

    @property
    def supports_both(this):
        return this == ActionType.DELETE


class ActionDirection(Enum):
    SRC2DST:str = '>'
    DST2SRC:str = '<'
    BOTH:str = '<>'

    def __str__(this) ->str:
        return this.value






class SyncStatus(Enum):
    NOT_STARTED : int = 0
    IN_PROGRESS : int = 1
    SUCCESS     : int = 2
    FAILED      : int = 3