from abc import ABC, abstractmethod
from typing import Any, Union

class SyncEvent:

    def __init__(this, value: Any = None, *, processed: Union[int | None] = None, total: Union[int | None] = None):
        this.value = value
        this.processed = processed
        this.total = total

class RobinHoodBackend(ABC):
    '''This class manages the communication between the backend and frontend
    It contains a series of events that are triggered when they occur
    before/during/after comparing/synching two directories
    '''
    @abstractmethod
    def before_comparing(this, event: SyncEvent) -> None:
        ...

    @abstractmethod
    def on_comparing(this, event: SyncEvent) -> None:
        ...

    @abstractmethod
    def after_comparing(this, event: SyncEvent) -> None:
        ...

    @abstractmethod
    def before_synching(this, event: SyncEvent) -> None:
        ...

    @abstractmethod
    def on_synching(this, event:SyncEvent) -> None:
        ...

    @abstractmethod
    def after_synching(this, event: SyncEvent) -> None:
        ...

