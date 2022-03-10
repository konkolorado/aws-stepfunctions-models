import abc
import typing as t

from pydantic import BaseModel, StrictBool, root_validator


class CollectibleTransitions(abc.ABC):
    @property
    @abc.abstractmethod
    def transitions(self) -> list[str]:
        ...


class NextOrEndState(BaseModel):
    Next: t.Optional[str] = None
    End: StrictBool = False

    @root_validator
    def validate_mutually_exclusive_fields(cls, values):
        if values.get("Next") and values.get("End"):
            raise ValueError('"Next" and "End" are mutually exclusive')
        if values.get("Next") is None and not values.get("End", False):
            raise ValueError('Either "Next" or "End" are required')
        return values
