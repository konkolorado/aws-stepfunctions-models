import abc

from pydantic import BaseModel, StrictBool, model_validator


class CollectibleTransitions(abc.ABC):
    @property
    @abc.abstractmethod
    def transitions(self) -> list[str]: ...


class NextOrEndState(BaseModel):
    Next: str | None = None
    End: StrictBool = False

    @model_validator(mode="after")
    def validate_mutually_exclusive_fields(self):
        if self.Next and self.End:
            raise ValueError('"Next" and "End" are mutually exclusive')
        if self.Next is None and not self.End:
            raise ValueError('Either "Next" or "End" are required')
        return self
