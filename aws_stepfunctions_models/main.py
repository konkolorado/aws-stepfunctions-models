from __future__ import annotations

import datetime
import enum
import typing as t

import typing_extensions as te
from pydantic import BaseModel, Extra, Field, PositiveFloat, PositiveInt, StrictBool
from pydantic import ValidationError as StepFunctionValidationError
from pydantic import root_validator, validator

from aws_stepfunctions_models.base_models import CollectibleTransitions, NextOrEndState
from aws_stepfunctions_models.utils import (
    enforce_datetime_format,
    enforce_exclusive_fields,
    enforce_jsonpath,
    enforce_min_items,
)


class StateType(str, enum.Enum):
    Pass = "Pass"
    Task = "Task"
    Wait = "Wait"
    Choice = "Choice"
    Fail = "Fail"
    Parallel = "Parallel"
    Map = "Map"
    Succeed = "Succeed"


class Catchers(BaseModel, CollectibleTransitions):
    ErrorEquals: t.List[str] = Field(..., min_items=1)
    Next: str

    class Config:
        extra = Extra.forbid

    _enforce_jsonpath = validator("ResultPath", allow_reuse=True)(enforce_jsonpath)

    @property
    def transitions(self) -> list[str]:
        return [self.Next]


class Retriers(BaseModel):
    ErrorEquals: t.List[str] = Field(..., min_items=1)
    IntervalSeconds: t.Optional[PositiveInt] = None
    MaxAttempts: t.Optional[PositiveInt] = None
    BackoffRate: t.Optional[PositiveFloat] = None


class DataTestExpression(BaseModel):
    Variable: str
    StringEquals: t.Optional[str]
    StringEqualsPath: t.Optional[str]
    StringLessThan: t.Optional[str]
    StringLessThanPath: t.Optional[str]
    StringGreaterThan: t.Optional[str]
    StringGreaterThanPath: t.Optional[str]
    StringLessThanEquals: t.Optional[str]
    StringLessThanEqualsPath: t.Optional[str]
    StringGreaterThanEquals: t.Optional[str]
    StringGreaterThanEqualsPath: t.Optional[str]
    StringMatches: t.Optional[str]
    NumericEquals: t.Optional[str]
    NumericEqualsPath: t.Optional[str]
    NumericLessThan: t.Optional[str]
    NumericLessThanPath: t.Optional[str]
    NumericGreaterThan: t.Optional[str]
    NumericGreaterThanPath: t.Optional[str]
    NumericLessThanEquals: t.Optional[str]
    NumericLessThanEqualsPath: t.Optional[str]
    NumericGreaterThanEquals: t.Optional[str]
    NumericGreaterThanEqualsPath: t.Optional[str]
    BooleanEquals: t.Optional[StrictBool]
    BooleanEqualsPath: t.Optional[str]
    TimestampEquals: t.Optional[str]
    TimestampEqualsPath: t.Optional[str]
    TimestampLessThan: t.Optional[str]
    TimestampLessThanPath: t.Optional[str]
    TimestampGreaterThan: t.Optional[str]
    TimestampGreaterThanPath: t.Optional[str]
    TimestampLessThanEquals: t.Optional[str]
    TimestampLessThanEqualsPath: t.Optional[str]
    TimestampGreaterThanEquals: t.Optional[str]
    TimestampGreaterThanEqualsPath: t.Optional[str]
    IsNull: t.Optional[StrictBool]
    IsPresent: t.Optional[StrictBool]
    IsNumeric: t.Optional[StrictBool]
    IsString: t.Optional[StrictBool]
    IsBoolean: t.Optional[StrictBool]
    IsTimestamp: t.Optional[StrictBool]

    class Config:
        extra = Extra.forbid

    _enforce_jsonpath = validator(
        "Variable",
        "StringEqualsPath",
        "StringLessThanPath",
        "StringGreaterThanPath",
        "StringLessThanEqualsPath",
        "StringGreaterThanEqualsPath",
        "NumericEqualsPath",
        "NumericLessThanPath",
        "NumericGreaterThanPath",
        "NumericLessThanEqualsPath",
        "NumericGreaterThanEqualsPath",
        "BooleanEqualsPath",
        "TimestampEqualsPath",
        "TimestampLessThanPath",
        "TimestampGreaterThanPath",
        "TimestampLessThanEqualsPath",
        "TimestampGreaterThanEqualsPath",
        allow_reuse=True,
    )(enforce_jsonpath)

    _enforce_datetime_format = validator(
        "TimestampEquals",
        "TimestampLessThan",
        "TimestampGreaterThan",
        "TimestampLessThanEquals",
        "TimestampGreaterThanEquals",
        allow_reuse=True,
    )(enforce_datetime_format)

    @root_validator(skip_on_failure=True)
    def validate_only_one_rule_is_set(cls, values):
        fields = [k for k in values.keys() if k not in {"Variable", "Next"}]
        enforce_exclusive_fields(values, fields)
        return values


class BooleanExpression(BaseModel):
    And: t.Optional[t.List[t.Union[DataTestExpression, BooleanExpression]]] = None
    Or: t.Optional[t.List[t.Union[DataTestExpression, BooleanExpression]]] = None
    Not: t.Optional[t.Union[DataTestExpression, BooleanExpression]] = None

    class Config:
        extra = Extra.forbid

    _enforce_min_items_if_set = validator("And", "Or", allow_reuse=True)(
        enforce_min_items
    )

    @root_validator(skip_on_failure=True)
    def validate_only_one_boolean_choice_rule_set(cls, values):
        enforce_exclusive_fields(values, ["And", "Or", "Not"])
        return values


class DataTestExpressionWithTransition(DataTestExpression):
    Next: str


class BooleanExpressionWithTransition(BooleanExpression):
    Next: str


class ChoiceState(BaseModel, CollectibleTransitions):
    Type: te.Literal[StateType.Choice]
    Comment: t.Optional[str] = None
    InputPath: t.Optional[str] = None
    OutputPath: t.Optional[str] = None
    Choices: t.List[
        t.Union[BooleanExpressionWithTransition, DataTestExpressionWithTransition]
    ] = Field(..., min_items=1)
    Default: t.Optional[str]

    class Config:
        extra = Extra.forbid

    _enforce_jsonpath = validator("InputPath", "OutputPath", allow_reuse=True)(
        enforce_jsonpath
    )

    @property
    def transitions(self) -> list[str]:
        transitions = []
        if self.Default:
            transitions.append(self.Default)
        for choice in self.Choices:
            transitions.append(choice.Next)
        return transitions


class TaskState(NextOrEndState, CollectibleTransitions):
    Type: te.Literal[StateType.Task]
    Comment: t.Optional[str] = None
    InputPath: t.Optional[str] = None
    OutputPath: t.Optional[str] = None
    ResultPath: t.Optional[str] = None
    ResultSelector: t.Optional[t.Dict[str, t.Any]] = None
    Parameters: t.Optional[t.Dict[str, t.Any]] = None
    Resource: str
    TimeoutSeconds: t.Optional[PositiveInt] = None
    TimeoutSecondsPath: t.Optional[str] = None
    HeartbeatSeconds: t.Optional[PositiveInt] = None
    HeartbeatSecondsPath: t.Optional[str] = None
    Retry: t.Optional[t.List[Retriers]] = Field(None, min_items=1)
    Catch: t.Optional[t.List[Catchers]] = Field(None, min_items=1)

    class Config:
        extra = Extra.forbid

    _enforce_jsonpath = validator(
        "InputPath",
        "OutputPath",
        "Parameters",
        "ResultPath",
        "ResultSelector",
        "TimeoutSecondsPath",
        "HeartbeatSecondsPath",
        allow_reuse=True,
    )(enforce_jsonpath)

    @root_validator
    def validate_mutually_exclusive_fields(cls, values):
        exclusive_fields_list = [
            ["InputPath", "Parameters"],
            ["TimeoutSeconds", "TimeoutSecondsPath"],
            ["HeartbeatSeconds", "HeartbeatSecondsPath"],
        ]
        for exlusive_fields in exclusive_fields_list:
            enforce_exclusive_fields(values, exlusive_fields)
        return values

    @root_validator
    def validate_heartbeat_seconds_le_timeout_seconds(cls, values):
        heartbeat_s = values.get("HeartbeatSeconds")
        timeout_s = values.get("TimeoutSeconds")
        if (
            heartbeat_s is not None
            and timeout_s is not None
            and heartbeat_s > timeout_s
        ):
            raise ValueError(
                f"HeartbeatSeconds ({heartbeat_s}) cannot be greater than TimeoutSeconds ({timeout_s})"
            )
        return values

    @property
    def transitions(self) -> list[str]:
        transitions = []
        if self.Next:
            transitions.append(self.Next)
        if self.Catch:
            for catcher in self.Catch:
                transitions.extend(catcher.transitions)
        return transitions


class FailState(BaseModel, CollectibleTransitions):
    Type: te.Literal[StateType.Fail]
    Comment: t.Optional[str] = None
    Cause: t.Optional[str] = None
    Error: t.Optional[str] = None

    class Config:
        extra = Extra.forbid

    @property
    def transitions(self) -> list[str]:
        return []


class SucceedState(BaseModel, CollectibleTransitions):
    Type: te.Literal[StateType.Succeed]
    Comment: t.Optional[str] = None
    InputPath: t.Optional[str] = None
    OutputPath: t.Optional[str] = None

    class Config:
        extra = Extra.forbid

    _enforce_jsonpath = validator("InputPath", "OutputPath", allow_reuse=True)(
        enforce_jsonpath
    )

    @property
    def transitions(self) -> list[str]:
        return []


class PassState(NextOrEndState, CollectibleTransitions):
    Type: te.Literal[StateType.Pass]
    Comment: t.Optional[str] = None
    InputPath: t.Optional[str] = None
    OutputPath: t.Optional[str] = None
    Result: t.Optional[dict] = None
    ResultPath: t.Optional[str] = None
    Parameters: t.Optional[t.Dict[str, t.Any]] = None

    class Config:
        extra = Extra.forbid

    _enforce_jsonpath = validator(
        "InputPath",
        "OutputPath",
        "ResultPath",
        "Result",
        "Parameters",
        allow_reuse=True,
    )(enforce_jsonpath)

    @property
    def transitions(self) -> list[str]:
        return [self.Next] if self.Next else []


class WaitState(NextOrEndState, CollectibleTransitions):
    Type: te.Literal[StateType.Wait]
    Comment: t.Optional[str] = None
    InputPath: t.Optional[str] = None
    OutputPath: t.Optional[str] = None
    Seconds: t.Optional[int] = None
    Timestamp: t.Optional[datetime.datetime] = None
    SecondsPath: t.Optional[str] = None
    TimestampPath: t.Optional[str] = None

    class Config:
        extra = Extra.forbid

    _enforce_jsonpath = validator(
        "InputPath", "OutputPath", "SecondsPath", "TimestampPath", allow_reuse=True
    )(enforce_jsonpath)

    _enforce_datetime_format = validator("Timestamp", allow_reuse=True)(
        enforce_datetime_format
    )

    @root_validator(skip_on_failure=True)
    def validate_mutually_exclusive_fields(cls, values):
        enforce_exclusive_fields(
            values, ["Seconds", "Timestamp", "SecondsPath", "TimestampPath"]
        )
        return values

    @property
    def transitions(self) -> list[str]:
        return [self.Next] if self.Next else []


class ParallelState(NextOrEndState, CollectibleTransitions):
    Type: te.Literal[StateType.Parallel]
    Comment: t.Optional[str] = None
    InputPath: t.Optional[str] = None
    OutputPath: t.Optional[str] = None
    ResultPath: t.Optional[str] = None
    ResultSelector: t.Optional[t.Dict[str, t.Any]] = None
    Parameters: t.Optional[t.Dict[str, t.Any]] = None
    Branches: t.List[StepFunctionDefinition]
    Retry: t.Optional[t.List[Retriers]] = Field(None, min_items=1)
    Catch: t.Optional[t.List[Catchers]] = Field(None, min_items=1)

    class Config:
        extra = Extra.forbid

    _enforce_min_items_if_set = validator("Branches", allow_reuse=True)(
        enforce_min_items
    )
    _enforce_jsonpath = validator(
        "InputPath",
        "OutputPath",
        "ResultPath",
        "ResultSelector",
        "Parameters",
        allow_reuse=True,
    )(enforce_jsonpath)

    @property
    def transitions(self) -> list[str]:
        transitions = []
        if self.Next:
            transitions.append(self.Next)
        if self.Catch:
            for catcher in self.Catch:
                transitions.extend(catcher.transitions)

        # No need to recurse in self.Branches since the StepFunctionDefinition
        # validation will do that itself
        return transitions


class MapState(NextOrEndState, CollectibleTransitions):
    Type: te.Literal[StateType.Map]
    Comment: t.Optional[str] = None
    InputPath: t.Optional[str] = None
    OutputPath: t.Optional[str] = None
    ResultPath: t.Optional[str] = None
    ResultSelector: t.Optional[t.Dict[str, t.Any]] = None
    Parameters: t.Optional[t.Dict[str, t.Any]] = None
    Iterator: StepFunctionDefinition
    ItemsPath: t.Optional[str] = None
    MaxConcurrency: t.Optional[PositiveInt] = None
    Retry: t.Optional[t.List[Retriers]] = Field(None, min_items=1)
    Catch: t.Optional[t.List[Catchers]] = Field(None, min_items=1)

    class Config:
        extra = Extra.forbid

    _enforce_jsonpath = validator(
        "InputPath",
        "OutputPath",
        "ResultPath",
        "ResultSelector",
        "Parameters",
        "ItemsPath",
        allow_reuse=True,
    )(enforce_jsonpath)

    @property
    def transitions(self) -> list[str]:
        transitions = []
        if self.Next:
            transitions.append(self.Next)
        if self.Catch:
            for catcher in self.Catch:
                transitions.extend(catcher.transitions)

        # No need to recurse in self.Iterator since the StepFunctionDefinition
        # validation will do that itself
        return transitions


StepFunctionState = te.Annotated[
    t.Union[
        PassState,
        WaitState,
        FailState,
        SucceedState,
        ChoiceState,
        TaskState,
        ParallelState,
        MapState,
    ],
    Field(discriminator="Type"),
]


class StepFunctionDefinition(BaseModel):
    StartAt: str
    States: t.Dict[str, StepFunctionState]
    Comment: t.Optional[str] = None

    class Config:
        extra = Extra.forbid

    def dict(self):
        return super().dict(exclude_unset=True)

    def json(self):
        return super().json(exclude_unset=True)

    @validator("States")
    def validate_states(cls, v):
        if len(v) < 1:
            raise ValueError('At least one "State" is required')

        invalid_state_names = [
            state_name
            for state_name in v.keys()
            if len(state_name) > 128 or len(state_name) == 0
        ]
        if invalid_state_names:
            raise ValueError(
                f"The following states have invalid names: {', '.join(invalid_state_names)}"
            )
        return v

    @root_validator(skip_on_failure=True)
    def validate_transition_states_exist(cls, values):
        states: dict[str, CollectibleTransitions] = values.get("States", {})
        defined_states = set(states.keys())
        referenced_states = {values["StartAt"]}

        for state in states.values():
            referenced_states.update(state.transitions)

        undefined_states = referenced_states - defined_states
        if undefined_states:
            raise ValueError(
                f'Flow definition contained a reference to unknown state(s): {", ".join(undefined_states)}'
            )
        unreferenced_states = defined_states - referenced_states
        if unreferenced_states:
            raise ValueError(
                f'Flow definition contained unreachable state(s): {", ".join(unreferenced_states)}'
            )
        return values


d = {"StartAt": "Test", "States": {"Test": {"Type": "Pass", "End": True}}}
d2 = {
    "StartAt": "SimplePass",
    "Comment": "This is allowed",
    "SomeCustomField": "This is disallowed",
    "MostCustomField": 2,
    "States": {
        "SimplePass": {
            "Type": "Pass",
            "Parameters": {},
            "End": True,
            "Next": "LMAO",
        },
    },
}

# print(StepFunctionDefinition(**d).json())
# print(StepFunctionDefinition(**d2).dict())
