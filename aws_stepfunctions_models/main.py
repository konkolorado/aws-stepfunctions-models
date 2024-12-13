from __future__ import annotations

import datetime
import enum
import typing as t

import typing_extensions as te
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveFloat,
    PositiveInt,
    StrictBool,
    ValidationError,
    field_validator,
    model_validator,
)

from aws_stepfunctions_models.base_models import CollectibleTransitions, NextOrEndState
from aws_stepfunctions_models.utils import (
    enforce_datetime_format,
    enforce_exclusive_fields,
    enforce_jsonpath,
    enforce_min_items,
)

StepFunctionValidationError = ValidationError


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
    ErrorEquals: t.List[str] = Field(..., min_length=1)
    Next: str

    model_config = ConfigDict(extra="forbid")

    @property
    def transitions(self) -> list[str]:
        return [self.Next]


class Retriers(BaseModel):
    ErrorEquals: t.List[str] = Field(..., min_length=1)
    IntervalSeconds: t.Optional[PositiveInt] = None
    MaxAttempts: t.Optional[PositiveInt] = None
    BackoffRate: t.Optional[PositiveFloat] = None


class DataTestExpression(BaseModel):
    Variable: str
    StringEquals: t.Optional[str] = None
    StringEqualsPath: t.Optional[str] = None
    StringLessThan: t.Optional[str] = None
    StringLessThanPath: t.Optional[str] = None
    StringGreaterThan: t.Optional[str] = None
    StringGreaterThanPath: t.Optional[str] = None
    StringLessThanEquals: t.Optional[str] = None
    StringLessThanEqualsPath: t.Optional[str] = None
    StringGreaterThanEquals: t.Optional[str] = None
    StringGreaterThanEqualsPath: t.Optional[str] = None
    StringMatches: t.Optional[str] = None
    NumericEquals: t.Optional[str] = None
    NumericEqualsPath: t.Optional[str] = None
    NumericLessThan: t.Optional[str] = None
    NumericLessThanPath: t.Optional[str] = None
    NumericGreaterThan: t.Optional[str] = None
    NumericGreaterThanPath: t.Optional[str] = None
    NumericLessThanEquals: t.Optional[str] = None
    NumericLessThanEqualsPath: t.Optional[str] = None
    NumericGreaterThanEquals: t.Optional[str] = None
    NumericGreaterThanEqualsPath: t.Optional[str] = None
    BooleanEquals: t.Optional[StrictBool] = None
    BooleanEqualsPath: t.Optional[str] = None
    TimestampEquals: t.Optional[str] = None
    TimestampEqualsPath: t.Optional[str] = None
    TimestampLessThan: t.Optional[str] = None
    TimestampLessThanPath: t.Optional[str] = None
    TimestampGreaterThan: t.Optional[str] = None
    TimestampGreaterThanPath: t.Optional[str] = None
    TimestampLessThanEquals: t.Optional[str] = None
    TimestampLessThanEqualsPath: t.Optional[str] = None
    TimestampGreaterThanEquals: t.Optional[str] = None
    TimestampGreaterThanEqualsPath: t.Optional[str] = None
    IsNull: t.Optional[StrictBool] = None
    IsPresent: t.Optional[StrictBool] = None
    IsNumeric: t.Optional[StrictBool] = None
    IsString: t.Optional[StrictBool] = None
    IsBoolean: t.Optional[StrictBool] = None
    IsTimestamp: t.Optional[StrictBool] = None

    model_config = ConfigDict(extra="forbid")
    _enforce_jsonpath = field_validator(
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
    )(enforce_jsonpath)
    _enforce_datetime_format = field_validator(
        "TimestampEquals",
        "TimestampLessThan",
        "TimestampGreaterThan",
        "TimestampLessThanEquals",
        "TimestampGreaterThanEquals",
    )(enforce_datetime_format)

    @model_validator(mode="after")
    def validate_only_one_rule_is_set(self):
        fields = [k for k in self.model_fields_set if k not in {"Variable", "Next"}]
        enforce_exclusive_fields(self, fields)
        return self


class BooleanExpression(BaseModel):
    And: t.Optional[t.List[t.Union[DataTestExpression, BooleanExpression]]] = None
    Or: t.Optional[t.List[t.Union[DataTestExpression, BooleanExpression]]] = None
    Not: t.Optional[t.Union[DataTestExpression, BooleanExpression]] = None

    model_config = ConfigDict(extra="forbid")
    _enforce_min_items_if_set = field_validator("And", "Or")(enforce_min_items)

    @model_validator(mode="after")
    def validate_only_one_boolean_choice_rule_set(self):
        enforce_exclusive_fields(self, ["And", "Or", "Not"])
        return self


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
    ] = Field(..., min_length=1)
    Default: t.Optional[str] = None
    model_config = ConfigDict(extra="forbid")

    _enforce_jsonpath = field_validator("InputPath", "OutputPath")(enforce_jsonpath)

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
    Retry: t.Optional[t.List[Retriers]] = Field(None, min_length=1)
    Catch: t.Optional[t.List[Catchers]] = Field(None, min_length=1)

    model_config = ConfigDict(extra="forbid")
    _enforce_jsonpath = field_validator(
        "InputPath",
        "OutputPath",
        "Parameters",
        "ResultPath",
        "ResultSelector",
        "TimeoutSecondsPath",
        "HeartbeatSecondsPath",
    )(enforce_jsonpath)

    @model_validator(mode="after")
    def validate_mutually_exclusive_fields(self):
        enforce_exclusive_fields(self, ["InputPath", "Parameters"], field_required=True)
        enforce_exclusive_fields(
            self, ["TimeoutSeconds", "TimeoutSecondsPath"], field_required=False
        )
        enforce_exclusive_fields(
            self, ["HeartbeatSeconds", "HeartbeatSecondsPath"], field_required=False
        )
        return self

    @model_validator(mode="after")
    def validate_heartbeat_seconds_le_timeout_seconds(self):
        heartbeat_s = self.HeartbeatSeconds
        timeout_s = self.TimeoutSeconds
        if (
            heartbeat_s is not None
            and timeout_s is not None
            and heartbeat_s > timeout_s
        ):
            raise ValueError(
                f"HeartbeatSeconds ({heartbeat_s}) cannot be greater than TimeoutSeconds ({timeout_s})"
            )
        return self

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

    model_config = ConfigDict(extra="forbid")

    @property
    def transitions(self) -> list[str]:
        return []


class SucceedState(BaseModel, CollectibleTransitions):
    Type: te.Literal[StateType.Succeed]
    Comment: t.Optional[str] = None
    InputPath: t.Optional[str] = None
    OutputPath: t.Optional[str] = None

    model_config = ConfigDict(extra="forbid")

    _enforce_jsonpath = field_validator("InputPath", "OutputPath")(enforce_jsonpath)

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

    model_config = ConfigDict(extra="forbid")

    _enforce_jsonpath = field_validator(
        "InputPath",
        "OutputPath",
        "ResultPath",
        "Result",
        "Parameters",
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

    model_config = ConfigDict(extra="forbid")
    _enforce_jsonpath = field_validator(
        "InputPath", "OutputPath", "SecondsPath", "TimestampPath"
    )(enforce_jsonpath)
    _enforce_datetime_format = field_validator("Timestamp")(enforce_datetime_format)

    @model_validator(mode="after")
    def validate_mutually_exclusive_fields(self):
        enforce_exclusive_fields(
            self, ["Seconds", "Timestamp", "SecondsPath", "TimestampPath"]
        )
        return self

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
    Retry: t.Optional[t.List[Retriers]] = Field(None, min_length=1)
    Catch: t.Optional[t.List[Catchers]] = Field(None, min_length=1)

    model_config = ConfigDict(extra="forbid")

    _enforce_min_items_if_set = field_validator(
        "Branches",
    )(enforce_min_items)
    _enforce_jsonpath = field_validator(
        "InputPath",
        "OutputPath",
        "ResultPath",
        "ResultSelector",
        "Parameters",
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
    Retry: t.Optional[t.List[Retriers]] = Field(None, min_length=1)
    Catch: t.Optional[t.List[Catchers]] = Field(None, min_length=1)

    model_config = ConfigDict(extra="forbid")

    _enforce_jsonpath = field_validator(
        "InputPath",
        "OutputPath",
        "ResultPath",
        "ResultSelector",
        "Parameters",
        "ItemsPath",
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

    model_config = ConfigDict(extra="forbid")

    def dict(self):
        return super().model_dump(exclude_unset=True)

    def json(self):
        return super().model_dump_json(exclude_unset=True)

    @field_validator("States")
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

    @model_validator(mode="after")
    def validate_transition_states_exist(self):
        states = self.States
        defined_states = set(states.keys())
        referenced_states = {self.StartAt}

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
        return self
