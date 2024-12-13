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
    ErrorEquals: list[str] = Field(..., min_length=1)
    Next: str

    model_config = ConfigDict(extra="forbid")

    @property
    def transitions(self) -> list[str]:
        return [self.Next]


class Retriers(BaseModel):
    ErrorEquals: list[str] = Field(..., min_length=1)
    IntervalSeconds: PositiveInt | None = None
    MaxAttempts: PositiveInt | None = None
    BackoffRate: PositiveFloat | None = None


class DataTestExpression(BaseModel):
    Variable: str
    StringEquals: str | None = None
    StringEqualsPath: str | None = None
    StringLessThan: str | None = None
    StringLessThanPath: str | None = None
    StringGreaterThan: str | None = None
    StringGreaterThanPath: str | None = None
    StringLessThanEquals: str | None = None
    StringLessThanEqualsPath: str | None = None
    StringGreaterThanEquals: str | None = None
    StringGreaterThanEqualsPath: str | None = None
    StringMatches: str | None = None
    NumericEquals: str | None = None
    NumericEqualsPath: str | None = None
    NumericLessThan: str | None = None
    NumericLessThanPath: str | None = None
    NumericGreaterThan: str | None = None
    NumericGreaterThanPath: str | None = None
    NumericLessThanEquals: str | None = None
    NumericLessThanEqualsPath: str | None = None
    NumericGreaterThanEquals: str | None = None
    NumericGreaterThanEqualsPath: str | None = None
    BooleanEquals: StrictBool | None = None
    BooleanEqualsPath: str | None = None
    TimestampEquals: str | None = None
    TimestampEqualsPath: str | None = None
    TimestampLessThan: str | None = None
    TimestampLessThanPath: str | None = None
    TimestampGreaterThan: str | None = None
    TimestampGreaterThanPath: str | None = None
    TimestampLessThanEquals: str | None = None
    TimestampLessThanEqualsPath: str | None = None
    TimestampGreaterThanEquals: str | None = None
    TimestampGreaterThanEqualsPath: str | None = None
    IsNull: StrictBool | None = None
    IsPresent: StrictBool | None = None
    IsNumeric: StrictBool | None = None
    IsString: StrictBool | None = None
    IsBoolean: StrictBool | None = None
    IsTimestamp: StrictBool | None = None

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
    And: list[DataTestExpression | BooleanExpression] | None = Field(None, min_length=1)
    Or: list[DataTestExpression | BooleanExpression] | None = Field(None, min_length=1)
    Not: DataTestExpression | BooleanExpression | None = None

    model_config = ConfigDict(extra="forbid")

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
    Comment: str | None = None
    InputPath: str | None = None
    OutputPath: str | None = None
    Choices: list[
        BooleanExpressionWithTransition | DataTestExpressionWithTransition
    ] = Field(..., min_length=1)
    Default: str | None = None
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
    Comment: str | None = None
    InputPath: str | None = None
    OutputPath: str | None = None
    ResultPath: str | None = None
    ResultSelector: dict | None = None
    Parameters: dict | None = None
    Resource: str
    TimeoutSeconds: PositiveInt | None = None
    TimeoutSecondsPath: str | None = None
    HeartbeatSeconds: PositiveInt | None = None
    HeartbeatSecondsPath: str | None = None
    Retry: list[Retriers] | None = Field(None, min_length=1)
    Catch: list[Catchers] | None = Field(None, min_length=1)

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
    Comment: str | None = None
    Cause: str | None = None
    Error: str | None = None

    model_config = ConfigDict(extra="forbid")

    @property
    def transitions(self) -> list[str]:
        return []


class SucceedState(BaseModel, CollectibleTransitions):
    Type: te.Literal[StateType.Succeed]
    Comment: str | None = None
    InputPath: str | None = None
    OutputPath: str | None = None

    model_config = ConfigDict(extra="forbid")

    _enforce_jsonpath = field_validator("InputPath", "OutputPath")(enforce_jsonpath)

    @property
    def transitions(self) -> list[str]:
        return []


class PassState(NextOrEndState, CollectibleTransitions):
    Type: te.Literal[StateType.Pass]
    Comment: str | None = None
    InputPath: str | None = None
    OutputPath: str | None = None
    Result: dict | None = None
    ResultPath: str | None = None
    Parameters: dict | None = None

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
    Comment: str | None = None
    InputPath: str | None = None
    OutputPath: str | None = None
    Seconds: int | None = None
    Timestamp: datetime.datetime | None = None
    SecondsPath: str | None = None
    TimestampPath: str | None = None

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
    Comment: str | None = None
    InputPath: str | None = None
    OutputPath: str | None = None
    ResultPath: str | None = None
    ResultSelector: dict | None = None
    Parameters: dict | None = None
    Branches: list[StepFunctionDefinition] = Field(..., min_length=1)
    Retry: list[Retriers] | None = Field(None, min_length=1)
    Catch: list[Catchers] | None = Field(None, min_length=1)

    model_config = ConfigDict(extra="forbid")
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
    Comment: str | None = None
    InputPath: str | None = None
    OutputPath: str | None = None
    ResultPath: str | None = None
    ResultSelector: dict | None = None
    Parameters: dict | None = None
    Iterator: StepFunctionDefinition
    ItemsPath: str | None = None
    MaxConcurrency: PositiveInt | None = None
    Retry: list[Retriers] | None = Field(None, min_length=1)
    Catch: list[Catchers] | None = Field(None, min_length=1)

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
    Comment: str | None = None

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
