import pytest

from aws_stepfunctions_models import StepFunctionValidationError, WaitState

from .common import StateTestCase

valid_state_cases = [
    StateTestCase(
        name="Valid WaitState",
        definition={
            "Type": "Wait",
            "Comment": "Simply Waiting",
            "Seconds": 1,
            "End": True,
        },
    )
]

invalid_state_cases = [
    StateTestCase(
        name="Multiple Wait parameters set",
        definition={
            "Type": "Wait",
            "Comment": "Simply Waiting",
            "Seconds": 1,
            "SecondsPath": "$.some_path",
            "End": True,
        },
    ),
    StateTestCase(
        name="No Wait parameters set",
        definition={
            "Type": "Wait",
            "Comment": "Simply Waiting",
            "End": True,
        },
    ),
    StateTestCase(
        name="Unknown fields are defined",
        definition={
            "Type": "Wait",
            "Comment": "Simply Waiting",
            "Seconds": 1,
            "ThisFieldIsUnexpected": True,
            "End": True,
        },
    ),
    StateTestCase(
        name="Input path is not JSONPath",
        definition={
            "Type": "Wait",
            "Comment": "Simply Waiting",
            "Seconds": 2,
            "InputPath": "not_json_path",
            "End": True,
        },
    ),
    StateTestCase(
        name="Output path is not JSONPath",
        definition={
            "Type": "Wait",
            "Comment": "Simply Waiting",
            "Seconds": 1,
            "OutputPath": "not_json_path",
            "End": True,
        },
    ),
    StateTestCase(
        name="Seconds path is not JSONPath",
        definition={
            "Type": "Wait",
            "Comment": "Simply Waiting",
            "SecondsPath": "not_json_path",
            "End": True,
        },
    ),
    StateTestCase(
        name="Timestamp path is not JSONPath",
        definition={
            "Type": "Wait",
            "Comment": "Simply Waiting",
            "TimestampPath": "not_json_path",
            "End": True,
        },
    ),
]


@pytest.mark.parametrize("test_case", valid_state_cases, ids=str)
def test_valid_wait_states_pass_validation(test_case: StateTestCase):
    state_model = WaitState(**test_case.definition)
    state_def_out = state_model.model_dump(exclude_unset=True)
    assert state_def_out == test_case.definition


@pytest.mark.parametrize("test_case", invalid_state_cases, ids=str)
def test_invalid_wait_states_fail_validation(test_case: StateTestCase):
    with pytest.raises(StepFunctionValidationError) as ve:
        WaitState(**test_case.definition)

    assert ve.type is StepFunctionValidationError
