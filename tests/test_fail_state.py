import pytest

from aws_stepfunctions_models import FailState, StepFunctionValidationError

from .common import StateTestCase

valid_test_cases = [
    StateTestCase(
        name="Simple Fail State",
        definition={
            "Type": "Fail",
            "Comment": "No info",
        },
    ),
    StateTestCase(
        name="Informative Fail State",
        definition={
            "Type": "Fail",
            "Comment": "No info",
            "Cause": "SomeCause",
            "Error": "SomeError",
        },
    ),
]

invalid_test_cases = [
    StateTestCase(
        name="Unknown fields are defined",
        definition={
            "Type": "Fail",
            "Comment": "No info",
            "Cause": "SomeCause",
            "Error": "SomeError",
            "SomeExtraField": "SomeValue",
        },
    ),
    StateTestCase(
        name="Disallowed transition is used",
        definition={
            "Type": "Fail",
            "Comment": "No info",
            "Next": "SomeState",
        },
    ),
]


@pytest.mark.parametrize("test_case", valid_test_cases, ids=str)
def test_valid_fail_states_pass_validation(test_case: StateTestCase):
    state_model = FailState(**test_case.definition)
    state_def_out = state_model.model_dump(exclude_unset=True)
    assert state_def_out == test_case.definition


@pytest.mark.parametrize("test_case", invalid_test_cases, ids=str)
def test_invalid_fail_states_fail_validation(test_case: StateTestCase):
    with pytest.raises(StepFunctionValidationError) as ve:
        FailState(**test_case.definition)

    assert ve.type is StepFunctionValidationError
