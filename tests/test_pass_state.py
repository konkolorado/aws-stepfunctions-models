import pytest

from aws_stepfunctions_models import PassState, StepFunctionValidationError

from .common import StateTestCase

valid_test_cases = [
    StateTestCase(
        name="Simple Pass State",
        definition={
            "Type": "Pass",
            "End": True,
        },
    ),
]

invalid_test_cases = [
    StateTestCase(
        name="Input path is not JSONPath",
        definition={
            "Type": "Pass",
            "InputPath": "not_a_json_path",
            "End": True,
        },
    ),
    StateTestCase(
        name="Result is not JSONPath",
        definition={
            "Type": "Pass",
            "Result": "not_a_json_path",
            "End": True,
        },
    ),
    StateTestCase(
        name="Parameter key is JSONPath but values are not JSONPath",
        definition={
            "Type": "Pass",
            "Parameters": {"json_path_key.$": "not_a_json_path"},
            "End": True,
        },
    ),
    StateTestCase(
        name="Nested Parameter key is JSONPath but values are not JSONPath",
        definition={
            "Type": "Pass",
            "Parameters": {"nested_json": {"json_path_key.$": "not_a_json_path"}},
            "End": True,
        },
    ),
    StateTestCase(
        name="Unknown fields set",
        definition={
            "Type": "Pass",
            "Parameters": {},
            "NonExistantStateField": True,
            "End": True,
        },
    ),
]


@pytest.mark.parametrize("test_case", valid_test_cases, ids=str)
def test_valid_pass_states_pass_validation(test_case: StateTestCase):
    state_model = PassState(**test_case.definition)
    state_def_out = state_model.model_dump(exclude_unset=True)
    assert state_def_out == test_case.definition


@pytest.mark.parametrize("test_case", invalid_test_cases, ids=str)
def test_invalid_pass_states_fail_validation(test_case: StateTestCase):
    with pytest.raises(StepFunctionValidationError) as ve:
        PassState(**test_case.definition)

    assert ve.type is StepFunctionValidationError
