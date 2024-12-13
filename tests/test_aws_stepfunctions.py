import pytest

from aws_stepfunctions_models import StepFunctionDefinition, StepFunctionValidationError

from .common import StateTestCase

valid_test_cases = [
    StateTestCase(
        "Contains expected top level fields",
        {
            "StartAt": "SimplePass",
            "Comment": "This is allowed",
            "States": {
                "SimplePass": {
                    "Type": "Pass",
                    "Parameters": {},
                    "End": True,
                },
            },
        },
    ),
]

invalid_test_cases = [
    StateTestCase(
        "Unexpected top level fields",
        {
            "StartAt": "SimplePass",
            "Comment": "Comment",
            "SomeCustomField": "This is disallowed",
            "States": {
                "SimplePass": {
                    "Type": "Pass",
                    "Parameters": {},
                    "End": True,
                },
            },
        },
    ),
    StateTestCase(
        "Missing StartAt",
        {
            "Comment": "Comment",
            "States": {
                "SimplePass": {
                    "Type": "Pass",
                    "Parameters": {},
                    "End": True,
                },
            },
        },
    ),
    StateTestCase("Missing States", {"StartAt": "SimplePass", "Comment": "Comment"}),
    StateTestCase("Missing StartAt and States", {"Comment": "Comment"}),
    StateTestCase("Empty definition", {}),
]


@pytest.mark.parametrize("flow_def", valid_test_cases)
def test_valid_flows_pass_validation(flow_def: StateTestCase):
    flow_model = StepFunctionDefinition(**flow_def.definition)
    flow_def_out = flow_model.model_dump(exclude_unset=True)
    assert flow_def_out == flow_def.definition


@pytest.mark.parametrize("flow_def", invalid_test_cases)
def test_invalid_flows_fail_validation(flow_def: StateTestCase):
    with pytest.raises(StepFunctionValidationError) as ve:
        StepFunctionDefinition(**flow_def.definition)

    assert ve.type is StepFunctionValidationError
