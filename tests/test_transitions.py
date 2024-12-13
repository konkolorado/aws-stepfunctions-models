import pytest

from aws_stepfunctions_models import StepFunctionDefinition, StepFunctionValidationError

from .common import StateTestCase

valid_test_cases = [
    StateTestCase(
        "Valid start state",
        {
            "StartAt": "SimplePass",
            "States": {
                "SimplePass": {
                    "Type": "Pass",
                    "End": True,
                }
            },
        },
    ),
    StateTestCase(
        "Catch refers to existing state",
        {
            "StartAt": "SimpleTask",
            "States": {
                "SimpleTask": {
                    "Type": "Task",
                    "Resource": "https://example.org/hello_world",
                    "Parameters": {},
                    "Catch": [
                        {
                            "ErrorEquals": ["SomeError"],
                            "Next": "HandlerState",
                        }
                    ],
                    "End": True,
                },
                "HandlerState": {
                    "Type": "Pass",
                    "End": True,
                },
            },
        },
    ),
    StateTestCase(
        "Choice default refers to existing state",
        {
            "StartAt": "ChoiceState",
            "States": {
                "ChoiceState": {
                    "Type": "Choice",
                    "Default": "FinalState",
                    "Choices": [
                        {
                            "Variable": "$.some_variable",
                            "BooleanEquals": False,
                            "Next": "FinalState",
                        }
                    ],
                },
                "FinalState": {
                    "Type": "Pass",
                    "End": True,
                },
            },
        },
    ),
    StateTestCase(
        "Choice rule refers to existing state",
        {
            "StartAt": "ChoiceState",
            "States": {
                "ChoiceState": {
                    "Type": "Choice",
                    "Default": "FinalState",
                    "Choices": [
                        {
                            "Variable": "$.some_variable",
                            "BooleanEquals": False,
                            "Next": "FinalState",
                        }
                    ],
                },
                "FinalState": {
                    "Type": "Pass",
                    "End": True,
                },
            },
        },
    ),
]

invalid_test_cases = [
    StateTestCase(
        "Non existant start state",
        {
            "StartAt": "DoesNotExistAsAState",
            "States": {
                "SimplePass": {
                    "Type": "Pass",
                    "End": True,
                },
            },
        },
    ),
    StateTestCase(
        "Non existant next state",
        {
            "StartAt": "SimplePass",
            "States": {
                "SimplePass": {
                    "Type": "Pass",
                    "Next": "SomeUndefinedState",
                },
                "SimplePass2": {
                    "Type": "Pass",
                    "End": True,
                },
            },
        },
    ),
    StateTestCase(
        "Flow defines an unreachable state",
        {
            "StartAt": "SimplePass",
            "States": {
                "SimplePass": {
                    "Type": "Pass",
                    "End": True,
                },
                "UnreachableState": {
                    "Type": "Pass",
                    "End": True,
                },
            },
        },
    ),
    StateTestCase(
        "State defines both next and end",
        {
            "StartAt": "SimplePass",
            "States": {
                "SimplePass": {
                    "Type": "Pass",
                    "Comment": "both_next_and_end_states_defined",
                    "Next": "SimplePass2",
                    "End": True,
                },
            },
        },
    ),
    StateTestCase(
        "Catch refers to non existant state",
        {
            "StartAt": "SimpleTask",
            "States": {
                "SimpleTask": {
                    "Type": "Task",
                    "Resource": "https://example.org/hello_world",
                    "Parameters": {},
                    "Catch": [
                        {
                            "ErrorEquals": ["SomeError"],
                            "Next": "NotAState",
                        }
                    ],
                    "End": True,
                },
            },
        },
    ),
    StateTestCase(
        "Choice default refers to non existant state",
        {
            "StartAt": "ChoiceState",
            "States": {
                "ChoiceState": {
                    "Type": "Choice",
                    "Default": "NotAState",
                    "Choices": [
                        {
                            "Variable": "$.some_variable",
                            "BooleanEquals": False,
                            "Next": "FinalState",
                        }
                    ],
                },
                "FinalState": {
                    "Type": "Pass",
                    "End": True,
                },
            },
        },
    ),
    StateTestCase(
        "Choice rule refers to non existant state",
        {
            "StartAt": "ChoiceState",
            "States": {
                "ChoiceState": {
                    "Type": "Choice",
                    "Default": "FinalState",
                    "Choices": [
                        {
                            "Variable": "$.some_variable",
                            "BooleanEquals": False,
                            "Next": "NotAState",
                        }
                    ],
                },
                "FinalState": {
                    "Type": "Pass",
                    "End": True,
                },
            },
        },
    ),
    StateTestCase(
        "Boolean exp choise state refers to non existant state",
        {
            "StartAt": "ChoiceState",
            "States": {
                "ChoiceState": {
                    "Type": "Choice",
                    "Comment": "No info",
                    "Choices": [
                        {
                            "Or": [
                                {
                                    "Variable": "$.SomePath.status",
                                    "StringEquals": "FAILED",
                                },
                                {
                                    "Variable": "$.SomePath.status",
                                    "StringEquals": "FAILED",
                                },
                            ],
                            "Next": "NonExistantState",
                        }
                    ],
                },
                "FinalState": {
                    "Type": "Pass",
                    "End": True,
                },
            },
        },
    ),
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
