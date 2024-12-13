import pytest

from aws_stepfunctions_models import ChoiceState, StepFunctionValidationError

from .common import StateTestCase

valid_test_cases = [
    StateTestCase(
        name="Simple Choice State",
        definition={
            "Type": "Choice",
            "Comment": "No info",
            "Choices": [
                {
                    "Variable": "$.SomePath.status",
                    "StringEquals": "FAILED",
                    "Next": "ChoiceAState",
                },
                {
                    "Variable": "$.SomePath.status",
                    "StringEquals": "FAILED",
                    "Next": "DefaultState",
                },
            ],
            "Default": "DefaultState",
        },
    ),
    StateTestCase(
        name="Choice State using And",
        definition={
            "Type": "Choice",
            "Comment": "No info",
            "Choices": [
                {
                    "And": [
                        {
                            "Variable": "$.SomePath.status",
                            "StringEquals": "FAILED",
                        },
                        {
                            "Variable": "$.SomePath.status",
                            "StringEquals": "FAILED",
                        },
                    ],
                    "Next": "ChoiceAState",
                }
            ],
            "Default": "DefaultState",
        },
    ),
    StateTestCase(
        name="Choice State using Or",
        definition={
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
                    "Next": "ChoiceAState",
                }
            ],
            "Default": "DefaultState",
        },
    ),
    StateTestCase(
        name="Choice State using Not",
        definition={
            "Type": "Choice",
            "Comment": "No info",
            "Choices": [
                {
                    "Not": {
                        "Variable": "$.SomePath.status",
                        "StringEquals": "FAILED",
                    },
                    "Next": "ChoiceAState",
                }
            ],
            "Default": "DefaultState",
        },
    ),
    StateTestCase(
        name="Choice State without Default",
        definition={
            "Type": "Choice",
            "Comment": "No info",
            "Choices": [
                {
                    "Variable": "$.SomePath.status",
                    "StringEquals": "FAILED",
                    "Next": "ChoiceAState",
                },
                {
                    "Variable": "$.SomePath.status",
                    "StringEquals": "FAILED",
                    "Next": "DefaultState",
                },
            ],
        },
    ),
    StateTestCase(
        name="Choice State with Simple Nested Boolean",
        definition={
            "Type": "Choice",
            "Comment": "No info",
            "Choices": [
                {
                    "Not": {"Not": {"Variable": "$.key", "StringEquals": "value"}},
                    "Next": "ChoiceAState",
                }
            ],
            "Default": "DefaultState",
        },
    ),
    StateTestCase(
        name="Choice State with conjunctive nested boolean condition",
        definition={
            "Type": "Choice",
            "Comment": "No info",
            "Choices": [
                {
                    "Not": {
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
                    },
                    "Next": "ChoiceAState",
                }
            ],
            "Default": "DefaultState",
        },
    ),
    StateTestCase(
        name="Choice State with boolean data test expression",
        definition={
            "Type": "Choice",
            "Comment": "No info",
            "Choices": [
                {
                    "Not": {
                        "Variable": "$.SomePath.status",
                        "IsString": True,
                    },
                    "Next": "ChoiceAState",
                }
            ],
            "Default": "DefaultState",
        },
    ),
    StateTestCase(
        name="Choice State using And and Or",
        definition={
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
                    "Next": "ChoiceAState",
                },
                {
                    "And": [
                        {
                            "Variable": "$.SomePath.status",
                            "StringEquals": "FAILED",
                        },
                        {
                            "Variable": "$.SomePath.status",
                            "StringEquals": "FAILED",
                        },
                    ],
                    "Next": "ChoiceAState",
                },
            ],
            "Default": "DefaultState",
        },
    ),
]

invalid_test_cases = [
    StateTestCase(
        name="Choice State using empty Or",
        definition={
            "Type": "Choice",
            "Comment": "No info",
            "Choices": [
                {
                    "Or": [],
                    "Next": "ChoiceAState",
                }
            ],
            "Default": "DefaultState",
        },
    ),
    StateTestCase(
        name="Choice State using empty And",
        definition={
            "Type": "Choice",
            "Comment": "No info",
            "Choices": [
                {
                    "And": [],
                    "Next": "ChoiceAState",
                }
            ],
            "Default": "DefaultState",
        },
    ),
    StateTestCase(
        name="Empty choices for Choice State",
        definition={
            "Type": "Choice",
            "Comment": "No info",
            "Choices": [],
            "Default": "DefaultState",
        },
    ),
    StateTestCase(
        name="Extra fields set on Choice State",
        definition={
            "Type": "Choice",
            "Comment": "No info",
            "ThisIsAnExtraField": True,
            "Choices": [
                {
                    "Variable": "$.SomePath.status",
                    "StringEquals": "FAILED",
                    "Next": "ChoiceAState",
                },
                {
                    "Variable": "$.SomePath.status",
                    "StringEquals": "FAILED",
                    "Next": "DefaultState",
                },
            ],
            "Default": "DefaultState",
        },
    ),
    StateTestCase(
        name="Choice State using or where next is defined",
        definition={
            "Type": "Choice",
            "Comment": "No info",
            "Choices": [
                {
                    "Or": [
                        {
                            "Variable": "$.SomePath.status",
                            "StringEquals": "FAILED",
                            "Next": "ChoiceAState",
                        },
                        {
                            "Variable": "$.SomePath.status",
                            "StringEquals": "FAILED",
                        },
                    ],
                    "Next": "ChoiceAState",
                }
            ],
            "Default": "DefaultState",
        },
    ),
    StateTestCase(
        name="Choice State using Not where Next is defined",
        definition={
            "Type": "Choice",
            "Comment": "No info",
            "Choices": [
                {
                    "Not": {
                        "Variable": "$.SomePath.status",
                        "StringEquals": "FAILED",
                        "Next": "ChoiceAState",
                    },
                    "Next": "ChoiceAState",
                }
            ],
            "Default": "DefaultState",
        },
    ),
    StateTestCase(
        name="Choice State with invalid comparison operator",
        definition={
            "Type": "Choice",
            "Comment": "No info",
            "Choices": [
                {
                    "Variable": "$.SomePath.status",
                    "NOT_AN_OPERATOR": "FAILED",
                    "Next": "ChoiceAState",
                },
            ],
            "Default": "DefaultState",
        },
    ),
    StateTestCase(
        name="Choice State with conjunctive nested boolean condition using Next",
        definition={
            "Type": "Choice",
            "Comment": "No info",
            "Choices": [
                {
                    "Not": {
                        "Or": [
                            {
                                "Variable": "$.SomePath.status",
                                "StringEquals": "FAILED",
                                "Next": "DefaultState",
                            },
                            {
                                "Variable": "$.SomePath.status",
                                "StringEquals": "FAILED",
                            },
                        ],
                    },
                    "Next": "ChoiceAState",
                }
            ],
            "Default": "DefaultState",
        },
    ),
    StateTestCase(
        name="Choice State with conjunctive nested boolean condition with extra fields",
        definition={
            "Type": "Choice",
            "Comment": "No info",
            "Choices": [
                {
                    "Not": {
                        "Or": [
                            {
                                "Variable": "$.SomePath.status",
                                "StringEquals": "FAILED",
                                "ThisIsAnExtraField": True,
                            },
                            {
                                "Variable": "$.SomePath.status",
                                "StringEquals": "FAILED",
                            },
                        ],
                    },
                    "Next": "ChoiceAState",
                }
            ],
            "Default": "DefaultState",
        },
    ),
    StateTestCase(
        name="Choice State with non-boolean data test expression",
        definition={
            "Type": "Choice",
            "Comment": "No info",
            "Choices": [
                {
                    "Not": {
                        "Variable": "$.SomePath.status",
                        "IsString": "True",
                    },
                    "Next": "ChoiceAState",
                }
            ],
            "Default": "DefaultState",
        },
    ),
]


@pytest.mark.parametrize("test_case", valid_test_cases, ids=str)
def test_valid_choice_states_pass_validation(test_case: StateTestCase):
    state_model = ChoiceState(**test_case.definition)
    state_def_out = state_model.model_dump(exclude_unset=True)
    assert state_def_out == test_case.definition


@pytest.mark.parametrize("test_case", invalid_test_cases, ids=str)
def test_invalid_choice_states_fail_validation(test_case: StateTestCase):
    with pytest.raises(StepFunctionValidationError) as ve:
        ChoiceState(**test_case.definition)

    assert ve.type is StepFunctionValidationError
