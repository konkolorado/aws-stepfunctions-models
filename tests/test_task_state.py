import pytest

from aws_stepfunctions_models import StepFunctionValidationError, TaskState

from .common import StateTestCase

valid_test_cases = [
    StateTestCase(
        name="Simple Task State",
        definition={
            "Type": "Task",
            "Comment": "No info",
            "Resource": "http://localhost:5000",
            "Parameters": {},
            "ResultPath": "$.some_path",
            "End": True,
        },
    ),
    StateTestCase(
        name="Task State with Catcher",
        definition={
            "Type": "Task",
            "Comment": "No info",
            "Resource": "http://localhost:5000",
            "Parameters": {},
            "ResultPath": "$.some_path",
            "End": True,
            "Catch": [
                {
                    "ErrorEquals": ["ErrorA", "ErrorB"],
                    "Next": "TerminalState",
                }
            ],
        },
    ),
    StateTestCase(
        name="Task State with multiple Catchers",
        definition={
            "Type": "Task",
            "Comment": "No info",
            "Resource": "http://localhost:5000",
            "Parameters": {},
            "ResultPath": "$.some_path",
            "End": True,
            "Catch": [
                {
                    "ErrorEquals": ["ErrorA", "ErrorB"],
                    "Next": "TerminalState",
                },
                {
                    "ErrorEquals": ["ErrorC"],
                    "Next": "TerminalState",
                },
            ],
        },
    ),
    StateTestCase(
        name="Task State with nested Parameters with JSONPath syntax",
        definition={
            "Type": "Task",
            "Comment": "No info",
            "Resource": "http://localhost:5000",
            "Parameters": {"a_json_path.$": "$.a_json_path"},
            "ResultPath": "$.some_path",
            "End": True,
        },
    ),
    StateTestCase(
        name="Task State with partial task url",
        definition={
            "Type": "Task",
            "Comment": "No info",
            "Resource": "localhost:5000",
            "Parameters": {},
            "ResultPath": "$.some_path",
            "End": True,
        },
    ),
    StateTestCase(
        name="Task State with PostgreSQL task url",
        definition={
            "Type": "Task",
            "Comment": "No info",
            "Resource": "pgsql://localhost:5000",
            "Parameters": {},
            "ResultPath": "$.some_path",
            "End": True,
        },
    ),
]

invalid_test_cases = [
    StateTestCase(
        name="Task State with catchers with invalid ResultPath",
        definition={
            "Type": "Task",
            "Comment": "No info",
            "Resource": "http://localhost:5000",
            "Parameters": {},
            "ResultPath": "$.some_path",
            "End": True,
            "Catch": [
                {
                    "ErrorEquals": ["ErrorA", "ErrorB"],
                    "Next": "TerminalState",
                    "ResultPath": "NOT_A_JSON_PATH",
                }
            ],
        },
    ),
    StateTestCase(
        name="Task State with Catchers with empty ErrorEquals",
        definition={
            "Type": "Task",
            "Comment": "No info",
            "Resource": "http://localhost:5000",
            "Parameters": {},
            "ResultPath": "$.some_path",
            "End": True,
            "Catch": [
                {
                    "ErrorEquals": [],
                    "Next": "TerminalState",
                }
            ],
        },
    ),
    StateTestCase(
        name="Task State with Catchers with extrafields",
        definition={
            "Type": "Task",
            "Comment": "No info",
            "Resource": "http://localhost:5000",
            "Parameters": {},
            "ResultPath": "$.some_path",
            "End": True,
            "Catch": [
                {
                    "ErrorEquals": ["ErrorA"],
                    "Next": "TerminalState",
                    "ThisIsAnExtraField": True,
                }
            ],
        },
    ),
    StateTestCase(
        name="Task State with missing Parameters and InputPath",
        definition={
            "Type": "Task",
            "Comment": "No info",
            "Resource": "http://localhost:5000",
            "ResultPath": "$.some_path",
            "End": True,
        },
    ),
    StateTestCase(
        name="InputPath is not JSONPath",
        definition={
            "Type": "Task",
            "Comment": "No info",
            "Resource": "http://localhost:5000",
            "InputPath": "NOT_JSON_PATH",
            "ResultPath": "$.some_path",
            "End": True,
        },
    ),
    StateTestCase(
        name="ResultPath is not JSONPath",
        definition={
            "Type": "Task",
            "Comment": "No info",
            "Resource": "http://localhost:5000",
            "ResultPath": "NOT_JSON_PATH",
            "End": True,
        },
    ),
    StateTestCase(
        name="Parameters are not a dict",
        definition={
            "Type": "Task",
            "Comment": "No info",
            "Resource": "http://localhost:5000",
            "Parameters": True,
            "ResultPath": "$.some_path",
            "End": True,
        },
    ),
    StateTestCase(
        name="Nested Parameters with invalid JSONPath value",
        definition={
            "Type": "Task",
            "Comment": "No info",
            "Resource": "http://localhost:5000",
            "Parameters": {"a_json_path.$": "not_a_json_path"},
            "ResultPath": "$.some_path",
            "End": True,
        },
    ),
    StateTestCase(
        name="Missing resource url",
        definition={
            "Type": "Task",
            "Comment": "No info",
            "Parameters": True,
            "ResultPath": "$.some_path",
            "End": True,
        },
    ),
]


@pytest.mark.parametrize("test_case", valid_test_cases, ids=str)
def test_valid_task_states_pass_validation(test_case: StateTestCase):
    state_model = TaskState(**test_case.definition)
    state_def_out = state_model.model_dump(exclude_unset=True)
    assert state_def_out == test_case.definition


@pytest.mark.parametrize("test_case", invalid_test_cases, ids=str)
def test_invalid_task_states_fail_validation(test_case: StateTestCase):
    with pytest.raises(StepFunctionValidationError) as ve:
        TaskState(**test_case.definition)

    assert ve.type is StepFunctionValidationError
