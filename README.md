# Pydantic Validation for AWS StepFunctions

This repo contains pydantic models that can be used to validate AWS StepFunction
definitions. It provides all the ergonimics of a validated + serialized
StepFunction definitions for use in Python code. 

```python
from aws_stepfunctions_models import StepFunctionDefinition

definition = StepFunctionDefinition(**{
    "StartAt": "SimplePass",
    "Comment": "This is allowed",
    "States": {
        "SimplePass": {
            "Type": "Pass",
            "Parameters": {},
            "End": True,
        },
    },
})

assert definition.StartAt == "SimplePass"
```

You can also handle validation errors:

```python
from aws_stepfunctions_models import StepFunctionDefinition, StepFunctionValidationError

try:
    definition = StepFunctionDefinition(
        **{"StartAt": "Test", "Comment": "There are no states defined...", "States": {}}
    )
except StepFunctionValidationError:
    print("That's not a valid step function")
```

### TODO
- Investigate performing all validation, and returning all failures
- Investigate customizing field definitions 
- Restructure to be more modular (?)
- Support camel_case field access (instead of PascalCase)
