import typing as t

import arrow
from pydantic import BaseModel

T = t.TypeVar("T", str, dict)


def enforce_jsonpath(item: T | None) -> T | None:
    if item is None:
        return item
    elif isinstance(item, str) and not item.startswith("$."):
        raise ValueError(f'"{item}" must be a JSONPath and start with "$."')
    elif isinstance(item, dict):
        for k, v in item.items():
            if k.endswith(".$") and (not isinstance(v, str) or not v.startswith("$.")):
                raise ValueError(
                    f'Key "{k}" indicates its value will be a JSONPath, however its value "{v}" is not a JSONPath'
                )
            elif isinstance(v, dict):
                enforce_jsonpath(v)
    return item


def enforce_exclusive_fields(
    self: BaseModel, exclusive_fields: list[str], field_required: bool = True
) -> None:
    set_fields = {f for f in exclusive_fields if getattr(self, f, None) is not None}
    if len(set_fields) > 1:
        raise ValueError(
            f"Only one of the following fields may be set: {', '.join(set_fields)}"
        )
    if field_required and len(set_fields) == 0:
        raise ValueError(
            f"At least one of the following fields must be set: {', '.join(exclusive_fields)}"
        )


def enforce_datetime_format(v: str):
    # Check ISO-8601 format
    try:
        arrow.get(v)
    except arrow.ParserError:
        raise ValueError(f"Date-time string '{v}' is not ISO-8601 compliant.")

    # Check that a date and time are specified
    if len(v) < 10:
        raise ValueError(f"Date-time string '{v}' must indicate a date and a time.")

    # Check 'T' seperator is used
    if v[10] != "T":
        raise ValueError(
            f"Date-time string '{v}' must use 'T' to separate the date and time values."
        )

    # Check that non-timezone aware date times end with Z
    if v[-5] != "+" and v[-3] != ":" and not v.endswith("Z"):
        raise ValueError("Naive date-time strings must end in Z")
