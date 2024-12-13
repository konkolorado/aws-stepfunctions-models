from dataclasses import dataclass


@dataclass
class StateTestCase:
    name: str
    definition: dict

    def __str__(self) -> str:
        return self.name
