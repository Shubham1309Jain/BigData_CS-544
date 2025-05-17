import enum

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lib.types import Score


class TestStatus(enum.Enum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    FATAL = "FATAL"
    PARTIAL = "PARTIAL"

    @classmethod
    def from_score(cls, score: "Score") -> "TestStatus":
        if score.earned == 0:
            return TestStatus.FAILED
        if 0 < score.earned < score.total:
            return TestStatus.PARTIAL
        return TestStatus.PASSED


class Project(enum.Enum):
    P1 = "p1"
    P2 = "p2"
    P3 = "p3"
    P4 = "p4"
    P5 = "p5"
    P6 = "p6"
    P7 = "p7"
    P8 = "p8"


class StdOut(enum.Enum):
    PRINT = "print"
    JSON = "json"


class Registry(enum.Enum):
    TEST = "test"
    CALLBACK = "callback"
