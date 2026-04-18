from enum import Enum


class S(Enum):
    """
    애플리케이션의 상태를 정의하는 열거형 클래스.
    """

    IDLE = "IDLE"
    QUICK = "QUICK"
    SEARCHING = "SEARCHING"
    WORKING = "WORKING"
    MEETING = "MEETING"
    DONE = "DONE"

    def __str__(self):
        return str(self.value)