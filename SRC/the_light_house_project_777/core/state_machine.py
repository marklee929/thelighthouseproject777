from enum import Enum


class S(Enum):
    """
    Enumeration that defines the application states.
    """

    IDLE = "IDLE"
    QUICK = "QUICK"
    SEARCHING = "SEARCHING"
    WORKING = "WORKING"
    MEETING = "MEETING"
    DONE = "DONE"

    def __str__(self):
        return str(self.value)
