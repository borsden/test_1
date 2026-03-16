"""Helpers to work with B3 matchEventIndicator bitmask."""

END_OF_EVENT_MASK = 0x80
RECOVERY_MASK = 0x20


def is_end_of_event(value: int) -> bool:
    return bool(value & END_OF_EVENT_MASK)


def is_recovery(value: int) -> bool:
    return bool(value & RECOVERY_MASK)
