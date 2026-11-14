import math


def text(value: str, *, name: str, limit: int, nonempty: bool = False) -> bytes:
    encoded = value.encode("utf-8")
    if len(encoded) > limit or (nonempty and not encoded):
        raise ValueError(
            "{} must contain {}..{} UTF-8 bytes".format(name, 1 if nonempty else 0, limit)
        )
    return encoded


def byte_payload(value: object, *, limit: int) -> bytes:
    if not isinstance(value, bytes):
        raise TypeError("application payload must be bytes")
    if len(value) > limit:
        raise ValueError("payload exceeds {} bytes".format(limit))
    return value


def milliseconds(seconds: float | None) -> int:
    if seconds is None:
        return -1
    if isinstance(seconds, bool) or not math.isfinite(seconds) or seconds < 0:
        raise ValueError("duration must be finite and nonnegative")
    scaled = seconds * 1000
    if math.isfinite(scaled):
        return math.ceil(scaled)
    numerator, denominator = seconds.as_integer_ratio()
    return -(-numerator * 1000 // denominator)
