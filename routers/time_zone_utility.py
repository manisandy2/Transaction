from datetime import datetime


TS_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
]

def safe_parse_timestamp(value):
    if value in (None, "", 0):
        return None

    if isinstance(value, datetime):
        return value

    value = str(value).strip()

    for fmt in TS_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    return None