from pyiceberg.types import (
    IntegerType,
    StringType,
    DoubleType,
    BooleanType,
    TimestampType,
)
from pyiceberg.transforms import (
    IdentityTransform, YearTransform, MonthTransform,
    DayTransform, HourTransform, BucketTransform, TruncateTransform)

def get_iceberg_type(type_str: str):
    type_str = type_str.lower()
    if type_str in ["int", "integer"]:
        return IntegerType()
    elif type_str in ["string", "str"]:
        return StringType()
    elif type_str in ["double", "float"]:
        return DoubleType()
    elif type_str in ["bool", "boolean"]:
        return BooleanType()
    elif type_str in ["timestamp", "datetime"]:
        return TimestampType()
    else:
        raise ValueError(f"Unsupported Iceberg type: {type_str}")


# Helper: Map string -> Iceberg Transform
def get_transform(transform: str, arg: int | None = None):
    transform = transform.lower()
    if transform == "identity":
        return IdentityTransform()
    elif transform == "year":
        return YearTransform()
    elif transform == "month":
        return MonthTransform()
    elif transform == "day":
        return DayTransform()
    elif transform == "hour":
        return HourTransform()
    elif transform.startswith("bucket"):
        return BucketTransform(arg or 16)
    elif transform.startswith("truncate"):
        return TruncateTransform(arg or 4)
    else:
        raise ValueError(f"Unsupported transform: {transform}")