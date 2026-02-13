import logging
import json
from optparse import Option
from typing import Dict, List, Tuple, Any,Optional
import pyarrow as pa
from pyiceberg.types import (
    BooleanType, LongType, DoubleType, DateType, IntegerType,
    TimestampType, StringType, NestedField, FloatType
)
from datetime import datetime, date
from pyiceberg.schema import Schema


logger = logging.getLogger(__name__)

def schema(record: Dict[str, Any],
            required_fields: List[str],
            field_overrides: Dict[str, tuple]
           ) -> Tuple[Schema, pa.Schema]:

    # Validate required fields
    missing = [f for f in required_fields if f not in record]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")

    iceberg_fields = []
    arrow_fields = []

    # Sort for deterministic field IDs
    sorted_items = sorted(record.items())

    for idx, (name, value) in enumerate(sorted_items, start=1):
        if name in field_overrides:
            ice_type, arrow_type, required = field_overrides[name]
        else:
            required = False

            # Boolean
            if isinstance(value, bool):
                ice_type, arrow_type = BooleanType(), pa.bool_()

            # Integer
            elif isinstance(value, int):
                ice_type, arrow_type = LongType(), pa.int64()

            # Float
            elif isinstance(value, float):
                ice_type, arrow_type = DoubleType(), pa.float64()

            # Date only
            elif isinstance(value, date) and not isinstance(value, datetime):
                ice_type, arrow_type = DateType(), pa.date32()

            # Timestamp
            elif isinstance(value, datetime):
                ice_type, arrow_type = TimestampType(), pa.timestamp("ms")

            # String (default)
            else:
                ice_type, arrow_type = StringType(), pa.string()

        iceberg_fields.append(
            NestedField(field_id=idx, name=name, field_type=ice_type, required=required)
        )
        arrow_fields.append(pa.field(name, arrow_type, nullable=not required))

    iceberg_schema = Schema(*iceberg_fields)
    arrow_schema = pa.schema(arrow_fields)
    return iceberg_schema, arrow_schema


def clean_rows(
        rows: List[Dict[str, Any]],
        boolean_fields: Optional[List[str]] = None,
        timestamps_fields:Optional[List[str]] = None,
        date_fields: Optional[List[str]] = None,
    field_overrides: Optional[Dict[str, tuple]] = None
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Clean and normalize row data for hub_masters schema compliance.

    Args:
        rows: List of row dictionaries.
        boolean_fields: List of field names that should be normalized to boolean values.
        timestamps_fields: List of field names that should be parsed/normalized as timestamps.
        field_overrides: Mapping of field names to override tuples used to adjust values during cleaning.

    Returns:
        Cleaned list of row dictionaries.
    """
    boolean_fields = boolean_fields or []
    timestamps_fields = timestamps_fields or []
    date_fields = date_fields or []
    field_overrides = field_overrides or {}

    dt_formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%m/%d/%Y %H:%M",
        "%Y-%m-%d",
    ]

    date_formats = [
        "%Y-%m-%d",
        "%d/%m/%Y",
    ]

    row_errors = []
    cleaned_rows = []

    for row in rows:
        pri_id = row.get("pri_id", "UNKNOWN_ID")
        try:
            # 1. Boolean Fields
            for f in boolean_fields:
                val = row.get(f)
                if val is None:
                    # logger.warning(f"[ID: {pri_id}] Required boolean field {f} is None, defaulting to False")
                    row[f] = False
                elif isinstance(val, bool):
                    row[f] = val
                elif isinstance(val, int):
                    row[f] = bool(val)
                elif isinstance(val, str):
                    row[f] = val.lower() in ("1", "true", "yes", "on")
                else:
                    row[f] = False

            # 2. Timestamp Fields
            for f in timestamps_fields:
                val = row.get(f)
                
                # Identify if field is required from overrides
                is_required = False
                if f in field_overrides:
                    _, _, is_required = field_overrides[f]

                # Normalize "null"/"None" strings to None
                if isinstance(val, str) and val.lower() in ("null", "none", ""):
                    val = None

                if val is None:
                    if is_required:
                        # logger.info(f"[ID: {pri_id}] Required timestamp {f} is None, using current timestamp")
                        row[f] = datetime.now()
                    else:
                        # Optional field, leave as None
                        row[f] = None
                    continue

                if isinstance(val, (datetime, date)):
                    row[f] = val
                    continue

                # Try multiple formats
                parsed = None
                for fmt in dt_formats:
                    try:
                        parsed = datetime.strptime(str(val), fmt)
                        break
                    except (ValueError, TypeError):
                        pass

                if parsed is None:
                    if is_required:
                        logger.warning(f"[ID: {pri_id}] Failed to parse required timestamp {f}: {val}, using current timestamp")
                        row[f] = datetime.now()
                    else:
                        # logger.debug(f"[ID: {pri_id}] Could not parse optional timestamp {f}: {val}, keeping as None")
                        row[f] = None
                else:
                    row[f] = parsed

            # 3. Date Fields (date only)
            for f in date_fields:
                val = row.get(f)
                
                # Identify if field is required from overrides
                is_required = False
                if f in field_overrides:
                    _, _, is_required = field_overrides[f]

                # Normalize "null"/"None" strings to None
                if isinstance(val, str) and val.lower() in ("null", "none", ""):
                    val = None

                if val is None:
                    if is_required:
                        # logger.info(f"[ID: {pri_id}] Date field {f} is None, defaulting to today")
                        row[f] = date.today()
                    else:
                        row[f] = None
                    continue

                if isinstance(val, date):
                    row[f] = val
                    continue

                # Try formats
                parsed = None
                for fmt in date_formats:
                    try:
                        parsed = datetime.strptime(str(val), fmt).date()
                        break
                    except (ValueError, TypeError):
                        pass

                if parsed is None:
                    if is_required:
                        logger.warning(f"[ID: {pri_id}] Failed to parse required date {f}: {val}, using today")
                        row[f] = date.today()
                    else:
                        row[f] = None
                else:
                    row[f] = parsed

            # 4. String Fields (Everything else)
            for key, val in row.items():
                if key not in boolean_fields + timestamps_fields + date_fields: # Added date_fields here
                    # Check if this field override exists and is required
                    if key in field_overrides:
                        _, _, is_required = field_overrides[key]
                        if val is None:
                            if is_required:
                                logger.warning(f"[ID: {pri_id}] Required string field {key} is None, defaulting to empty string")
                                row[key] = ""
                            else:
                                row[key] = None
                        else:
                            row[key] = str(val)
                    else:
                        # Generic handling for non-overridden fields
                        row[key] = str(val) if val is not None else None
            
            cleaned_rows.append(row)

        except Exception as e:
            logger.error(f"[ID: {pri_id}] Error cleaning row: {e}")
            row_errors.append({"pri_id": pri_id, "error": str(e), "row_data": row})

    return cleaned_rows, row_errors

def process_chunk(chunk, arrow_schema):
    processed_rows = []
    date_formats = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y")

    for row_idx, row in enumerate(chunk):
        converted_row = {}
        # print(f" Processing row {row_idx} -> keys: {list(row.keys())}")

        for field in arrow_schema:
            val = row.get(field.name, None)

            # Debug mismatched field
            if field.name not in row:
                print(f"⚠️ Field '{field.name}' missing in row; available keys: {list(row.keys())}")

            try:
                # --- Handle empty or None values ---
                if val in ("", " ", None):
                    converted_row[field.name] = None
                    continue

                # --- Integer fields ---
                if pa.types.is_integer(field.type):
                    converted_row[field.name] = int(val)

                # --- Float fields ---
                elif pa.types.is_floating(field.type):
                    converted_row[field.name] = float(val)

                # --- Timestamp or date fields ---
                elif pa.types.is_timestamp(field.type) or pa.types.is_date(field.type):
                    parsed_date = None

                    if isinstance(val, (datetime, date)):
                        parsed_date = val
                    elif isinstance(val, str):
                        val = val.strip()
                        for fmt in date_formats:
                            try:
                                parsed_date = datetime.strptime(val, fmt)
                                break
                            except ValueError:
                                continue

                    if parsed_date:
                        converted_row[field.name] = (
                            parsed_date if isinstance(parsed_date, datetime)
                            else datetime.combine(parsed_date, datetime.min.time())
                        )
                    else:
                        print(f" Row {row_idx}: Unrecognized date in '{field.name}': {val}")
                        converted_row[field.name] = None

                # --- Default: keep as string or object ---
                else:
                    converted_row[field.name] = val

            except Exception as e:
                print(f" Row {row_idx}, Field '{field.name}', Value: {val}, Error: {e}")
                converted_row[field.name] = None

        processed_rows.append(converted_row)

    #  Debug before conversion
    # print(" Example converted_row:", processed_rows[0] if processed_rows else "EMPTY")

    return pa.Table.from_pylist(processed_rows, schema=arrow_schema)