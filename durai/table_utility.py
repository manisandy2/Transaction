import os
import logging
from datetime import datetime, date
from typing import Dict, List, Tuple, Any, Optional
import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType, LongType, DoubleType, DateType, TimestampType,
    StringType, NestedField, FloatType
)

# Main logger
logger = logging.getLogger(__name__)

# Separate logger for row-level errors (Log Store)
LOG_DIR = "logs_backup"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

row_error_logger = logging.getLogger("row_errors")
row_error_logger.setLevel(logging.WARNING)
row_error_logger.propagate = False # Don't send these to the root logger

# Clear existing handlers if any to avoid duplicates in dev environments
if row_error_logger.hasHandlers():
    row_error_logger.handlers.clear()

fh = logging.FileHandler(os.path.join(LOG_DIR, "row_errors.log"))
fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
row_error_logger.addHandler(fh)

def clean_rows(
        rows: List[Dict[str, Any]],
        boolean_fields: List[str],
        timestamps_fields: List[str],
        field_overrides: Dict[str, tuple]
    ) -> List[Dict[str, Any]]:
    """
    Clean and normalize row data for schema compliance.
    """
    dt_formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%m/%d/%Y %H:%M",
        "%Y-%m-%d",
    ]

    for row in rows:
        pri_id = row.get("pri_id", "UNKNOWN")
        
        # 1. Boolean Fields
        for f in boolean_fields:
            val = row.get(f)
            if val is None:
                logger.warning(f"[ID: {pri_id}] Required boolean field {f} is None, defaulting to False")
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
                    logger.info(f"[ID: {pri_id}] Required timestamp {f} is None, using current timestamp")
                    row[f] = datetime.now()
                else:
                    # Optional field, leave as None
                    row[f] = None
                continue

            if isinstance(val, (datetime, date)):
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
                    logger.debug(f"[ID: {pri_id}] Could not parse optional timestamp {f}: {val}, keeping as None")
                    row[f] = None
            else:
                row[f] = parsed

        # 3. Handle Other Fields
        for key, val in row.items():
            if key not in boolean_fields + timestamps_fields:
                if key in field_overrides:
                    _, _, is_required = field_overrides[key]
                    if val is None:
                        if is_required:
                            logger.warning(f"[ID: {pri_id}] Required field {key} is None, defaulting to empty string")
                            row[key] = ""
                        else:
                            row[key] = None
                    else:
                        row[key] = str(val)
                else:
                    row[key] = str(val) if val is not None else None

    return rows

def generate_field_id(name: str) -> int:
    """
    Stable, deterministic field_id based on field name
    """
    return abs(hash(name)) % 1_000_000 + 1

def process_chunk(chunk: List[Dict[str, Any]], arrow_schema: pa.Schema) -> Tuple[pa.Table, List[Dict[str, Any]]]:
    """
    Process a chunk of rows and convert to Arrow Table.
    Logs errors to a separate store indexed by pri_id.
    """
    processed_rows = []
    row_errors = []
    date_formats = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y")
    
    for row_idx, row in enumerate(chunk):
        converted_row = {}
        pri_id = row.get("pri_id", f"idx_{row_idx}")
        row_has_error = False

        for field in arrow_schema:
            val = row.get(field.name, None)

            # Debug missing field
            if field.name not in row:
                row_error_logger.warning(f"[ID: {pri_id}] Field '{field.name}' missing in row data")

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
                        err_msg = f"Unrecognized date format in '{field.name}': {val}"
                        row_error_logger.error(f"[ID: {pri_id}] {err_msg}")
                        row_errors.append({
                            "pri_id": pri_id, 
                            "field": field.name, 
                            "error": err_msg, 
                            "value": val,
                            "row_data": row # Include original row
                        })
                        converted_row[field.name] = None
                        row_has_error = True

                # --- Default: keep as string or object ---
                else:
                    converted_row[field.name] = val

            except Exception as e:
                err_msg = f"Field '{field.name}' conversion failed: {str(e)}"
                row_error_logger.error(f"[ID: {pri_id}] {err_msg} | Value: {val}")
                row_errors.append({
                    "pri_id": pri_id,
                    "field": field.name,
                    "error": err_msg,
                    "value": str(val),
                    "row_data": row # Include original row
                })
                converted_row[field.name] = None
                row_has_error = True

        processed_rows.append(converted_row)

    return pa.Table.from_pylist(processed_rows, schema=arrow_schema), row_errors