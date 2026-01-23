"""
Error handling utilities for data ingestion failures
Provides two strategies:
1. Add error columns to existing records
2. Save failed records to a separate error table
"""

import json
from datetime import datetime, date
from typing import List, Dict, Any, Optional
import pyarrow as pa
from core.catalog_client import get_catalog_client
from pyiceberg.catalog import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, TimestampType, LongType, StructType
)
from core.logger import get_logger

logger = get_logger("error-handler")


class ErrorHandler:
    """Handle failed data ingestion records"""
    
    def __init__(self, namespace: str = "POS_Transactions"):
        self.namespace = namespace
        self.catalog = get_catalog_client()
    
    def add_error_columns_to_records(
        self, 
        records: List[Dict[str, Any]], 
        error_message: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Add error tracking columns to records
        
        Args:
            records: List of record dictionaries
            error_message: Error message to add (None for success)
            
        Returns:
            Records with added error columns
        """
        error_status = "failed" if error_message else "success"
        error_timestamp = datetime.utcnow().isoformat()
        
        for record in records:
            record["ingestion_error_status"] = error_status
            record["ingestion_error_message"] = error_message or ""
            record["ingestion_error_timestamp"] = error_timestamp
            record["ingestion_retry_count"] = 0
        
        return records
    
    def save_failed_records_to_error_table(
        self,
        table_name: str,
        failed_records: List[Dict[str, Any]],
        error_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Save failed records to a dedicated error table
        
        Args:
            table_name: Original table name
            failed_records: List of records that failed
            error_info: Dictionary with error details (error_type, error_message, etc.)
            
        Returns:
            Result dictionary with success status and details
        """
        error_table_name = f"{table_name}_errors"
        table_identifier = f"{self.namespace}.{error_table_name}"
        
        logger.info(f"Saving {len(failed_records)} failed records to {table_identifier}")
        
        # Enrich records with error metadata
        enriched_records = []
        for record in failed_records:
            # Try to find a reasonable ID
            rid = str(record.get("pri_id") or record.get("id") or record.get("Id") or "unknown")
            
            # Custom JSON serializer for dates/datetimes
            def json_serial(obj):
                if isinstance(obj, (datetime, date)):
                    return obj.isoformat()
                raise TypeError ("Type %s not serializable" % type(obj))

            error_record = {
                "original_table": table_name,
                "error_type": error_info.get("error_type", "UNKNOWN"),
                "error_message": error_info.get("error_message", ""),
                "error_timestamp": datetime.utcnow().isoformat(),
                "retry_count": 0,
                "original_data": json.dumps(record, default=json_serial),  # Store as JSON string
                "record_id": rid
            }
            enriched_records.append(error_record)
        
        try:
            # Try to load existing error table
            error_table = self.catalog.load_table(table_identifier)
            logger.info(f"Error table {table_identifier} exists, appending records")
            
        except NoSuchTableError:
            # Create error table if it doesn't exist
            logger.warning(f"Error table {table_identifier} not found, creating it")
            error_schema = self._create_error_table_schema()
            
            try:
                error_table = self.catalog.create_table(
                    identifier=table_identifier,
                    schema=error_schema,
                )
                logger.info(f"Created error table: {table_identifier}")
                
            except Exception as e:
                logger.exception(f"Failed to create error table: {e}")
                return {
                    "success": False,
                    "error": f"Could not create error table: {str(e)}"
                }
        
        # Convert to Arrow and append
        try:
            arrow_schema = pa.schema([
                pa.field("original_table", pa.string(), nullable=False),
                pa.field("error_type", pa.string(), nullable=False),
                pa.field("error_message", pa.string(), nullable=True),
                pa.field("error_timestamp", pa.string(), nullable=False),
                pa.field("retry_count", pa.int64(), nullable=False),
                pa.field("original_data", pa.string(), nullable=False),
                pa.field("record_id", pa.string(), nullable=True),
            ])
            
            arrow_table = pa.Table.from_pylist(enriched_records, schema=arrow_schema)
            error_table.append(arrow_table)
            
            logger.info(f"Successfully saved {len(enriched_records)} error records")
            
            return {
                "success": True,
                "error_table": table_identifier,
                "records_saved": len(enriched_records),
                "error_type": error_info.get("error_type"),
            }
            
        except Exception as e:
            logger.exception(f"Failed to append to error table: {e}")
            return {
                "success": False,
                "error": f"Failed to save error records: {str(e)}"
            }
    
    def _create_error_table_schema(self) -> Schema:
        """Create Iceberg schema for error table"""
        return Schema(
            NestedField(1, "original_table", StringType(), required=True),
            NestedField(2, "error_type", StringType(), required=True),
            NestedField(3, "error_message", StringType(), required=False),
            NestedField(4, "error_timestamp", StringType(), required=True),
            NestedField(5, "retry_count", LongType(), required=True),
            NestedField(6, "original_data", StringType(), required=True),
            NestedField(7, "record_id", StringType(), required=False),
        )
    
    def get_error_records(
        self, 
        table_name: str, 
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Retrieve error records from error table
        
        Args:
            table_name: Original table name
            limit: Maximum number of records to retrieve
            
        Returns:
            List of error records
        """
        error_table_name = f"{table_name}_errors"
        table_identifier = f"{self.namespace}.{error_table_name}"
        
        try:
            error_table = self.catalog.load_table(table_identifier)
            scan = error_table.scan(limit=limit)
            
            records = []
            for batch in scan.to_arrow():
                records.extend(batch.to_pylist())
            
            return records
            
        except NoSuchTableError:
            logger.warning(f"Error table {table_identifier} does not exist")
            return []
        except Exception as e:
            logger.exception(f"Failed to retrieve error records: {e}")
            return []


# Utility function for easy import
def handle_ingestion_error(
    table_name: str,
    failed_records: List[Dict[str, Any]],
    error_type: str,
    error_message: str,
    use_error_table: bool = True,
    namespace: str = "POS_Transactions"
) -> Dict[str, Any]:
    """
    Convenience function to handle ingestion errors
    
    Args:
        table_name: Name of the table
        failed_records: Records that failed to ingest
        error_type: Type of error (e.g., "ARROW_CONVERSION", "ICEBERG_APPEND")
        error_message: Detailed error message
        use_error_table: If True, save to error table; if False, add error columns
        namespace: Catalog namespace
        
    Returns:
        Result dictionary
    """
    handler = ErrorHandler(namespace=namespace)
    
    if use_error_table:
        return handler.save_failed_records_to_error_table(
            table_name=table_name,
            failed_records=failed_records,
            error_info={
                "error_type": error_type,
                "error_message": error_message
            }
        )
    else:
        # Add error columns to records
        enriched = handler.add_error_columns_to_records(
            records=failed_records,
            error_message=error_message
        )
        return {
            "success": True,
            "method": "error_columns",
            "records_enriched": len(enriched),
            "records": enriched
        }
