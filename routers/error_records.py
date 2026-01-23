"""
API endpoints for error record management
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from .error_handler import ErrorHandler
from core.logger import get_logger

logger = get_logger("error-records-api")

router = APIRouter(prefix="/errors", tags=["Error Management"])


@router.get("/{table_name}")
def get_error_records(
    table_name: str,
    limit: int = Query(100, description="Maximum number of error records to retrieve"),
):
    """
    Retrieve error records for a specific table
    
    Args:
        table_name: Name of the original table (e.g., 'pickup_delivery_items_w')
        limit: Maximum number of records to return
        
    Returns:
        List of error records with metadata
    """
    logger.info(f"Fetching error records for table: {table_name}, limit: {limit}")
    
    try:
        handler = ErrorHandler()
        error_records = handler.get_error_records(table_name, limit=limit)
        
        if not error_records:
            return {
                "success": True,
                "message": f"No error records found for table {table_name}",
                "count": 0,
                "records": []
            }
        
        logger.info(f"Retrieved {len(error_records)} error records")
        
        return {
            "success": True,
            "message": f"Retrieved error records for {table_name}",
            "count": len(error_records),
            "records": error_records
        }
        
    except Exception as e:
        logger.exception(f"Failed to retrieve error records: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve error records: {str(e)}"
        )


@router.get("/{table_name}/summary")
def get_error_summary(table_name: str):
    """
    Get summary statistics of error records for a table
    
    Args:
        table_name: Name of the original table
        
    Returns:
        Summary statistics (count by error type, etc.)
    """
    logger.info(f"Fetching error summary for table: {table_name}")
    
    try:
        handler = ErrorHandler()
        error_records = handler.get_error_records(table_name, limit=10000)
        
        if not error_records:
            return {
                "success": True,
                "table_name": table_name,
                "total_errors": 0,
                "error_types": {}
            }
        
        # Group by error type
        error_types = {}
        for record in error_records:
            error_type = record.get("error_type", "UNKNOWN")
            error_types[error_type] = error_types.get(error_type, 0) + 1
        
        logger.info(f"Error summary: {len(error_records)} total errors")
        
        return {
            "success": True,
            "table_name": table_name,
            "total_errors": len(error_records),
            "error_types": error_types,
            "sample_record": error_records[0] if error_records else None
        }
        
    except Exception as e:
        logger.exception(f"Failed to generate error summary: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate error summary: {str(e)}"
        )
