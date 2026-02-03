from datetime import date, datetime
from collections import defaultdict
import pyarrow as pa
from core.catalog_client import get_catalog_client

def run_month_wise_pos_aggregation(
    catalog,
    source_table_name: str,
    agg_table_name: str,
    start_date: date,
    end_date: date,
    batch_size: int = 50_000
):
    """
    Month-wise aggregation for POS Transactions (Iceberg → Iceberg)

    Args:
        catalog: Iceberg catalog client
        source_table_name: e.g. POS_Transactions.transaction
        agg_table_name: e.g. POS_Transactions.transaction_month_agg
        start_date: date(2026, 2, 1)
        end_date: date(2026, 3, 1)   # exclusive
        batch_size: Arrow batch size
    """

    src_table = catalog.load_table(source_table_name)
    agg_table = catalog.load_table(agg_table_name)

    # -------------------------------
    # Aggregation containers
    # -------------------------------
    agg = defaultdict(lambda: {
        "total_bills": 0,
        "total_amount": 0.0,
        "net_amount": 0.0,
        "cancelled_amount": 0.0,
        "customers": set(),
        "stores": set()
    })

    # -------------------------------
    # Scan Iceberg in batches
    # -------------------------------
    scan = src_table.scan(
        row_filter=(
            f"Bill_Date__c >= DATE '{start_date}' "
            f"AND Bill_Date__c < DATE '{end_date}'"
        )
    )

    for batch in scan.to_arrow_batches(batch_size=batch_size):
        rows = batch.to_pylist()

        for r in rows:
            bill_date = r.get("Bill_Date__c")
            if not bill_date:
                continue

            month_key = bill_date.replace(day=1)

            a = agg[month_key]
            a["total_bills"] += 1
            a["total_amount"] += r.get("bill_grand_total__c") or 0.0
            a["net_amount"] += r.get("bill_net_amount__c") or 0.0
            a["cancelled_amount"] += r.get("bill_cancel_amount__c") or 0.0

            if r.get("customer_mobile__c"):
                a["customers"].add(r["customer_mobile__c"])

            if r.get("store_code__c"):
                a["stores"].add(r["store_code__c"])

    # -------------------------------
    # Prepare Arrow rows
    # -------------------------------
    result_rows = []
    now = datetime.utcnow()

    for month, v in agg.items():
        result_rows.append({
            "agg_month": month,
            "total_bills": v["total_bills"],
            "total_amount": round(v["total_amount"], 2),
            "net_amount": round(v["net_amount"], 2),
            "cancelled_amount": round(v["cancelled_amount"], 2),
            "distinct_customers": len(v["customers"]),
            "distinct_stores": len(v["stores"]),
            "created_at": now
        })

    if not result_rows:
        return {"status": "no_data"}

    # -------------------------------
    # Append to Iceberg aggregation table
    # -------------------------------
    arrow_table = pa.Table.from_pylist(result_rows)
    agg_table.append(arrow_table)

    return {
        "status": "success",
        "months": len(result_rows),
        "rows_written": len(result_rows)
    }

result = run_month_wise_pos_aggregation(
    catalog=get_catalog_client(),
    source_table_name="POS_Transactions.transaction",
    agg_table_name="POS_Transactions.transaction_month_agg",
    start_date=date(2026, 1, 1),
    end_date=date(2026, 2,1 )
)

print(result)