import pyarrow as pa
from pyiceberg.types import (
    BooleanType, LongType, DoubleType, DateType, IntegerType,
    TimestampType, StringType, NestedField, FloatType
)


REQUIRED_FIELDS = [
    "pri_id",
    "customer_mobile__c",
    "created_At",
    "bill_datetime",
]

BOOLEAN_FIELDS = [
    "IsDeleted",
]

TIMESTAMP_FIELDS = [
    "created_At",
    "updated_At",
    "bill_datetime",
]

VARCHAR_FIELDS = [
    # 🔑 Identifiers
    "Id",
    "OwnerId",
    "customerId",
    "Bill_No__c",

    # 📞 Customer
    "customer_mobile__c",
    "customer_mobile__c_backup",
    "customer_mobile__c2",
    "customer_email__c",
    "Customer_Name__c",
    "Customer_Last_Name__c",

    # 🏬 Store / Branch
    "store_code__c",
    "Branch_Name__c",
    "Showroom__c",
    "Showroom_code__c",

    # 🧾 Invoice / Bill
    "Invoice_Date__c",
    "Bill_Date__c",
    "bill_status__c",
    "bill_type__c",

    # 📦 Item
    "item_name",
    "item_code__c",
    "Item_Name__c",
    "Item_Brand_Name__c",
    "item_serial_no__c",
    "IMEINumber__c",

    # 🏠 Address
    "customer_city__c",
    "customer_state__c",
    "customer_state_code",
    "customer_pincode",

    # 🧾 Audit
    "CreatedById",
    "LastModifiedById",
]

FIELD_OVERRIDES = {

    # 🔑 Primary Keys
    "pri_id": (LongType(), pa.int64(), True),
    "tid": (LongType(), pa.int64(), False),

    # 📞 Customer
    "customer_mobile__c": (StringType(), pa.string(), True),
    "customer_mobile__c_backup": (StringType(), pa.string(), False),
    "customer_mobile__c2": (StringType(), pa.string(), False),
    "customerId": (StringType(), pa.string(), False),

    # 👤 Customer identity
    "Customer_Name__c": (StringType(), pa.string(), False),
    "Customer_Last_Name__c": (StringType(), pa.string(), False),
    "customer_email__c": (StringType(), pa.string(), False),

    # 🧾 Invoice / Bill
    "Invoice_Amount__c": (LongType(), pa.int64(), False),
    "Bill_No__c": (StringType(), pa.string(), False),
    "Bill_Grant_Total__c": (DoubleType(), pa.float64(), False),

    # 🏬 Store
    "store_code__c": (StringType(), pa.string(), False),
    "Branch_Name__c": (StringType(), pa.string(), False),
    "Showroom__c": (StringType(), pa.string(), False),
    "Showroom_code__c": (StringType(), pa.string(), False),

    # 📦 Item
    "item_name": (StringType(), pa.string(), False),
    "Item_Name__c": (StringType(), pa.string(), False),
    "Item_Brand_Name__c": (StringType(), pa.string(), False),
    "item_serial_no__c": (StringType(), pa.string(), False),
    "IMEINumber__c": (StringType(), pa.string(), False),

    # 💰 GST (keep as string – parse later)
    "item_cgst": (StringType(), pa.string(), False),
    "item_sgst": (StringType(), pa.string(), False),
    "item_igst": (StringType(), pa.string(), False),
    "item_cgst_perc": (StringType(), pa.string(), False),
    "item_sgst_perc": (StringType(), pa.string(), False),
    "item_igst_perc": (StringType(), pa.string(), False),

    # ⚙ Flags
    "IsDeleted": (BooleanType(), pa.bool_(), False),

    # 🕒 Timestamps (REAL analytics columns)
    "created_At": (TimestampType(), pa.timestamp("ms"), True),
    "updated_At": (TimestampType(), pa.timestamp("ms"), False),
    "bill_datetime": (TimestampType(), pa.timestamp("ms"), True),
}

