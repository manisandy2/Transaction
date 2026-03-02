from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    DateType,
    TimestampType
)


bill_header_schema = [

    # -------------------------
    # Meta
    # -------------------------
    NestedField(1, "batch_id", StringType(), required=False),

    # -------------------------
    # Store Info
    # -------------------------
    NestedField(2, "store_code", StringType(), required=False),
    NestedField(3, "billed_at_branch_name", StringType(), required=False),
    NestedField(4, "billed_at_company_name", StringType(), required=False),
    NestedField(5, "billed_at_addressline1", StringType(), required=False),
    NestedField(6, "billed_at_addressline2", StringType(), required=False),
    NestedField(7, "billed_at_addressline3", StringType(), required=False),
    NestedField(8, "billed_at_city", StringType(), required=False),
    NestedField(9, "billed_at_pincode", StringType(), required=False),
    NestedField(10, "billed_at_state", StringType(), required=False),
    NestedField(11, "billed_at_phone_no1", StringType(), required=False),
    NestedField(12, "billed_at_phone_no2", StringType(), required=False),
    NestedField(13, "billed_at_GSTN_no", StringType(), required=False),
    NestedField(14, "billed_at_state_code", StringType(), required=False),
    NestedField(15, "billed_at_PAN_no", StringType(), required=False),

    # -------------------------
    # Delivery From
    # -------------------------
    NestedField(16, "delivery_from_branch_store_code", StringType(), required=False),
    NestedField(17, "delivery_from_branch_name", StringType(), required=False),
    NestedField(18, "delivery_from_company_name", StringType(), required=False),
    NestedField(19, "delivery_from_addressline1", StringType(), required=False),
    NestedField(20, "delivery_from_addressline2", StringType(), required=False),
    NestedField(21, "delivery_from_addressline3", StringType(), required=False),
    NestedField(22, "delivery_from_city", StringType(), required=False),
    NestedField(23, "delivery_from_state", StringType(), required=False),
    NestedField(24, "delivery_from_phone_no1", StringType(), required=False),
    NestedField(25, "delivery_from_phone_no2", StringType(), required=False),
    NestedField(26, "delivery_from_GSTN_no", StringType(), required=False),
    NestedField(27, "delivery_from_state_code", StringType(), required=False),
    NestedField(28, "delivery_from_PAN_no", StringType(), required=False),

    # -------------------------
    # Bill Info
    # -------------------------
    NestedField(29, "bill_no", StringType(), required=True),
    NestedField(30, "bill_date", DateType(), required=False),
    NestedField(31, "bill_time", TimestampType(), required=False),
    NestedField(32, "bill_transaction_type", StringType(), required=False),
    NestedField(33, "bill_status", StringType(), required=False),
    NestedField(34, "bill_transcation_no", StringType(), required=False),
    NestedField(35, "bill_refference_no", StringType(), required=False),
    NestedField(36, "bill_refference_date", DateType(), required=False),

    # -------------------------
    # Amounts
    # -------------------------
    NestedField(37, "bill_item_gross_amount", DoubleType(), required=False),
    NestedField(38, "bill_item_total_discount", DoubleType(), required=False),
    NestedField(39, "bill_item_total_tax", DoubleType(), required=False),
    NestedField(40, "bill_item_net_amount", DoubleType(), required=False),
    NestedField(41, "bill_total_trade_deduction", DoubleType(), required=False),
    NestedField(42, "bill_total_trade_addition", DoubleType(), required=False),
    NestedField(43, "bill_grand_total", DoubleType(), required=False),

    # -------------------------
    # Cancel / Modify
    # -------------------------
    NestedField(44, "bill_cancel_date", DateType(), required=False),
    NestedField(45, "bill_cancel_time", TimestampType(), required=False),
    NestedField(46, "bill_cancel_amount", DoubleType(), required=False),
    NestedField(47, "bill_modify_date", DateType(), required=False),
    NestedField(48, "bill_modify_time", TimestampType(), required=False),

    NestedField(49, "bill_remarks1", StringType(), required=False),
    NestedField(50, "bill_remarks2", StringType(), required=False),
    NestedField(51, "bill_tender_type", StringType(), required=False),

    # -------------------------
    # Customer Info
    # -------------------------
    NestedField(52, "customer_fname", StringType(), required=False),
    NestedField(53, "customer_lname", StringType(), required=False),
    NestedField(54, "customer_mobile", StringType(), required=False),
    NestedField(55, "customer_email", StringType(), required=False),
    NestedField(56, "customer_dob", DateType(), required=False),
    NestedField(57, "customer_doa", DateType(), required=False),
    NestedField(58, "customer_code", StringType(), required=False),
    NestedField(59, "customer_gender", StringType(), required=False),
    NestedField(60, "customer_city", StringType(), required=False),
    NestedField(61, "customer_area", StringType(), required=False),
    NestedField(62, "customer_addressline1", StringType(), required=False),
    NestedField(63, "customer_addressline2", StringType(), required=False),
    NestedField(64, "customer_addressline3", StringType(), required=False),
    NestedField(65, "customer_pincode", StringType(), required=False),
    NestedField(66, "customer_phone_no1", StringType(), required=False),
    NestedField(67, "customer_phone_no2", StringType(), required=False),
    NestedField(68, "customer_GSTN_no", StringType(), required=False),
    NestedField(69, "customer_PAN_no", StringType(), required=False),
    NestedField(70, "customer_state", StringType(), required=False),
    NestedField(71, "customer_state_code", StringType(), required=False),

    # -------------------------
    # Delivery To
    # -------------------------
    NestedField(72, "delivery_to_Name", StringType(), required=False),
    NestedField(73, "delivery_to_addressline1", StringType(), required=False),
    NestedField(74, "delivery_to_addressline2", StringType(), required=False),
    NestedField(75, "delivery_to_addressline3", StringType(), required=False),
    NestedField(76, "delivery_to_city", StringType(), required=False),
    NestedField(77, "delivery_to_state", StringType(), required=False),
    NestedField(78, "delivery_to_pincode", StringType(), required=False),
    NestedField(79, "delivery_to_phone_no1", StringType(), required=False),
    NestedField(80, "delivery_to_phone_no2", StringType(), required=False),
    NestedField(81, "delivery_to_GSTN_no", StringType(), required=False),
    NestedField(82, "delivery_to_state_code", StringType(), required=False),
    NestedField(83, "delivery_to_PAN_no", StringType(), required=False),
]

bill_items_schema = [

    # -----------------------------
    # Batch & Bill Info
    # -----------------------------
    NestedField(1, "bill_no", StringType(), required=True),
    NestedField(2, "item_sno", IntegerType(), required=False),

    NestedField(3, "batch_id", StringType(), required=False),
    NestedField(4, "bill_date", DateType(), required=False),
    NestedField(5, "bill_transcation_no", StringType(), required=False),

    # -----------------------------
    # Item Identity
    # -----------------------------
    NestedField(6, "item_code", StringType(), required=False),
    NestedField(7, "item_name", StringType(), required=False),
    NestedField(8, "item_brand_name", StringType(), required=False),
    NestedField(9, "item_category_name", StringType(), required=False),
    NestedField(10, "item_product_name", StringType(), required=False),
    NestedField(11, "item_status", StringType(), required=False),

    # -----------------------------
    # Quantity & Rates
    # -----------------------------
    NestedField(12, "item_quantity", IntegerType(), required=False),
    NestedField(13, "item_gross_rate", DoubleType(), required=False),
    NestedField(14, "item_inctax_rate", DoubleType(), required=False),
    NestedField(15, "item_gross_amount", DoubleType(), required=False),
    NestedField(16, "item_taxable_amount", DoubleType(), required=False),

    # -----------------------------
    # Discounts
    # -----------------------------
    NestedField(17, "item_discount1", DoubleType(), required=False),
    NestedField(18, "item_discount2", DoubleType(), required=False),

    # -----------------------------
    # Tax Details
    # -----------------------------
    NestedField(19, "item_tax", DoubleType(), required=False),
    NestedField(20, "item_cgst_perc", DoubleType(), required=False),
    NestedField(21, "item_cgst", DoubleType(), required=False),
    NestedField(22, "item_sgst_perc", DoubleType(), required=False),
    NestedField(23, "item_sgst", DoubleType(), required=False),
    NestedField(24, "item_igst_perc", DoubleType(), required=False),
    NestedField(25, "item_igst", DoubleType(), required=False),

    # -----------------------------
    # Net
    # -----------------------------
    NestedField(26, "item_net_amount", DoubleType(), required=False),

    # -----------------------------
    # Remarks
    # -----------------------------
    NestedField(27, "item_remarks1", StringType(), required=False),
    NestedField(28, "item_remarks2", StringType(), required=False),
]