import argparse
import logging
from pyspark.sql import SparkSession, functions as F, types as T

# ------------------------------------------------------------------------------
# 🪵 Logger Configuration
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("UTB_Validator")

# ------------------------------------------------------------------------------
# 0️⃣ Parse Arguments
# ------------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="UTB Extractor Validator")
parser.add_argument(
    "--source_table",
    required=True,
    help="Spark table name for validation (e.g., logistics.bronze.truckr_loads)"
)
args = parser.parse_args()
source_table = args.source_table

# ------------------------------------------------------------------------------
# 1️⃣ Spark Session
# ------------------------------------------------------------------------------
spark = SparkSession.builder.appName("UTB_Extractor_Validator").getOrCreate()

# ------------------------------------------------------------------------------
# 2️⃣ Schema Definition
# ------------------------------------------------------------------------------
fields = [
    "source_file", "broker_name", "broker_phone", "broker_fax", "broker_address",
    "broker_city", "broker_state", "broker_zipcode", "broker_email",
    "loadConfirmationNumber", "totalCarrierPay", "carrier_name", "carrier_mc",
    "carrier_address", "carrier_city", "carrier_state", "carrier_zipcode",
    "carrier_phone", "carrier_fax", "carrier_contact", "pickup_customer_1",
    "pickup_address_1", "pickup_city_1", "pickup_state_1", "pickup_zipcode_1",
    "pickup_start_datetime_1", "pickup_end_datetime_1", "delivery_customer_1",
    "delivery_customer_2", "delivery_address_1", "delivery_address_2",
    "delivery_city_1", "delivery_city_2", "delivery_state_1", "delivery_state_2",
    "delivery_zipcode_1", "delivery_zipcode_2", "delivery_start_datetime_1",
    "delivery_start_datetime_2", "delivery_end_datetime_1", "delivery_end_datetime_2"
]
schema = T.StructType([T.StructField(f, T.StringType()) for f in fields])

# ------------------------------------------------------------------------------
# 3️⃣ Ground Truth Record
# ------------------------------------------------------------------------------
truth_record = {
    "source_file": "dbfs:/Volumes/logistics/bronze/raw/txt/source=UTB/1637351047899_UTB%20ME-FL-FL.txt",
    "broker_name": "USA Truck Brokers Inc.",
    "broker_phone": "305-819-3000",
    "broker_fax": "305-819-7146",
    "broker_address": "14750 NW 77 Court Suite 200",
    "broker_city": "Miami Lakes",
    "broker_state": "FL",
    "broker_zipcode": "33016",
    "broker_email": "accounting@usatruckbrokers.com; talzate@usatruckbrokers.com",
    "loadConfirmationNumber": "301238",
    "totalCarrierPay": "3800.00",
    "carrier_name": "GTT FREIGHT CORP LLC",
    "carrier_mc": "1311415",
    "carrier_address": "120 9th st #1126",
    "carrier_city": "SAN ANTONIO",
    "carrier_state": "TX",
    "carrier_zipcode": "78215",
    "carrier_phone": "786-796-0858",
    "carrier_fax": "",
    "carrier_contact": "Alejandro Arboleda (dispatcher",
    "pickup_customer_1": "DINGLEY PRESS LEWISTON",
    "pickup_address_1": "40 WESTMINSTER ST",
    "pickup_city_1": "LEWISTON",
    "pickup_state_1": "ME",
    "pickup_zipcode_1": "04240",
    "pickup_start_datetime_1": "2021-11-19T08:00:00",
    "pickup_end_datetime_1": "2021-11-19T23:00:00",
    "delivery_customer_1": "YBOR",
    "delivery_customer_2": "WEST PALM BEACH P&DC",
    "delivery_address_1": "1801 GRANT ST",
    "delivery_address_2": "3200 Summit Blvd",
    "delivery_city_1": "TAMPA",
    "delivery_city_2": "WEST PALM BEACH",
    "delivery_state_1": "FL",
    "delivery_state_2": "FL",
    "delivery_zipcode_1": "33605",
    "delivery_zipcode_2": "33406",
    "delivery_start_datetime_1": "2021-11-21T12:00:00",
    "delivery_start_datetime_2": "2021-11-21T19:00:00",
    "delivery_end_datetime_1": "2021-11-21T12:00:00",
    "delivery_end_datetime_2": "2021-11-21T19:00:00",
}
truth_df = spark.createDataFrame([truth_record], schema=schema)

# ------------------------------------------------------------------------------
# 4️⃣ Load Target Table from Parameter
# ------------------------------------------------------------------------------
target_df = spark.table(source_table)

# ------------------------------------------------------------------------------
# 5️⃣ Normalize Data
# ------------------------------------------------------------------------------
def normalize(df):
    string_cols = [c for c, t in df.dtypes if t == "string"]
    return df.select(*[
        F.trim(F.lower(F.col(c))).alias(c) if c in string_cols else F.col(c)
        for c in df.columns
    ])

truth_df = normalize(truth_df)
target_df = normalize(target_df)

# ------------------------------------------------------------------------------
# 6️⃣ Compare Values Field-by-Field
# ------------------------------------------------------------------------------
load_id = truth_record["loadConfirmationNumber"]
target_rows = target_df.filter(F.col("loadConfirmationNumber") == load_id).collect()

results = []

if not target_rows:
    logger.error(f"No record found for loadConfirmationNumber={load_id}")
    for col in schema.fieldNames():
        results.append((col, "❌ Missing record", truth_record.get(col), None))
else:
    logger.info(f"Found record for loadConfirmationNumber={load_id}")
    target_values = target_rows[0].asDict()
    for col in schema.fieldNames():
        truth_val = truth_record.get(col)
        target_val = target_values.get(col)
        norm_truth = str(truth_val).strip().lower() if truth_val else None
        norm_target = str(target_val).strip().lower() if target_val else None
        status = "✅ Match" if norm_truth == norm_target else "❌ Mismatch"
        results.append((col, status, truth_val, target_val))

# ------------------------------------------------------------------------------
# 7️⃣ Log Results
# ------------------------------------------------------------------------------
logger.info(f"Validation results for loadConfirmationNumber={load_id}:")
for field, status, truth, target in results:
    if status == "✅ Match":
        logger.info(f"{field:30} | {status:10} | truth='{truth}' | target='{target}'")
    else:
        logger.error(f"{field:30} | {status:10} | truth='{truth}' | target='{target}'")

# ------------------------------------------------------------------------------
# 8️⃣ Fail Pipeline if Errors Detected
# ------------------------------------------------------------------------------
errors = [r for r in results if r[1].startswith("❌")]
if errors:
    logger.error(f"Validation failed for {len(errors)} fields")
    for field, status, truth, target in errors:
        logger.error(f"  - {field}: expected='{truth}' got='{target}'")
    raise ValueError(f"Validation failed for {len(errors)} fields")
else:
    logger.info("✅ All fields match perfectly.")
