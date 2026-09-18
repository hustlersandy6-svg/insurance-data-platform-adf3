# =============================================================================
# Insurance Data Platform — Bronze -> Silver (Policy + Claims) 
# Exported working code from Databricks notebook: Insurance_Bronze_Silver_Gold
# Workspace: dbw-insurance-de (Central India) | Storage: insurancedlshustler
# Verified working against real data on 2026-09-16/17.
# =============================================================================

from pyspark.sql import Window
from pyspark.sql.functions import row_number, col, trim, upper, to_date, expr
from delta.tables import DeltaTable
from decimal import Decimal
from datetime import date, datetime

# -----------------------------------------------------------------------------
# 1. BRONZE — Read Policy from ADLS (all ingestion_date partitions, unioned)
# -----------------------------------------------------------------------------
bronze_path = "abfss://bronze@insurancedlshustler.dfs.core.windows.net/insurance/policy/"

df_policy_bronze = (
    spark.read
    .option("basePath", bronze_path)   # exposes ingestion_date as a real column
    .parquet(bronze_path)
)

print(f"Row count: {df_policy_bronze.count()}")
df_policy_bronze.printSchema()
# Verified: 3,576 rows. Columns: PolicyID, CustomerID, AgentID, PolicyType,
# PremiumAmount (decimal), SumAssured (decimal), StartDate/EndDate (date),
# Status, Branch, PaymentFrequency, LastUpdated (timestamp), ingestion_date (date)

# -----------------------------------------------------------------------------
# 2. SILVER — Dedup to latest version per PolicyID + data quality rules
# -----------------------------------------------------------------------------
window_spec = Window.partitionBy("PolicyID").orderBy(col("LastUpdated").desc())

df_policy_dedup = (
    df_policy_bronze
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

df_policy_clean = (
    df_policy_dedup
    .withColumn("Status", upper(trim(col("Status"))))
    .withColumn("PolicyType", upper(trim(col("PolicyType"))))
    .withColumn("Branch", upper(trim(col("Branch"))))
    .filter(col("PolicyID").isNotNull())
    .filter(col("PremiumAmount") > 0)
    .filter(col("SumAssured") > 0)
    .filter(col("StartDate") <= col("EndDate"))
)

print(f"Bronze rows:        {df_policy_bronze.count()}")
print(f"After dedup:        {df_policy_dedup.count()}")
print(f"Silver (after DQ):  {df_policy_clean.count()}")
# Verified: 3576 -> 447 (dedup) -> 447 (all passed DQ rules)

silver_path = "abfss://silver@insurancedlshustler.dfs.core.windows.net/insurance/policy/"

(df_policy_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_path))

# Register as a Unity Catalog external table so it's queryable via SQL
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS dbw_insurance_de.default.silver_policy
    USING DELTA
    LOCATION '{silver_path}'
""")

# -----------------------------------------------------------------------------
# 3. MERGE — Simulate an incremental "Day 2" load (3 updates + 2 new policies)
# -----------------------------------------------------------------------------
day2_data = [
    ("POL00000002","CUST0000259","AGT00006","LIFE",   Decimal("46198.73"), Decimal("1459478.92"), date(2024,3,18), date(2025,3,18), "LAPSED",    "MUMBAI",    "MONTHLY",   datetime(2026,9,17,9,0,0),  date(2026,9,17)),
    ("POL00000005","CUST0000013","AGT00050","HOME",   Decimal("60428.84"), Decimal("3595883.19"), date(2025,6,16), date(2026,6,16), "ACTIVE",    "PUNE",      "MONTHLY",   datetime(2026,9,17,9,5,0),  date(2026,9,17)),
    ("POL00000010","CUST0000046","AGT00012","HEALTH", Decimal("45000.00"), Decimal("2600000.00"), date(2025,2,22), date(2026,2,22), "CANCELLED", "DELHI",     "QUARTERLY", datetime(2026,9,17,9,10,0), date(2026,9,17)),
    ("POL00003577","CUST0000901","AGT00015","MOTOR",  Decimal("28000.00"), Decimal("900000.00"),  date(2026,9,17), date(2027,9,17), "ACTIVE",    "CHENNAI",   "MONTHLY",   datetime(2026,9,17,9,15,0), date(2026,9,17)),
    ("POL00003578","CUST0000902","AGT00033","TRAVEL", Decimal("9500.00"),  Decimal("500000.00"),  date(2026,9,17), date(2027,9,17), "ACTIVE",    "BANGALORE", "YEARLY",    datetime(2026,9,17,9,20,0), date(2026,9,17)),
]
columns = ["PolicyID","CustomerID","AgentID","PolicyType","PremiumAmount","SumAssured",
           "StartDate","EndDate","Status","Branch","PaymentFrequency","LastUpdated","ingestion_date"]

df_day2 = spark.createDataFrame(day2_data, columns)
df_day2_clean = (
    df_day2
    .withColumn("Status", upper(trim(col("Status"))))
    .withColumn("PolicyType", upper(trim(col("PolicyType"))))
    .withColumn("Branch", upper(trim(col("Branch"))))
)

print(f"Silver row count BEFORE merge: {spark.table('dbw_insurance_de.default.silver_policy').count()}")

silver_table = DeltaTable.forPath(spark, silver_path)
(silver_table.alias("target")
    .merge(df_day2_clean.alias("source"), "target.PolicyID = source.PolicyID")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

print(f"Silver row count AFTER merge:  {spark.table('dbw_insurance_de.default.silver_policy').count()}")
# Verified: 447 -> 449 (3 updated in place, 2 inserted as new)

# -----------------------------------------------------------------------------
# 4. TIME TRAVEL — Query a prior version of the Silver table
# -----------------------------------------------------------------------------
display_history = spark.sql("DESCRIBE HISTORY dbw_insurance_de.default.silver_policy")
# version 0 = initial WRITE (447 rows), version 1 = MERGE (449 rows)

df_before_merge = spark.read.format("delta").option("versionAsOf", 0).load(silver_path)
print(f"Row count at version 0 (before merge): {df_before_merge.count()}")

df_current = spark.read.format("delta").load(silver_path)
print(f"Row count at current version:          {df_current.count()}")

# To restore a prior version in production (not run here):
# spark.sql("RESTORE TABLE dbw_insurance_de.default.silver_policy TO VERSION AS OF 0")

# -----------------------------------------------------------------------------
# 5. OPTIMIZE + ZORDER — Compact files, cluster on a low-cardinality filter column
# -----------------------------------------------------------------------------
spark.sql("""
    OPTIMIZE dbw_insurance_de.default.silver_policy
    ZORDER BY (Status)
""")
# ZORDER chosen on Status (4 distinct values) rather than PolicyID (near-unique) —
# high-cardinality columns give no data-skipping benefit from ZORDER.

# -----------------------------------------------------------------------------
# 6. BRONZE -> SILVER — Claims (all monetary/date fields arrive as STRING)
# -----------------------------------------------------------------------------
claims_path = "abfss://bronze@insurancedlshustler.dfs.core.windows.net/insurance/claims/"
df_claims_bronze = spark.read.parquet(claims_path)
print(f"Claims row count: {df_claims_bronze.count()}")
df_claims_bronze.printSchema()
# Verified: 1,400 rows. ClaimAmount/ApprovedAmount/ClaimDate all came in as string —
# vendor file, not a typed source system.

df_claims_cast = (
    df_claims_bronze
    .withColumn("ClaimAmount_parsed", expr("try_cast(ClaimAmount as decimal(18,2))"))
    .withColumn("ApprovedAmount_parsed", expr("try_cast(ApprovedAmount as decimal(18,2))"))
    .withColumn("ClaimDate_parsed", to_date(col("ClaimDate")))
    .withColumn("ClaimType", upper(trim(col("ClaimType"))))
    .withColumn("ClaimStatus", upper(trim(col("ClaimStatus"))))
)

# Two DISTINCT reject categories — do not collapse into one filter.
# NULL > 0 evaluates to NULL in SQL, not False, so a naive filter silently
# lets unparseable rows survive. Check .isNull() explicitly first.
df_claims_unparseable = df_claims_cast.filter(
    col("ClaimAmount_parsed").isNull() & col("ClaimAmount").isNotNull()
)
df_claims_business_invalid = df_claims_cast.filter(
    col("ClaimAmount_parsed").isNotNull() & (col("ClaimAmount_parsed") <= 0)
)

df_claims_clean = (
    df_claims_cast
    .filter(col("ClaimAmount_parsed").isNotNull())
    .filter(col("ClaimAmount_parsed") > 0)
    .drop("ClaimAmount", "ApprovedAmount", "ClaimDate")
    .withColumnRenamed("ClaimAmount_parsed", "ClaimAmount")
    .withColumnRenamed("ApprovedAmount_parsed", "ApprovedAmount")
    .withColumnRenamed("ClaimDate_parsed", "ClaimDate")
)

print(f"Claims Bronze rows:                       {df_claims_bronze.count()}")
print(f"Unparseable ClaimAmount (e.g. '-'):        {df_claims_unparseable.count()}")
print(f"Business-invalid (valid number, <= 0):     {df_claims_business_invalid.count()}")
print(f"Silver Claims (clean):                     {df_claims_clean.count()}")

silver_claims_path = "abfss://silver@insurancedlshustler.dfs.core.windows.net/insurance/claims/"

(df_claims_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_claims_path))

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS dbw_insurance_de.default.silver_claims
    USING DELTA
    LOCATION '{silver_claims_path}'
""")

# =============================================================================
# NEXT (not yet run): Gold layer — Claims Ratio aggregation joining
# silver_policy + silver_claims. See conversation for design once rebuilt.
# =============================================================================
