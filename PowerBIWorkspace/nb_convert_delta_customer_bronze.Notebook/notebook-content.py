# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "06bc4272-aaf6-40e2-8665-54f4e49995ab",
# META       "default_lakehouse_name": "lh_train_fabric_01_bronze",
# META       "default_lakehouse_workspace_id": "4bab1027-76e9-421b-9021-ad37d26a5e61",
# META       "known_lakehouses": [
# META         {
# META           "id": "06bc4272-aaf6-40e2-8665-54f4e49995ab"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Configuration & Parameters

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS customer")

from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType
)

customer_schema = StructType([
    StructField("CustomerKey", IntegerType(), False),
    StructField("Prefix", StringType(), False),
    StructField("FirstName", StringType(), True),
    StructField("LastName", StringType(), True),
    StructField("BirthDate", StringType(), True),
    StructField("MaritalStatus", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("EmailAddress", StringType(), True), 
    StructField("AnnualIncome", StringType(), True), 
    StructField("EducationLevel", StringType(), True),
    StructField("Occupation", StringType(), True), 
    StructField("HomeOwner", StringType(), True),  
    StructField("TotalChildren", StringType(), False)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Read Files & Convert to Delta

# CELL ********************

bronze_path = "abfss://4bab1027-76e9-421b-9021-ad37d26a5e61@onelake.dfs.fabric.microsoft.com/06bc4272-aaf6-40e2-8665-54f4e49995ab/Files/AdventureWorks_Customers.csv"
bronze_table = "customer.adventureworks_customers"

(
    spark.read
        .format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("mode", "PERMISSIVE")
        .schema(customer_schema)
        .load(bronze_path)
    .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(bronze_table)
)

print(f"Bronze table '{bronze_table}' created successfully.")
spark.sql(f"SELECT COUNT(*) FROM {bronze_table}").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
