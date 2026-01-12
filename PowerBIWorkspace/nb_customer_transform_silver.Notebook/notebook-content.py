# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7105be4f-c4ee-45e0-a8f4-4c68c9f6a08f",
# META       "default_lakehouse_name": "lh_train_fabric_01_silver",
# META       "default_lakehouse_workspace_id": "4bab1027-76e9-421b-9021-ad37d26a5e61",
# META       "known_lakehouses": [
# META         {
# META           "id": "7105be4f-c4ee-45e0-a8f4-4c68c9f6a08f"
# META         },
# META         {
# META           "id": "06bc4272-aaf6-40e2-8665-54f4e49995ab"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Transform Customer Data to Silver

# MARKDOWN ********************

# ## Configuration & Parameters

# CELL ********************

# Lakehouse & Schema
bronze_table = "lh_train_fabric_01_bronze.customer.adventureworks_customers"
silver_table = "lh_train_fabric_01_silver.customer.adventureworks_customers"

# Optional: Partitionierung für Silver
partition_col = "BirthYear"  # Beispiel: Jahr aus BirthDate extrahiert

# Weitere Parameter
overwrite_silver = True  # oder False für Merge

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Imports

# CELL ********************

from pyspark.sql.functions import col, to_date, year
from pyspark.sql import DataFrame

df_bronze = spark.table(bronze_table)
display(df_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Source Read (Bronze)

# MARKDOWN ********************

# ## Transformations

# MARKDOWN ********************

# ## Data Quality Checks

# MARKDOWN ********************

# ## Merge into Silver

# CELL ********************

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("lh_train_fabric_02_silver.silver.adventureworks_customers")

print(f"Silver table '{silver_table}' merged successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Logging & Metrics
