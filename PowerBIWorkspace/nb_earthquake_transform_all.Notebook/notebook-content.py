# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f7983fab-86ae-4f49-97b6-829aba461a88",
# META       "default_lakehouse_name": "lh_earthquake_all",
# META       "default_lakehouse_workspace_id": "4bab1027-76e9-421b-9021-ad37d26a5e61",
# META       "known_lakehouses": [
# META         {
# META           "id": "f7983fab-86ae-4f49-97b6-829aba461a88"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Earthquake Data Transformation (Silver)

# CELL ********************

df = spark.read.option("multiline", "true").json("abfss://4bab1027-76e9-421b-9021-ad37d26a5e61@onelake.dfs.fabric.microsoft.com/f7983fab-86ae-4f49-97b6-829aba461a88/Files/2025-12-30_earthquake_data.json")
# df now is a Spark DataFrame containing JSON data from "abfss://4bab1027-76e9-421b-9021-ad37d26a5e61@onelake.dfs.fabric.microsoft.com/f7983fab-86ae-4f49-97b6-829aba461a88/Files/2025-12-30_earthquake_data.json".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Schema of Dataframe

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Expand the Columns

# CELL ********************

from pyspark.sql.functions import col

df_clean = df.select(
    col("id"), 

    # Geometry
    col("geometry.type").alias("geometry_type"),
    col("geometry.coordinates")[0].alias("longitude"),
    col("geometry.coordinates")[1].alias("latitude"),
    col("geometry.coordinates")[2].alias("depth"),

    # Properties
    col("properties.alert").alias("alert"),
    col("properties.cdi").alias("cdi"),
    col("properties.code").alias("code"),
    col("properties.detail").alias("detail"),
    col("properties.dmin").alias("dmin"),
    col("properties.felt").alias("felt"),
    col("properties.gap").alias("gap"),
    col("properties.ids").alias("ids"),
    col("properties.mag").alias("magnitude"),
    col("properties.magType").alias("mag_type"),
    col("properties.mmi").alias("mmi"),
    col("properties.net").alias("net"),
    col("properties.nst").alias("nst"),
    col("properties.place").alias("place"),
    col("properties.rms").alias("rms"),
    col("properties.sig").alias("sig"),
    col("properties.sources").alias("sources"),
    col("properties.status").alias("status"),
    col("properties.time").alias("event_time"),
    col("properties.title").alias("title"),
    col("properties.tsunami").alias("tsunami"),
    col("properties.type").alias("event_type"),
    col("properties.types").alias("types"),
    col("properties.tz").alias("tz"),
    col("properties.updated").alias("updated"),
    col("properties.url").alias("url")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_clean)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Save the Cleaned Dataframe

# CELL ********************

df_clean.write.mode("overwrite").format("delta").saveAsTable("lh_earthquake_all.03_gold.earthquake_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
