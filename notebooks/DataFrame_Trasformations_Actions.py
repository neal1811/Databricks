# Databricks notebook source
# Creating Dataframe from csv stored in data lake
df=(spark
    .read
    .option("header","true")
    .option("inferSchema","true")
    .csv("mnt/mountdatabrick/anadir/yellow_tripdata_2019-01.csv")
  )

# Show df
# display(df)

# COMMAND ----------

df.rdd.getNumPartitions()
df.repartition(10).rdd.getNumPartitions()

# COMMAND ----------

df.collect()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.filter(df.VendorID==1).count()
display(df.limit(5))