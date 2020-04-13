# Databricks notebook source
#Creating Directory in mnt

dbutils.fs.mkdirs("/mnt/mountdatabrick")

# COMMAND ----------

# Setting azure data lake configs

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "2aad1fdc-0e37-4e63-9310-736e687e7880",
           "fs.azure.account.oauth2.client.secret": "O?-w:QSfwiN:ZTsUUA14onTPM2AX1oRJ",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/6e475a14-3e24-45a8-ae34-6e1a840ba44e/oauth2/token"}

# Creating or mounting data lake to dbfs.
dbutils.fs.mount(
  source = "abfss://anacontainer@anastr123.dfs.core.windows.net/",
  mount_point = "/mnt/mountdatabrick",
  extra_configs = configs)


# COMMAND ----------

# Unmounting data lake 

#dbutils.fs.unmount("/mnt/mountdatabrick")

# COMMAND ----------

#Checking or listing the mounted directory

dbutils.fs.ls("/mnt/mountdatabrick/anadir/")

# COMMAND ----------

# Creating Dataframe from csv stored in data lake
df=(spark
    .read
    .option("header","true")
    .option("inferSchema","true")
    .csv("mnt/mountdatabrick/anadir/yellow_tripdata_2019-01.csv")
  )

# Show df
display(df)

# COMMAND ----------

#Creating External Table from DataFrame

df.write.option('path', "/mnt/mountdatabrick/anadir/mysparkdb").saveAsTable("yellow_trip")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Describing database
# MAGIC 
# MAGIC DESCRIBE DATABASE mysparkdb;

# COMMAND ----------

df.write.option('path', "/mnt/mountdatabrick/anadir/mysparkdb/yellow_trip.csv").saveAsTable("yellow_trip_external")

# COMMAND ----------

# MAGIC %sql 
# MAGIC --Describing or checking table created
# MAGIC 
# MAGIC describe table extended default.yellow_trip_external;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from yellow_trip_external limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select VendorID, count(passenger_count) no_of_passengers, sum(total_amount) amount_earn from  yellow_trip_external group by vendorid;

# COMMAND ----------

df.write.option('path', "/mnt/mountdatabrick/anadir/mysparkdb/yellow_trip.csv").saveAsTable("mysparkdb.yellow_trip_external")