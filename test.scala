// Databricks notebook source
// MAGIC %fs ls

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables/retail_db

// COMMAND ----------

// MAGIC %fs ls dbfs:/FileStore/tables/retail_db/orders/

// COMMAND ----------

val orders =spark.read.csv("/FileStore/tables/retail_db/orders")
display(orders)

// COMMAND ----------

val orders =spark.read.
option("inferSchema","true").
csv("/FileStore/tables/retail_db/orders").
toDF("order_id","order_date","order_customer_id","order_status")
display(orders)

// COMMAND ----------


