-- Databricks notebook source
select "hello world"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC message = 'Hello World'
-- MAGIC display(message)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Intro to Databricks
-- MAGIC
-- MAGIC ## Databricks notebook
-- MAGIC
-- MAGIC ### What languages
-- MAGIC - SQL
-- MAGIC - Python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Let's get our data into databricks
-- MAGIC
-- MAGIC ### "sql" query first and then "pyspark" query for the same

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### reading the tables and displaying them

-- COMMAND ----------

select * from `hive_metastore`.`default`.`sales_data`;

-- COMMAND ----------

describe`hive_metastore`.`default`.`sales_data`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sales_data_python = spark.sql("select * from `hive_metastore`.`default`.`sales_data`")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sales_data_python.limit(10).display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sales_data = '`hive_metastore`.`default`.`sales_data`'
-- MAGIC sales_data_python_2 = spark.read.table(sales_data)
-- MAGIC #reading the same table - an alternative

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sales_data_python_2.limit(10).display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Filtering the data

-- COMMAND ----------

select * from `hive_metastore`.`default`.`sales_data`
 where convenience_store = 'Y' limit 50

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sales_data_python_2.filter("convenience_store == 'Y'").show()

-- COMMAND ----------

select * from `hive_metastore`.`default`.`sales_data` where convenience_store = 'Y' and county = 'Des Moines' limit 50

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sales_data_python_2.filter((sales_data_python_2.convenience_store  == "Y") & (sales_data_python_2.county  == "Des Moines")).show(truncate=False)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Group by, Aggregations and sort 

-- COMMAND ----------

select county, sum(total) as total_sales from `hive_metastore`.`default`.`sales_data`
 group by county order by total_sales desc limit 10

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import sum, col, desc

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sales_data_python_2.groupBy("county").agg(sum("total").alias("sum_sales")).sort(desc("sum_sales")).show(truncate=False)

-- COMMAND ----------

select county, category_name, sum(total) as total_sales from `hive_metastore`.`default`.`sales_data`
 group by county, category_name order by total_sales desc limit 10

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sales_data_python_2.groupBy("county","category_name").sum("total","bottle_qty").show(truncate=False)

-- COMMAND ----------

select county, category_name, sum(total) as total_sales, avg(total) as avg_sales from `hive_metastore`.`default`.`sales_data` group by county, category_name order by total_sales limit 50

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import sum,avg,max

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sales_data_python_2.groupBy("county","category_name").agg(sum("total").alias("sum_sales"),avg("total").alias("avg_sales")).show(truncate=False)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Group by, Aggregations and sort with filter

-- COMMAND ----------

select county, sum(total) as total_sales from `hive_metastore`.`default`.`sales_data`
 where county in('Des Moines','Polk') group by county order by total_sales desc

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sales_data_python_2.groupBy("county").agg(sum("total").alias("sum_sales")).where(col("county") == "Des Moines").sort(desc("sum_sales")).show(truncate=False)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sales_data_python_2.groupBy("county").agg(sum("total").alias("sum_sales")).filter((sales_data_python_2.county == 'Des Moines') | (sales_data_python_2.county == 'Polk')).sort(desc("sum_sales")).show(truncate=False)
