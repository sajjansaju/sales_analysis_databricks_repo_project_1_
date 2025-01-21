# Databricks notebook source
# MAGIC %sql
# MAGIC select "hello world"

# COMMAND ----------

message = 'Hello World'
display(message)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Intro to Databricks
# MAGIC
# MAGIC ## Databricks notebook
# MAGIC
# MAGIC ### What languages
# MAGIC - SQL
# MAGIC - Python

# COMMAND ----------

# MAGIC %md
# MAGIC #Let's get our data into databricks
# MAGIC
# MAGIC ### "sql" query first and then "pyspark" query for the same

# COMMAND ----------

# MAGIC %md
# MAGIC #### reading the tables and displaying them

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `hive_metastore`.`default`.`sales_data`;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe sales_data

# COMMAND ----------

sales_data_python = spark.sql("select * from sales_data")

# COMMAND ----------

sales_data_python.limit(10).display()

# COMMAND ----------

sales_data = 'sales_data'
sales_data_python_2 = spark.read.table(sales_data)
#reading the same table - an alternative

# COMMAND ----------

sales_data_python_2.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filtering the data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_data where convenience_store = 'Y' limit 50

# COMMAND ----------

sales_data_python_2.filter("convenience_store == 'Y'").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_data where convenience_store = 'Y' and county = 'Des Moines' limit 50

# COMMAND ----------

sales_data_python_2.filter((sales_data_python_2.convenience_store  == "Y") & (sales_data_python_2.county  == "Des Moines")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Group by, Aggregations and sort 

# COMMAND ----------

# MAGIC %sql
# MAGIC select county, sum(total) as total_sales from sales_data group by county order by total_sales desc limit 10

# COMMAND ----------

from pyspark.sql.functions import sum, col, desc

# COMMAND ----------

sales_data_python_2.groupBy("county").agg(sum("total").alias("sum_sales")).sort(desc("sum_sales")).show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC select county, category_name, sum(total) as total_sales from sales_data group by county, category_name order by total_sales desc limit 10

# COMMAND ----------

sales_data_python_2.groupBy("county","category_name").sum("total","bottle_qty").show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC select county, category_name, sum(total) as total_sales, avg(total) as avg_sales from sales_data group by county, category_name order by total_sales limit 50

# COMMAND ----------

from pyspark.sql.functions import sum,avg,max

# COMMAND ----------

sales_data_python_2.groupBy("county","category_name").agg(sum("total").alias("sum_sales"),avg("total").alias("avg_sales")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Group by, Aggregations and sort with filter

# COMMAND ----------

# MAGIC %sql
# MAGIC select county, sum(total) as total_sales from sales_data where county in('Des Moines','Polk') group by county order by total_sales desc

# COMMAND ----------

sales_data_python_2.groupBy("county").agg(sum("total").alias("sum_sales")).where(col("county") == "Des Moines").sort(desc("sum_sales")).show(truncate=False)

# COMMAND ----------

sales_data_python_2.groupBy("county").agg(sum("total").alias("sum_sales")).filter((sales_data_python_2.county == 'Des Moines') | (sales_data_python_2.county == 'Polk')).sort(desc("sum_sales")).show(truncate=False)
