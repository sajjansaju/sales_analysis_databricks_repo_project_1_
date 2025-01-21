# Databricks Project: First Experience with Databricks Notebooks

## Overview

This project marks my first experience using **Databricks**, a unified data analytics platform. The goal was to explore Databricks notebooks, learn how to query data using SQL and PySpark within the notebook, and manipulate datasets through filtering, grouping, and aggregations. This hands-on project provided a foundational understanding of working with data in a Databricks environment.

---

## Key Objectives

1. Understand the Databricks notebook environment.
2. Perform SQL queries and replicate them using PySpark.
3. Explore basic operations such as filtering, grouping, aggregations, and sorting.
4. Gain hands-on experience with table reading and transformations.

---

## Steps Undertaken

### 1. Getting Started with Databricks
- **SQL Query**: Executed a simple SQL query to print "Hello World".
- **Python Code**: Displayed "Hello World" using Python to understand code cell execution.

---

### 2. Exploring Databricks Notebook Features
- Documented key functionalities using Markdown cells, such as:
  - SQL and Python integration.
  - Adding rich text for explanations.

---

### 3. Loading Data
- **SQL**: Queried the `sales_data` table using:
  ```sql
  select * from `hive_metastore`.`default`.`sales_data`;

### PySpark: Loaded the same table using two approaches:
- **SQL**: Queried in Python:
```python
sales_data_python = spark.sql("select * from `hive_metastore`.`default`.`sales_data`")
```
- **Direct table read**:
```python
sales_data_python_2 = spark.read.table('sales_data')
```

---

### 4. Data Filtering
- Applied filters to extract specific rows based on conditions:
  - SQL Example:
    ```sql
    select * from sales_data where convenience_store = 'Y' limit 50;```
  - PySpark Example:
    ```python
      sales_data_python_2.filter("convenience_store == 'Y'").show()```

---

### 5.Aggregations and Grouping
- Used SQL and PySpark to calculate total sales and group data by county:
  - SQL Example:
    ```sql
    select county, sum(total) as total_sales from sales_data group by county order by total_sales desc limit 10;
    ```

  - PySpark Example:
    ```python
    sales_data_python_2.groupBy("county").agg(sum("total").alias("sum_sales")).sort(desc("sum_sales")).show()
    ```

---

### 6.Advanced Analysis
- Combined grouping with multiple aggregations (e.g., sum, average) and sorting:
  - SQL Example:
    ```sql
    select county, category_name, sum(total) as total_sales, avg(total) as avg_sales
    from sales_data
    group by county, category_name
    order by total_sales limit 50;
    ```
  - PySpark Example:
    ```python
     sales_data_python_2.groupBy("county", "category_name")
    .agg(sum("total").alias("sum_sales"), avg("total").alias("avg_sales"))
    .show(truncate=False)
    ```
## Key Learnings

1. **SQL and PySpark Integration**: Databricks allows seamless switching between SQL and PySpark, enhancing flexibility.
2. **Data Exploration**: Learned to filter, group, and aggregate data using both SQL and PySpark.
3. **Notebook Functionality**: Markdown cells are a great way to document the workflow for better understanding and future reference.

---

## Challenges

1. Adjusting to the Databricks environment and understanding the syntax for SQL and PySpark operations.
2. Debugging errors when table names or column references were incorrect.

---

## Future Goals

1. Explore advanced PySpark transformations and actions.
2. Learn to create visualizations directly in Databricks.
3. Dive deeper into Databricks-specific features like Delta Lake and MLflow.

---

## Acknowledgment

This project was an invaluable first step in my data analytics journey. Databricks' versatility in handling SQL and PySpark has motivated me to continue exploring its full potential.


    
